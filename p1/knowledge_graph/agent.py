import os
import sys
import logging
import boto3
from sqlalchemy import create_engine, text
from langchain_aws import ChatBedrockConverse
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.tools import StructuredTool
from p2.utility.database_driver import PostgresDriver
from utility.secret_manager import load_secrets
#load_secrets()
logger = logging.getLogger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

MAX_RESULTS = 20

SYSTEM_PROMPT = """
You are an AI assistant for a Knowledge Graph stored in PostgreSQL.

Your only source of truth is the Knowledge Graph, accessed through the tools
provided to you. Never answer from memory, never guess, never invent nodes,
properties, or relationships.

--------------------------------------------------
GRAPH MODEL
--------------------------------------------------
- Nodes represent assets: Equipment, Pump, Vessel, Exchanger, Valve,
  Instrument, Line, Plant, etc.
- Relationships connect two nodes and have a direction. Types include (but are not limited to): CONNECTED_TO, PART_OF, LOCATED_AT, HAS_EQUIPMENT, HAS_DOCUMENT, HAS_WORK_ORDER, HAS_TASK_LIST, HAS_TAG, HAS_FAILURE_MODE, HAS_FAILURE_CAUSE, HAS_FAILURE_EFFECT, SUBSCRIBES_TO, PUBLISHES_TO, READS_WRITES, OWNED_BY. Other relationships can also exist in the database.
- IMPORTANT: Relationships can be traversed both ways. Pay attention to the direction (`Source` or `Destination`):
  - If A --PART_OF--> B [Destination], then A is a Child, and B is the Parent.
  - If B --PART_OF--> A [Source], then B is a Parent, and A is the Child.
  - Apply this reverse-logic to all relationship types (e.g., if A is the Destination of HAS_DOCUMENT, then A is the Document itself).
- "Neighbour" -> any directly connected node regardless of type.

--------------------------------------------------
NODE PROPERTIES (SCHEMA)
--------------------------------------------------
Nodes contain a JSONB `properties` dictionary. When filtering, the most common and useful keys are:
- `equipment_id`, `tag_name`, `plant_code`, `status`, `criticality`
- `manufacturer`, `model`, `construction_year`, `equipment_category`
- `priority`, `failure_cause_text`, `failure_effect_text`, `failure_mode_text`
- `wo_number` (Work Order), `planner_group`, `cost_center`
Use these exact keys when calling `filter_nodes_by_property(label, property_key, property_value)`.

--------------------------------------------------
TOOLS
--------------------------------------------------
search_node(identifier)
    Find nodes by name, tag_name, or equipment_id (partial match).
    Use for: "find X", "locate X", "tell me about X", or as the first step
    before any relationship/neighbor question.
    *CRITICAL*: If the user asks "tell me about X", after finding exactly ONE match, you must ALSO call get_node_relationships to provide a detailed summary of the node and its connections!

count_nodes(label)
    Count nodes of a given label (e.g. "Pump", "Vessel").

get_neighbors(identifier)
    All nodes directly connected to the matched node, in either direction.

get_node_relationships(identifier)
    All relationships (type + direction + other node) for the matched node.

find_path(source_identifier, target_identifier)
    Finds the shortest path connecting two pieces of equipment (up to 7 hops). 
    Use for: "how is X connected to Y?", "trace the line from X to Y".

get_equipment_breakdown(identifier)
    Gets the complete downward bill of materials or component breakdown for an equipment node. 
    Use for: "what are the components of X?", "breakdown of X".

get_parent_lineage(identifier)
    Finds exactly where a component is located by tracing upwards to its parent systems, units, and plant. 
    Use for: "where is X located?", "what system does X belong to?".

filter_nodes_by_property(label, property_key, property_value)
    Advanced search to find nodes by their properties. 
    Use for: "find all Pumps with status Inactive", "show Equipment where criticality is High".

get_associated_documents(identifier)
    Instantly grabs all manuals, datasheets, SOPs, and failure codes for a node. 
    Use for: "what documents does X have?", "failure modes of X".

--------------------------------------------------
HANDLING TOOL RESULTS
--------------------------------------------------
- If a tool reports NO_MATCH, tell the user the item could not be found in
  the Knowledge Graph. Do not invent a plausible-sounding answer.
- If a tool reports MULTIPLE_MATCHES, do NOT pick one yourself. You MUST copy and paste the exact list of candidates (including their node_id, name, and properties) directly into your final answer to the user, and ask them to reply with the exact node_id they want. Wait for their answer before calling get_neighbors or get_node_relationships.
- If a tool reports ONE_MATCH plus relationship/neighbor data, answer
  directly using that data.
- Never expose raw SQL, table names, or internal tool names in your answer.

# --------------------------------------------------
# CONVERSATION CONTEXT
# --------------------------------------------------

Maintain conversation context throughout the interaction.
When the user refers to a previously discussed node using words such as:

- it
- this
- that
- this equipment
- this node
- the previous one
- the same pump
- the same equipment

interpret the reference as the most recently discussed node whenever the reference is clear.

Examples

Example 1
User:
Tell me about P101.
Assistant:
...
User:
What is connected to it?
Interpret "it" as P101.
----------------------------------------

Example 2
User:
Show me E101.
Assistant:
...
User:
Who is connected to this equipment?
Interpret "this equipment" as E101.
----------------------------------------

Example 3
User:
Show me Pump P101.
Assistant:
...
User:
Show its relationships.
Interpret "its" as P101.
If the reference is not clear or multiple previous nodes could match, ask a clarification question instead of making assumptions.

--------------------------------------------------
RESPONSE STYLE
--------------------------------------------------
- Answer the question first, on its own line, then give supporting details. \n
- Put each distinct fact, node, or relationship on its own line. Never run
  multiple facts together in one long sentence separated only by commas. \n
- Use a Markdown bullet ("- ") for each item in a list (equipment, neighbors,
  relationships). One bullet per line, always. \n
- Insert a blank line between the summary sentence and the list that follows
  it, and between distinct sections of the answer (e.g. between a summary of
  equipment and a summary of relationships). \n
- For a single node: name, label, and relevant properties (tag, equipment id,
  status, plant code) -- each on its own line. \n
- For relationships: source -> relationship type -> target, and whether it's
  Source or Destination -- one per bullet line. \n
- For multiple nodes: summarize the count on the first line, blank line, then
  one bullet per node. \n
- UI HIGHLIGHTING RULE: Whenever you mention a specific node by its name in your final response, you MUST format it as a standard markdown link containing its exact node_id. 
  For example, if the name is CP-KN-0001 and the node_id is 4294#equipment, you must output exactly [CP-KN-0001](node:4294#equipment) without any surrounding backticks or quotes. Do NOT put backticks around the link, otherwise it will render as code instead of a clickable link! Do NOT expose the raw node_id anywhere else in plain text. \n
- Keep it concise and readable. Don't dump raw JSON unless asked. \n
"""


class KnowledgeGraphAgent:

    def __init__(self):
        self.engine = self.connect_database()
        self.llm = self.create_llm()
        self.tools = self.create_tools()
        self.checkpointer = MemorySaver()
        self.agent = self.create_agent()

    # Database
    def connect_database(self):
        db_driver = PostgresDriver()
        config = db_driver.config

        db_uri = (
            f"postgresql+psycopg2://"
            f"{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}"
            f"/{config['database']}"
        )
        return create_engine(db_uri, pool_pre_ping=True)

    def _run_query(self, sql, params):
        """Run a parameterized query and return a list of plain dicts."""
        # Enforce strictly READ-ONLY queries
        if not sql.strip().upper().startswith(("SELECT", "WITH")):
            raise Exception("SECURITY_VIOLATION: The agent is restricted to read-only access and cannot modify the database.")
            
        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()
            return [dict(row) for row in rows]

    def _find_nodes(self, identifier: str):
        # We combine exact and partial search into a single lightning-fast DB roundtrip!
        # Exact matches are mathematically sorted to the top.
        sql = """
            SELECT DISTINCT
                node_id,
                name,
                label,
                properties,
                CASE 
                    WHEN node_id = :exact OR name ILIKE :exact OR properties ->> 'tag_name' ILIKE :exact OR properties ->> 'equipment_id' ILIKE :exact THEN 1
                    ELSE 0 
                END AS is_exact
            FROM kg_nodes
            WHERE node_id = :exact
               OR name ILIKE :pattern
               OR properties ->> 'tag_name' ILIKE :pattern
               OR (properties ->> 'equipment_id' ILIKE :pattern AND (label = 'Equipment' OR label = 'Plant'))
            ORDER BY is_exact DESC, name ASC
            LIMIT :limit
        """
        params = {"exact": identifier, "pattern": f"%{identifier}%", "limit": MAX_RESULTS}
        return self._run_query(sql, params)

    def _resolve_node(self, identifier: str):
        """
        Resolve a user-provided identifier into a single Knowledge Graph node.
        Returns a standardized response so every tool behaves consistently.
        Status values:
            - found
            - not_found
            - multiple_matches
            - error
        """
        try:
            # 1 DB Round-Trip (cuts latency in half for partial matches!)
            matches = self._find_nodes(identifier)
            
            # If the database found exact matches at the top, ignore the partial matches
            if matches and matches[0].get('is_exact') == 1:
                matches = [m for m in matches if m.get('is_exact') == 1]

        except Exception as e:
            logger.exception("Node resolution failed")
            return {
                "success": False,
                "status": "error",
                "message": str(e),
                "node": None,
                "matches": []
            }

        # Handle missing node
        if not matches:
            return {
                "success": False,
                "status": "not_found",
                "message": f"No node found for '{identifier}'.",
                "node": None,
                "matches": []
            }

        # Handle exact match
        if len(matches) == 1:
            return {
                "success": True,
                "status": "found",
                "message": "One matching node found.",
                "node": matches[0],
                "matches": matches
            }

        # Handle multiple candidates
        return {
            "success": False,
            "status": "multiple_matches",
            "message": f"{len(matches)} matching nodes found.",
            "node": None,
            "matches": matches
        }

    @staticmethod
    def _describe_node(node):
        props = node.get("properties") or {}
        bits = []
        # Include node_id for UI link generation
        bits.append(f"node_id={node.get('node_id')}")
        bits.append(f"name={node.get('name')}")
        bits.append(f"label={node.get('label')}")
        
        for key in ("tag_name", "equipment_id", "status", "plant_code", "description"):
            if props.get(key):
                bits.append(f"{key}={props[key]}")
        return ", ".join(bits)

    # Tools
    def _format_resolution_error(self, resolved: dict) -> str:
        status = resolved["status"]
        if status == "error":
            return f"DATABASE_ERROR: {resolved['message']}"
        if status == "not_found":
            return f"NO_MATCH: {resolved['message']}"
        if status == "multiple_matches":
            matches = resolved["matches"]
            lines = "\n".join(f"- {self._describe_node(node)}" for node in matches)
            return (
                f"MULTIPLE_MATCHES ({len(matches)} found):\n"
                f"{lines}\n\n"
                f"INSTRUCTION TO AI: You MUST stop and ask the user to clarify which specific node they mean. YOU MUST COPY AND PASTE THE ENTIRE LIST OF CANDIDATES ABOVE directly into your final answer to the user so they can see their options! Do NOT proceed or guess until they clarify."
            )
        return "UNKNOWN_ERROR"
    
    # Tool Implementations
    def search_node(self, identifier: str) -> str:
        resolved = self._resolve_node(identifier)
        if resolved["status"] != "found":
            return self._format_resolution_error(resolved)
            
        node = resolved["node"]
        return f"ONE_MATCH: {self._describe_node(node)}"

    def count_nodes(self, label: str) -> str:
        sql = "SELECT COUNT(*) AS total FROM kg_nodes WHERE label ILIKE :label"
        try:
            result = self._run_query(sql, {"label": label})
            return f"COUNT: {result[0]['total']} node(s) with label '{label}'."
        except Exception as e:
            logger.exception("count_nodes failed")
            return f"DATABASE_ERROR: {e}"

    def get_neighbors(self, identifier: str) -> str:
        resolved = self._resolve_node(identifier)
        if resolved["status"] != "found":
            return self._format_resolution_error(resolved)
            
        node = resolved["node"]
        node_id = node["node_id"]
        
        sql = """
            SELECT
                CASE WHEN r.from_node_id = :node_id THEN t.name ELSE s.name END AS neighbour_name,
                CASE WHEN r.from_node_id = :node_id THEN t.label ELSE s.label END AS neighbour_label,
                r.rel_type,
                CASE WHEN r.from_node_id = :node_id THEN 'Destination' ELSE 'Source' END AS direction
            FROM kg_relationships r
            JOIN kg_nodes s ON s.node_id = r.from_node_id
            JOIN kg_nodes t ON t.node_id = r.to_node_id
            WHERE r.from_node_id = :node_id OR r.to_node_id = :node_id
            LIMIT :limit
        """
        try:
            rows = self._run_query(sql, {"node_id": node_id, "limit": MAX_RESULTS})
        except Exception as e:
            logger.exception("get_neighbors query failed")
            return f"DATABASE_ERROR: {e}"

        if not rows:
            return f"ONE_MATCH: {self._describe_node(node)}\nNO_NEIGHBORS: this node has no connections."

        lines = "\n".join(
            f"- {r['neighbour_name']} ({r['neighbour_label']}) via {r['rel_type']} [{r['direction']}]"
            for r in rows
        )
        return f"ONE_MATCH: {self._describe_node(node)}\nNEIGHBORS ({len(rows)}):\n{lines}"

    def get_node_relationships(self, identifier: str) -> str:
        resolved = self._resolve_node(identifier)
        if resolved["status"] != "found":
            return self._format_resolution_error(resolved)
            
        node = resolved["node"]
        node_id = node["node_id"]
        
        sql = """
            SELECT
                r.rel_type,
                s.name AS source_name,
                t.name AS target_name,
                CASE WHEN r.from_node_id = :node_id THEN 'Destination' ELSE 'Source' END AS direction
            FROM kg_relationships r
            JOIN kg_nodes s ON s.node_id = r.from_node_id
            JOIN kg_nodes t ON t.node_id = r.to_node_id
            WHERE r.from_node_id = :node_id OR r.to_node_id = :node_id
            LIMIT :limit
        """
        try:
            rows = self._run_query(sql, {"node_id": node_id, "limit": MAX_RESULTS})
        except Exception as e:
            logger.exception("get_node_relationships query failed")
            return f"DATABASE_ERROR: {e}"
            
        if not rows:
            return f"ONE_MATCH: {self._describe_node(node)}\nNO_RELATIONSHIPS: this node has no relationships."

        lines = []
        for r in rows:
            neighbour_name = r['target_name'] if r['direction'] == 'Destination' else r['source_name']
            lines.append(f"{node['name']} --{r['rel_type']}--> {neighbour_name} [{r['direction']}]")
            
        return (
            f"Matched Node:\n"
            f"{self._describe_node(node)}\n\n"
            f"Connections ({len(rows)}):\n"
            + "\n".join(lines)
        )

    def find_path(self, source_identifier: str, target_identifier: str) -> str:
        src_resolved = self._resolve_node(source_identifier)
        if src_resolved["status"] != "found":
            return f"Source Node Issue: Failed to cleanly resolve '{source_identifier}'. Result: {src_resolved['status']}"
            
        tgt_resolved = self._resolve_node(target_identifier)
        if tgt_resolved["status"] != "found":
            return f"Target Node Issue: Failed to cleanly resolve '{target_identifier}'. Result: {tgt_resolved['status']}"
            
        src_id = src_resolved["node"]["node_id"]
        tgt_id = tgt_resolved["node"]["node_id"]
        
        sql = """
            WITH RECURSIVE path_search AS (
                SELECT 
                    :src_id::text AS current_node,
                    ARRAY[:src_id::text] AS path_array,
                    0 AS depth
                
                UNION ALL
                
                SELECT 
                    CASE WHEN r.from_node_id = ps.current_node THEN r.to_node_id ELSE r.from_node_id END,
                    ps.path_array || CASE WHEN r.from_node_id = ps.current_node THEN r.to_node_id ELSE r.from_node_id END,
                    ps.depth + 1
                FROM path_search ps
                JOIN kg_relationships r ON r.from_node_id = ps.current_node OR r.to_node_id = ps.current_node
                WHERE ps.depth < 7
                  AND NOT (CASE WHEN r.from_node_id = ps.current_node THEN r.to_node_id ELSE r.from_node_id END = ANY(ps.path_array))
            )
            SELECT path_array 
            FROM path_search 
            WHERE current_node = :tgt_id
            ORDER BY depth ASC
            LIMIT 1;
        """
        try:
            rows = self._run_query(sql, {"src_id": src_id, "tgt_id": tgt_id})
        except Exception as e:
            logger.exception("find_path query failed")
            return f"DATABASE_ERROR: {e}"
            
        if not rows:
            return f"NO PATH FOUND: No connection exists between {src_resolved['node']['name']} and {tgt_resolved['node']['name']} within 7 hops."
            
        path_ids = rows[0]["path_array"]
        id_to_name = {}
        try:
            nodes_sql = "SELECT node_id, name FROM kg_nodes WHERE node_id = ANY(:path_ids)"
            node_rows = self._run_query(nodes_sql, {"path_ids": path_ids})
            for nr in node_rows:
                id_to_name[nr["node_id"]] = nr["name"]
        except Exception:
            pass
            
        path_names = [id_to_name.get(pid, pid) for pid in path_ids]
        return f"PATH FOUND ({len(path_names)-1} hops):\n" + " -> ".join(path_names)

    def get_equipment_breakdown(self, identifier: str) -> str:
        resolved = self._resolve_node(identifier)
        if resolved["status"] != "found":
            return self._format_resolution_error(resolved)
            
        node = resolved["node"]
        sql = """
            WITH RECURSIVE breakdown AS (
                SELECT r.from_node_id AS node_id, 1 AS level
                FROM kg_relationships r
                WHERE r.to_node_id = :root_id AND r.rel_type IN ('PART_OF', 'HAS_COMPONENT')
                
                UNION ALL
                
                SELECT r.from_node_id, b.level + 1
                FROM breakdown b
                JOIN kg_relationships r ON r.to_node_id = b.node_id
                WHERE r.rel_type IN ('PART_OF', 'HAS_COMPONENT') AND b.level < 10
            )
            SELECT n.node_id, n.name, n.label, b.level
            FROM breakdown b
            JOIN kg_nodes n ON n.node_id = b.node_id
            ORDER BY b.level ASC
            LIMIT :limit;
        """
        try:
            rows = self._run_query(sql, {"root_id": node["node_id"], "limit": 100})
        except Exception as e:
            return f"DATABASE_ERROR: {e}"
            
        if not rows:
            return f"BREAKDOWN: {node['name']} has no sub-components."
            
        lines = [f"{'  ' * (r['level'] - 1)}- {r['name']} ({r['label']})" for r in rows]
        return f"BREAKDOWN for {node['name']}:\n" + "\n".join(lines)

    def get_parent_lineage(self, identifier: str) -> str:
        resolved = self._resolve_node(identifier)
        if resolved["status"] != "found":
            return self._format_resolution_error(resolved)
            
        node = resolved["node"]
        sql = """
            WITH RECURSIVE lineage AS (
                SELECT r.to_node_id AS node_id, 1 AS level
                FROM kg_relationships r
                WHERE r.from_node_id = :root_id AND r.rel_type IN ('PART_OF', 'BELONGS_TO')
                
                UNION ALL
                
                SELECT r.to_node_id, l.level + 1
                FROM lineage l
                JOIN kg_relationships r ON r.from_node_id = l.node_id
                WHERE r.rel_type IN ('PART_OF', 'BELONGS_TO') AND l.level < 10
            )
            SELECT n.node_id, n.name, n.label, l.level
            FROM lineage l
            JOIN kg_nodes n ON n.node_id = l.node_id
            ORDER BY l.level ASC
            LIMIT :limit;
        """
        try:
            rows = self._run_query(sql, {"root_id": node["node_id"], "limit": 50})
        except Exception as e:
            return f"DATABASE_ERROR: {e}"
            
        if not rows:
            return f"LINEAGE: {node['name']} has no parent systems (it is a top-level node)."
            
        lines = [f"{'  ' * (r['level'] - 1)}-> {r['name']} ({r['label']})" for r in rows]
        return f"UPWARD LINEAGE for {node['name']}:\n" + "\n".join(lines)

    def filter_nodes_by_property(self, label: str, property_key: str, property_value: str) -> str:
        sql = """
            SELECT node_id, name, label, properties
            FROM kg_nodes
            WHERE label ILIKE :label
              AND properties ->> :key ILIKE :val
            LIMIT :limit
        """
        try:
            rows = self._run_query(sql, {
                "label": f"%{label}%", 
                "key": property_key, 
                "val": f"%{property_value}%",
                "limit": MAX_RESULTS
            })
        except Exception as e:
            return f"DATABASE_ERROR: {e}"
            
        if not rows:
            return f"NO MATCHES FOUND for {label} where {property_key} = {property_value}"
            
        lines = "\n".join(f"- {self._describe_node(r)}" for r in rows)
        return f"FILTER RESULTS ({len(rows)} found):\n{lines}"

    def get_associated_documents(self, identifier: str) -> str:
        resolved = self._resolve_node(identifier)
        if resolved["status"] != "found":
            return self._format_resolution_error(resolved)
            
        node = resolved["node"]
        node_id = node["node_id"]
        sql = """
            SELECT n.node_id, n.name, n.label, r.rel_type
            FROM kg_relationships r
            JOIN kg_nodes n ON (n.node_id = r.from_node_id OR n.node_id = r.to_node_id)
            WHERE (r.from_node_id = :node_id OR r.to_node_id = :node_id)
              AND n.node_id != :node_id
              AND n.label IN ('DataSheet', 'FailureCause', 'FailureEffect', 'FailureMode', 'SOP', 'Manual', 'WorkOrder', 'Document')
            LIMIT :limit
        """
        try:
            rows = self._run_query(sql, {"node_id": node_id, "limit": MAX_RESULTS})
        except Exception as e:
            return f"DATABASE_ERROR: {e}"
            
        if not rows:
            return f"DOCUMENTS: No associated documents or failure codes found for {node['name']}."
            
        lines = [f"- {r['name']} ({r['label']}) via {r['rel_type']}" for r in rows]
        return f"DOCUMENTS FOR {node['name']} ({len(rows)}):\n" + "\n".join(lines)

    def create_tools(self):
        return [
            StructuredTool.from_function(
                name="search_node",
                func=self.search_node,
                description=(
                    "Find a node by name, tag_name, or equipment_id (partial match allowed). "
                    "Use this first for any request that names or describes an asset. "
                    "Input: an identifier string. "
                    "Output starts with NO_MATCH, ONE_MATCH, or MULTIPLE_MATCHES."
                ),
            ),
            StructuredTool.from_function(
                name="count_nodes",
                func=self.count_nodes,
                description=(
                    "Count nodes with a given label, e.g. 'Pump', 'Vessel', 'Equipment'. "
                    "Input: the label string. Output: COUNT: <n> ..."
                ),
            ),
            StructuredTool.from_function(
                name="get_neighbors",
                func=self.get_neighbors,
                description=(
                    "Get all nodes directly connected to a given node (any relationship type, "
                    "either direction). Input: an identifier string (name/tag/equipment id). "
                ),
            ),
            StructuredTool.from_function(
                name="get_node_relationships",
                func=self.get_node_relationships,
                description=(
                    "Get all relationships (type + direction + other node) for a given node. "
                    "Input: an identifier string (name/tag/equipment id). "
                ),
            ),
            StructuredTool.from_function(
                name="find_path",
                func=self.find_path,
                description=(
                    "Finds the shortest path connecting two pieces of equipment (up to 7 hops). "
                    "Provide the names or IDs of the two nodes. "
                    "Input: source_identifier (string), target_identifier (string)."
                ),
            ),
            StructuredTool.from_function(
                name="get_equipment_breakdown",
                func=self.get_equipment_breakdown,
                description=(
                    "Gets the complete downward bill of materials or component breakdown for an equipment node. "
                    "Recursively traces PART_OF and HAS_COMPONENT relationships downwards. "
                    "Input: identifier (string)."
                ),
            ),
            StructuredTool.from_function(
                name="get_parent_lineage",
                func=self.get_parent_lineage,
                description=(
                    "Finds exactly where a component is located by tracing upwards to its parent systems, units, and plant. "
                    "Recursively traces PART_OF and BELONGS_TO relationships upwards. "
                    "Input: identifier (string)."
                ),
            ),
            StructuredTool.from_function(
                name="filter_nodes_by_property",
                func=self.filter_nodes_by_property,
                description=(
                    "Advanced search to find nodes by their properties. "
                    "E.g., filter_nodes_by_property('Equipment', 'status', 'Inactive'). "
                    "Input: label (string), property_key (string), property_value (string)."
                ),
            ),
            StructuredTool.from_function(
                name="get_associated_documents",
                func=self.get_associated_documents,
                description=(
                    "Instantly grabs all manuals, datasheets, SOPs, and failure codes (Cause, Effect, Mode) for a node. "
                    "Input: identifier (string)."
                ),
            ),
        ]

    def create_llm(self):
        bedrock_converse_client = boto3.client(
            "bedrock-runtime",
            region_name="ap-south-1",
        )
        return ChatBedrockConverse(
            client=bedrock_converse_client,
            model="arn:aws:bedrock:ap-south-1:629927974043:inference-profile/apac.anthropic.claude-3-5-sonnet-20241022-v2:0",
            provider="anthropic",
            temperature=0,
            max_tokens=4096,
        )

    def create_agent(self):
        return create_react_agent(
            model=self.llm,
            tools=self.tools,
            prompt=SYSTEM_PROMPT,
            checkpointer=self.checkpointer,
        )

    def ask(self, question: str, thread_id: str = "default") -> str:
        """
        thread_id should be a stable per-conversation/session id (e.g. the
        FastAPI session id or user id). It's what makes 'it' / 'this' /
        'that' work across turns -- without it every call is stateless.
        """
        config = {"configurable": {"thread_id": thread_id}}
        response = self.agent.invoke(
            {"messages": [("user", question)]},
            config=config,
        )
        return response["messages"][-1].content