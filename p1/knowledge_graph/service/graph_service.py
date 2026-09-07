import json
import math
import pandas as pd
from xlsxwriter import relationships
from p1.knowledge_graph.service.ui_to_kg_db_converter import UItoKGConverter

class GraphService:
    SECONDARY_LABELS_LIMITS = [
        ("Sensor", 50),
        ("WorkOrder", 30),
        ("TaskList", 20),
        ("FailureMode", 20),
        ("FailureEffect", 20),
        ("FailureCause", 20),
        ("Document", 20),
        ("FunctionalLocation", 20),
        ("Issue", 20),
        ("Parameter", 20),
        ("Cause", 20),
        ("Check", 20),
    ]

    def __init__(self):
        self.ui_converter = UItoKGConverter()
        self.kg_cur = self.ui_converter.cur
        self.kg_conn = self.ui_converter.conn

        self.TABLE_MAP = {
            "Equipment": {"table": "equipment", "pk": "equipment_uid"},
            "EquipmentPID": {"table": "equipment_pid", "pk": "pid_uid"},
            "EquipmentSAP": {"table": "equipment_sap", "pk": "sap_uid"},
            "WorkOrder": {"table": "work_order", "pk": "wo_uid"},
            "TaskList": {"table": "task_list", "pk": "task_list_uid"},
            "Sensor": {"table": "timeseries_metadata", "pk": "ts_uid"},
            "Document": {"table": "document_metadata", "pk": "document_uid"},
            "FailureMode": {"table": "failure_mode", "pk": "fm_uid"},
            "FailureEffect": {"table": "failure_effect", "pk": "fe_uid"},
            "FailureCause": {"table": "failure_cause", "pk": "fc_uid"},
            "FunctionalLocation": {"table": "functional_location", "pk": "floc_uid"},
        }

    def clean(self, v):
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(v, pd.Timestamp):
            return v.isoformat()
        return v

    def safe_props(self, rec):
        """Clean and extract properties from a record."""
        props = {}
        for k, v in rec.items():
            if v is None:
                props[k] = None
            else:
                v = self.clean(v)
                if v is not None and str(v).strip() != "":
                    props[k] = v
        return props

    def _parse_props(self, raw_props):
        return (
            raw_props if isinstance(raw_props, dict) else json.loads(raw_props or "{}")
        )

    def _format_node(self, row):
        props = self._parse_props(row["properties"])
        if "name" not in props and row.get("name"):
            props["name"] = row["name"]
        return {"id": row["node_id"], "labels": [row["label"]], "properties": props}

    def _format_rel(self, row):
        props = self._parse_props(row["properties"])
        return {
            "id": str(row["relationship_id"]),
            "type": row["rel_type"],
            "source": str(row["from_node_id"]),
            "target": str(row["to_node_id"]),
            "properties": props,
        }

    def delete_node(self, nid=None, label=None, key=None, persist=False):
        """Remove a node and its associated relationships."""
        if persist and nid is not None:
            return self.ui_converter.delete_node(nid)
        return False

    def delete_rel(self, src_nid, tgt_nid, rel_type, persist=False):
        """Remove a specific relationship."""
        if persist:
            return self.ui_converter.delete_rel(src_nid, tgt_nid, rel_type)
        return True

    def update_sync(self, changes):
        """Process a list of graph operations in bulk."""
        results = []
        for op in changes:
            op_type = op.get("type")
            action = op.get("action")
            persist = op.get("persist", True)

            try:
                if op_type == "node":
                    if action == "upsert":
                        nid = self.add_node(
                            op.get("label"),
                            op.get("key"),
                            op.get("properties"),
                            persist=persist,
                            explicit_node_id=op.get("node_id"),
                        )
                        results.append(
                            {"status": "success", "id": nid, "action": action}
                        )
                    elif action == "delete":
                        props_to_delete = op.get("properties")
                        if props_to_delete:
                            null_props = {k: None for k in props_to_delete.keys()}
                            nid = self.add_node(
                                op.get("label"),
                                op.get("key"),
                                null_props,
                                persist=persist,
                                explicit_node_id=op.get("node_id"),
                            )
                            results.append(
                                {
                                    "status": "success",
                                    "id": nid,
                                    "action": "delete_properties",
                                }
                            )
                        else:
                            success = self.delete_node(
                                op.get("node_id"),
                                op.get("label"),
                                op.get("key"),
                                persist=persist,
                            )
                            results.append(
                                {
                                    "status": "success" if success else "failed",
                                    "action": action,
                                }
                            )

                elif op_type == "relationship":
                    if action == "upsert":
                        src_id = op.get("src_id")
                        tgt_id = op.get("tgt_id")

                        if src_id is None and op.get("src_label") and op.get("src_key"):
                            src_id = self.add_node(
                                op.get("src_label"), op.get("src_key"), persist=persist
                            )
                        if tgt_id is None and op.get("tgt_label") and op.get("tgt_key"):
                            tgt_id = self.add_node(
                                op.get("tgt_label"), op.get("tgt_key"), persist=persist
                            )

                        if src_id and tgt_id:
                            self.add_rel(
                                op.get("rel_type"),
                                src_id,
                                tgt_id,
                                op.get("properties"),
                                persist=persist,
                            )
                            results.append({"status": "success", "action": action})
                        else:
                            results.append(
                                {
                                    "status": "error",
                                    "message": "Source or Target node not identified.",
                                    "action": action,
                                }
                            )
                    elif action == "delete":
                        src_id = op.get("src_id")
                        tgt_id = op.get("tgt_id")

                        if src_id is None and op.get("src_label") and op.get("src_key"):
                            src_id = self.add_node(
                                op.get("src_label"), op.get("src_key"), persist=False
                            )
                        if tgt_id is None and op.get("tgt_label") and op.get("tgt_key"):
                            tgt_id = self.add_node(
                                op.get("tgt_label"), op.get("tgt_key"), persist=False
                            )

                        props_to_delete = op.get("properties")
                        if props_to_delete:
                            null_props = {k: None for k in props_to_delete.keys()}
                            self.add_rel(
                                op.get("rel_type"),
                                src_id,
                                tgt_id,
                                null_props,
                                persist=persist,
                            )
                            results.append(
                                {"status": "success", "action": "delete_properties"}
                            )
                        else:
                            success = self.delete_rel(
                                src_id, tgt_id, op.get("rel_type"), persist=persist
                            )
                            results.append(
                                {
                                    "status": "success" if success else "failed",
                                    "action": action,
                                }
                            )
            except Exception as e:
                print(f"Bulk Sync Op Error: {e}")
                results.append({"status": "error", "message": str(e), "op": op})

        return results

    def add_node(self, label, key, props=None, persist=False, explicit_node_id=None):
        if explicit_node_id:
            nid = str(explicit_node_id)
            display_name = props.get("normalized_asset") or key if props else key
            new_props = {"name": display_name}
            new_props.update(self.safe_props(props or {}))
            if persist:
                self.ui_converter.upsert_node(
                    label, nid, new_props.get("name", nid), new_props
                )
            return nid

        if key is None:
            return None
        key_str = str(key).strip().upper()
        if not key_str:
            return None

        table_info = self.TABLE_MAP.get(label)
        if not table_info:
            raise ValueError(f"No TABLE_MAP entry for label={label}")

        pk_col = table_info["pk"]
        table_name = table_info["table"]

        pk_val = props.get(pk_col) if props else None
        if not pk_val:
            pk_val = key_str

        plant_val = props.get("plant_code") or props.get("plant") if props else None
        if plant_val and str(plant_val) not in str(pk_val):
            pk_val = f"{plant_val}-{pk_val}"

        if "#" in pk_val:
            nid = str(pk_val)
        else:
            nid = f"{pk_val}#{table_name}"

        display_name = props.get("normalized_asset") or key_str if props else key_str
        new_props = {"name": display_name}
        new_props.update(self.safe_props(props or {}))

        if persist:
            self.ui_converter.upsert_node(
                label, nid, new_props.get("name", nid), new_props
            )

        return nid

    def add_rel(self, rel_type, src, tgt, props=None, persist=False):
        if src is None or tgt is None:
            return
        rel_type = str(rel_type).strip().upper().replace(" ", "_")

        if src == tgt:
            raise ValueError("Self-connections are not allowed.")

        if persist:
            existing_row = self.ui_converter.check_existing_rel(src, tgt)
            if existing_row:
                existing_type = existing_row["rel_type"]
                existing_src = existing_row["from_node_id"]
                existing_tgt = existing_row["to_node_id"]

                SYMMETRIC_REL_TYPES = {"CONNECTED_TO", "FLOW_TO", "PROCESS_FLOW"}

                if (
                    rel_type in SYMMETRIC_REL_TYPES
                    and existing_type == rel_type
                    and str(existing_src) == str(tgt)
                    and str(existing_tgt) == str(src)
                ):
                    raise ValueError(
                        f"Reverse relationship already exists ({existing_type})."
                    )
                normalized_existing = str(existing_type).strip().upper().replace(" ", "_")
                if rel_type and normalized_existing != rel_type:
                    self.ui_converter.delete_rel(
                        existing_src, existing_tgt, existing_type
                    )
                    print(
                        f"Updated relationship type {existing_type} → {rel_type}"
                    )
                elif not rel_type:
                    rel_type = existing_type
                self.ui_converter.upsert_rel(rel_type, src, tgt, props)
                return

            self.ui_converter.upsert_rel(rel_type, src, tgt, props)

    def build_graph(self, capacity: int = 1000):
        capacity = max(1, min(1000, capacity))

        self.kg_cur.execute("SELECT * FROM public.kg_nodes")
        nodes_out = [self._format_node(r) for r in self.kg_cur.fetchall()]
        all_nodes = nodes_out

        self.kg_cur.execute("SELECT * FROM public.kg_relationships")
        rels_out = [self._format_rel(r) for r in self.kg_cur.fetchall()]

        remaining_capacity = capacity

        selected_nodes = []
        selected_node_ids = set()
        selected_relationships = []

        node_index = 0

        while node_index < len(all_nodes) and remaining_capacity >= 2:
            node = all_nodes[node_index]

            selected_nodes.append(
                {
                    "id": node["id"],
                    "labels": node["labels"],
                }
            )

            selected_node_ids.add(str(node["id"]))
            remaining_capacity -= 2
            node_index += 1

            # After every 2 nodes, attempt to add 1 relationship.
            if len(selected_nodes) % 2 == 0 and remaining_capacity >= 4:
                valid_relationships = [
                    rel
                    for rel in rels_out
                    if str(rel["source"]) in selected_node_ids
                    and str(rel["target"]) in selected_node_ids
                    and rel["id"] not in {
                        selected_rel["id"]
                        for selected_rel in selected_relationships
                    }
                ]

                if valid_relationships:
                    selected_relationships.append(valid_relationships[0])
                    remaining_capacity -= 4

            # After every 10 nodes, attempt to add 2 additional relationships.
            if len(selected_nodes) % 10 == 0:
                for _ in range(2):
                    if remaining_capacity < 4:
                        break

                    valid_relationships = [
                        rel
                        for rel in rels_out
                        if str(rel["source"]) in selected_node_ids
                        and str(rel["target"]) in selected_node_ids
                        and rel["id"] not in {
                            selected_rel["id"]
                            for selected_rel in selected_relationships
                        }
                    ]

                    if not valid_relationships:
                        break

                    selected_relationships.append(valid_relationships[0])
                    remaining_capacity -= 4

        # Once all nodes are exhausted, use any remaining capacity
        # to retrieve additional valid relationships.
        if node_index >= len(all_nodes):
            while remaining_capacity >= 4:
                selected_relationship_ids = {
                    selected_rel["id"]
                    for selected_rel in selected_relationships
                }

                valid_relationships = [
                    rel
                    for rel in rels_out
                    if str(rel["source"]) in selected_node_ids
                    and str(rel["target"]) in selected_node_ids
                    and rel["id"] not in selected_relationship_ids
                ]

                if not valid_relationships:
                    break

                selected_relationships.append(valid_relationships[0])
                remaining_capacity -= 4

        # Use any remaining capacity for properties.
        if remaining_capacity > 0:
            # Add node properties first.
            for selected_node in selected_nodes:
                if remaining_capacity <= 0:
                    break

                node_id = str(selected_node["id"])

                source_node = next(
                    (
                        node
                        for node in all_nodes
                        if str(node["id"]) == node_id
                    ),
                    None,
                )

                if not source_node:
                    continue

                raw_props = source_node.get("properties") or {}

                if not raw_props:
                    continue

                selected_props = {}

                for key, value in raw_props.items():
                    if remaining_capacity <= 0:
                        break

                    selected_props[key] = value
                    remaining_capacity -= 1

                if selected_props:
                    selected_node["properties"] = selected_props

            # Then use any remaining capacity for relationship properties.
            if remaining_capacity > 0:
                for relationship in selected_relationships:
                    if remaining_capacity <= 0:
                        break

                    raw_props = relationship.get("properties") or {}

                    if not raw_props:
                        continue

                    selected_props = {}

                    for key, value in raw_props.items():
                        if remaining_capacity <= 0:
                            break

                        selected_props[key] = value
                        remaining_capacity -= 1

                    if selected_props:
                        relationship["properties"] = selected_props

        return {
            "nodes": selected_nodes,
            "relationships": selected_relationships,
        }

    def _get_applicable_relationships(
        self,
        node_ids,
        loaded_relationship_ids,
        selected_relationship_ids,
    ):
        placeholders = ", ".join(["%s"] * len(node_ids))

        query = f"""
            SELECT *
            FROM public.kg_relationships
            WHERE from_node_id IN ({placeholders})
              AND to_node_id IN ({placeholders})
        """

        params = list(node_ids) + list(node_ids)

        self.kg_cur.execute(query, params)
        relationship_rows = self.kg_cur.fetchall()

        relationships = []

        for row in relationship_rows:
            relationship_id = str(row["relationship_id"])

            if relationship_id in loaded_relationship_ids:
                continue

            if relationship_id in selected_relationship_ids:
                continue

            relationships.append(
                {
                    "id": relationship_id,
                    "type": row["rel_type"],
                    "source": str(row["from_node_id"]),
                    "target": str(row["to_node_id"]),
                }
            )

        return relationships

    def expand_graph(self, expand_node_ids, loaded_relationship_ids, capacity):
        capacity = max(1, min(1000, capacity))

        input_node_ids = {str(node_id) for node_id in (expand_node_ids or [])}
        loaded_relationship_ids = {
            str(rel_id) for rel_id in (loaded_relationship_ids or [])
        }

        if not input_node_ids:
            return {"nodes": [], "relationships": []}

        # Load all nodes and relationships, same access pattern as build_graph().
        self.kg_cur.execute("SELECT * FROM public.kg_nodes")
        all_nodes = [self._format_node(row) for row in self.kg_cur.fetchall()]

        self.kg_cur.execute("SELECT * FROM public.kg_relationships")
        all_relationships = [self._format_rel(row) for row in self.kg_cur.fetchall()]

        # Collect nodes directly connected to the input nodes, excluding the seeds.
        neighbor_node_ids = set()
        for relationship in all_relationships:
            source_id = str(relationship["source"])
            target_id = str(relationship["target"])

            if source_id in input_node_ids:
                neighbor_node_ids.add(target_id)
            if target_id in input_node_ids:
                neighbor_node_ids.add(source_id)

        neighbor_node_ids -= input_node_ids

        if not neighbor_node_ids:
            return {"nodes": [], "relationships": []}

        neighborhood_nodes = [
            node for node in all_nodes if str(node["id"]) in neighbor_node_ids
        ]

        # Select nodes and relationships against a shared capacity budget.
        remaining_capacity = capacity

        selected_nodes = []
        selected_node_ids = set()

        selected_relationships = []
        selected_relationship_ids = set()

        node_index = 0

        while node_index < len(neighborhood_nodes) and remaining_capacity >= 2:
            node = neighborhood_nodes[node_index]

            selected_nodes.append({"id": node["id"], "labels": node["labels"]})
            selected_node_ids.add(str(node["id"]))

            remaining_capacity -= 2
            node_index += 1

            # Relationships may span seed nodes and newly selected nodes.
            all_node_ids = input_node_ids | selected_node_ids

            # Every 2 selected nodes, try to add 1 relationship.
            if len(selected_nodes) % 2 == 0 and remaining_capacity >= 4:
                valid_relationships = [
                    rel
                    for rel in all_relationships
                    if str(rel["source"]) in all_node_ids
                    and str(rel["target"]) in all_node_ids
                    and str(rel["id"]) not in loaded_relationship_ids
                    and str(rel["id"]) not in selected_relationship_ids
                ]

                if valid_relationships:
                    relationship = valid_relationships[0]

                    selected_relationships.append(
                        {
                            "id": relationship["id"],
                            "type": relationship["type"],
                            "source": relationship["source"],
                            "target": relationship["target"],
                        }
                    )

                    selected_relationship_ids.add(str(relationship["id"]))
                    remaining_capacity -= 4

            # Every 10 selected nodes, try to add 2 more relationships.
            if len(selected_nodes) % 10 == 0:
                for _ in range(2):
                    if remaining_capacity < 4:
                        break

                    all_node_ids = input_node_ids | selected_node_ids

                    valid_relationships = [
                        rel
                        for rel in all_relationships
                        if str(rel["source"]) in all_node_ids
                        and str(rel["target"]) in all_node_ids
                        and str(rel["id"]) not in loaded_relationship_ids
                        and str(rel["id"]) not in selected_relationship_ids
                    ]

                    if not valid_relationships:
                        break

                    relationship = valid_relationships[0]

                    selected_relationships.append(
                        {
                            "id": relationship["id"],
                            "type": relationship["type"],
                            "source": relationship["source"],
                            "target": relationship["target"],
                        }
                    )

                    selected_relationship_ids.add(str(relationship["id"]))
                    remaining_capacity -= 4

        # Spend any leftover capacity on further relationships.
        if remaining_capacity >= 4:
            all_node_ids = input_node_ids | selected_node_ids

            valid_relationships = [
                rel
                for rel in all_relationships
                if str(rel["source"]) in all_node_ids
                and str(rel["target"]) in all_node_ids
                and str(rel["id"]) not in loaded_relationship_ids
                and str(rel["id"]) not in selected_relationship_ids
            ]

            for relationship in valid_relationships:
                if remaining_capacity < 4:
                    break

                selected_relationships.append(
                    {
                        "id": relationship["id"],
                        "type": relationship["type"],
                        "source": relationship["source"],
                        "target": relationship["target"],
                    }
                )

                selected_relationship_ids.add(str(relationship["id"]))
                remaining_capacity -= 4

        return {
            "nodes": selected_nodes,
            "relationships": selected_relationships,
        }

    def get_initial_plants(self):
        self.kg_cur.execute("SELECT * FROM public.kg_nodes WHERE label = 'Plant'")
        nodes = [self._format_node(r) for r in self.kg_cur.fetchall()]
        return {"nodes": nodes, "relationships": []}

    def get_initial_graph_by_plants(self, total_nodes: int = 50, parent_node: str = 'Plant'):
        limit_val = total_nodes if total_nodes and total_nodes > 0 else 50
        
        # Fetch parent nodes
        self.kg_cur.execute(
            """
            SELECT * FROM public.kg_nodes 
            WHERE UPPER(label) = UPPER(%s) 
            LIMIT %s
            """,
            (parent_node, limit_val)
        )
        parent_rows = self.kg_cur.fetchall()
        
        if not parent_rows:
            return {"nodes": [], "relationships": []}

        nodes_out = [self._format_node(r) for r in parent_rows]
        seen_node_ids = {n["id"] for n in nodes_out}
        frontier_ids = [n["id"] for n in nodes_out]
        
        remaining_slots = max(0, limit_val - len(nodes_out))
        
        # Fetch connections in hops (BFS) until slots are filled
        while remaining_slots > 0 and frontier_ids:
            self.kg_cur.execute(
                """
                WITH children AS (
                    SELECT
                        p.parent_id,
                        n.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY p.parent_id
                            ORDER BY r.to_node_id
                        ) AS child_rank
                    FROM UNNEST(%s::text[]) p(parent_id)
                    JOIN public.kg_relationships r
                    ON r.from_node_id = p.parent_id

                    JOIN public.kg_nodes n
                    ON n.node_id = r.to_node_id
                )
                SELECT *
                FROM children
                ORDER BY child_rank, parent_id
                LIMIT %s;
                """,
                (frontier_ids, remaining_slots * 10) # Fetch enough to account for duplicates we will filter out
            )
            child_rows = self.kg_cur.fetchall()
            
            if not child_rows:
                break
                
            new_frontier = []
            for r in child_rows:
                if remaining_slots <= 0:
                    break
                node_id = r["node_id"]
                if node_id not in seen_node_ids:
                    seen_node_ids.add(node_id)
                    nodes_out.append(self._format_node(r))
                    new_frontier.append(node_id)
                    remaining_slots -= 1
            
            # Next hop frontier
            frontier_ids = new_frontier

        node_id_list = list(seen_node_ids)

        # Fetch relationships ONLY for the nodes we retrieved (Using UNION ALL)
        self.kg_cur.execute(
            """
            SELECT * FROM public.kg_relationships WHERE from_node_id = ANY(%s)
            UNION ALL
            SELECT * FROM public.kg_relationships WHERE to_node_id = ANY(%s)
            """,
            (node_id_list, node_id_list),
        )
        rel_rows = self.kg_cur.fetchall()

        # Ensure relationships only connect the nodes on screen
        rels_out = []
        seen_rel_ids = set()
        for r in rel_rows:
            rel_id = r["relationship_id"]
            if rel_id not in seen_rel_ids:
                if r["from_node_id"] in seen_node_ids and r["to_node_id"] in seen_node_ids:
                    rels_out.append(self._format_rel(r))
                    seen_rel_ids.add(rel_id)

        return {"nodes": nodes_out, "relationships": rels_out}

    def get_node_by_id(self, node_id: str):
        """Fetch a single node with its full set of properties by node_id.

        Used when the frontend needs the complete properties of one node that is
        already on screen (the graph endpoints trim properties to stay within the
        capacity budget). Returns None if the node_id does not exist.
        """
        node_id = str(node_id).strip()
        if not node_id:
            return None

        self.kg_cur.execute(
            "SELECT * FROM public.kg_nodes WHERE node_id = %s LIMIT 1",
            (node_id,),
        )
        row = self.kg_cur.fetchone()

        if not row:
            print(f"Node '{node_id}' not found in KG DB.")
            return None

        return self._format_node(row)

    def get_relationship_by_id(self, relationship_id: str):
        """Fetch a single relationship with its full set of properties by
        relationship_id. Mirror of get_node_by_id for edges. Returns None if
        the relationship_id does not exist.
        """
        relationship_id = str(relationship_id).strip()
        if not relationship_id:
            return None

        self.kg_cur.execute(
            "SELECT * FROM public.kg_relationships WHERE relationship_id::text = %s LIMIT 1",
            (relationship_id,),
        )
        row = self.kg_cur.fetchone()

        if not row:
            print(f"Relationship '{relationship_id}' not found in KG DB.")
            return None

        return self._format_rel(row)

    def get_neighbors(self, node: str, limit: int = 50):
        limit_val = limit if limit and limit > 0 else 50
        node = str(node).strip().upper()

        # 1. Find the starting node
        self.kg_cur.execute(
            """
            SELECT * FROM public.kg_nodes
            WHERE UPPER(name) = %s 
               OR UPPER(properties->>'plant_code') = %s
               OR UPPER(node_id) = %s
            LIMIT 1
            """,
            (node, node, node),
        )
        start_row = self.kg_cur.fetchone()

        if not start_row:
            print(f"Node '{node}' not found in KG DB.")
            return {"nodes": [], "relationships": []}

        nodes_out = [self._format_node(start_row)]
        seen_node_ids = {start_row["node_id"]}
        start_node_id = start_row["node_id"]
        
        remaining_slots = max(0, limit_val - 1)

        # 2. Get IMMEDIATE (1-hop) neighbors only
        if remaining_slots > 0:
            self.kg_cur.execute(
                """
                SELECT DISTINCT n.* 
                FROM public.kg_relationships r
                JOIN public.kg_nodes n ON n.node_id = CASE 
                    WHEN r.from_node_id = %s THEN r.to_node_id 
                    ELSE r.from_node_id 
                END
                WHERE r.from_node_id = %s OR r.to_node_id = %s
                LIMIT %s
                """,
                (start_node_id, start_node_id, start_node_id, remaining_slots)
            )
            child_rows = self.kg_cur.fetchall()
            
            for r in child_rows:
                node_id = r["node_id"]
                if node_id not in seen_node_ids:
                    seen_node_ids.add(node_id)
                    nodes_out.append(self._format_node(r))

        # 3. Fetch relationships connecting the retrieved nodes
        node_id_list = list(seen_node_ids)
        self.kg_cur.execute(
            """
            SELECT * FROM public.kg_relationships WHERE from_node_id = ANY(%s)
            UNION ALL
            SELECT * FROM public.kg_relationships WHERE to_node_id = ANY(%s)
            """,
            (node_id_list, node_id_list),
        )
        rel_rows = self.kg_cur.fetchall()

        rels_out = []
        seen_rel_ids = set()
        for r in rel_rows:
            rel_id = r["relationship_id"]
            if rel_id not in seen_rel_ids:
                if r["from_node_id"] in seen_node_ids and r["to_node_id"] in seen_node_ids:
                    rels_out.append(self._format_rel(r))
                    seen_rel_ids.add(rel_id)

        return {"nodes": nodes_out, "relationships": rels_out}

    def get_all_nodes(self, label: str = None, search: str = None):
        where_clauses = []
        params = []

        if label:
            where_clauses.append("UPPER(label) = UPPER(%s)")
            params.append(label)
        if search:
            where_clauses.append("name ILIKE %s")
            params.append(f"%{search}%")

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        self.kg_cur.execute(
            f"SELECT node_id, label, name FROM public.kg_nodes {where_sql} ORDER BY label, name",
            params,
        )
        return [
            {"id": r["node_id"], "label": r["label"], "name": r["name"]}
            for r in self.kg_cur.fetchall()
        ]

    def get_graph_schema(self):
        self.kg_cur.execute(
            "SELECT DISTINCT rel_type FROM public.kg_relationships ORDER BY rel_type"
        )
        rel_types = [r["rel_type"] for r in self.kg_cur.fetchall()]

        self.kg_cur.execute("""
            SELECT DISTINCT key
            FROM public.kg_nodes, jsonb_object_keys(properties::jsonb) AS key
            ORDER BY key
        """)
        node_property_keys = [r["key"] for r in self.kg_cur.fetchall()]

        self.kg_cur.execute("""
            SELECT DISTINCT key
            FROM public.kg_relationships, jsonb_object_keys(properties::jsonb) AS key
            ORDER BY key
        """)
        rel_property_keys = [r["key"] for r in self.kg_cur.fetchall()]

        return {
            "rel_types": rel_types,
            "node_property_keys": node_property_keys,
            "rel_property_keys": rel_property_keys,
        }

    def get_equipment_tags(self, equipment_name: str):
        self.kg_cur.execute(
            """
            SELECT n.node_id, n.label, n.name, n.properties 
            FROM public.kg_nodes n
            JOIN public.kg_relationships r ON r.to_node_id = n.node_id
            JOIN public.kg_nodes eq ON r.from_node_id = eq.node_id
            WHERE r.rel_type = 'HAS_TAG' 
              AND UPPER(eq.name) = UPPER(%s)
              AND eq.label = 'Equipment'
              AND n.label = 'Sensor'
        """,
            (str(equipment_name),),
        )

        return [self._format_node(r) for r in self.kg_cur.fetchall()]

    def get_unique_equipments(self):
        self.kg_cur.execute("""
            SELECT DISTINCT name 
            FROM public.kg_nodes 
            WHERE label = 'Equipment' AND name IS NOT NULL
            ORDER BY name
        """)
        return [r["name"] for r in self.kg_cur.fetchall()]

    def close(self):
        if hasattr(self, "ui_converter") and self.ui_converter:
            self.ui_converter.close()
