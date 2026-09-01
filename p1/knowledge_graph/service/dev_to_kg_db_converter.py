import json
import os
import sys
import datetime
import time
import pandas as pd
from decimal import Decimal
from psycopg2.extras import RealDictCursor, execute_values
from p2.utility.database_driver import PostgresDriver

# Ensure project root is in python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


# Serializer
def json_serial(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError("Type %s not serializable" % type(obj))


# GraphConverter
# stores results into kg_nodes / kg_relationships tables
class GraphConverter:
    def __init__(self, plant_code_id=None):
        if plant_code_id:
            from utility.middleware import plant_code_ctx
            plant_code_ctx.set(plant_code_id)
            
        self.source_conn = None
        self.graph_conn = None
        self.graph_cur = None
        self.symbolic_conn = None

        # ── in-memory graph state (mirrors GraphService) ──────────────────
        self.nodes = {}  # nid -> node_dict
        self.relationships = []
        self.node_map = {}  # (label, identity_id) -> nid
        self.business_map = {}  # (label, key_str, plant_code) -> [nid]
        self.seen_rels = set()
        self.edge_map = {}  # (src, tgt) -> {rel_id, type, priority}
        self.current_source_table = None

        self._connect()

    def _create_table(self):
        try:
            # CREATE NODES TABLE
            self.graph_cur.execute("""
            CREATE TABLE IF NOT EXISTS public.kg_nodes (
                node_id      TEXT PRIMARY KEY,
                label        TEXT NOT NULL,
                name         TEXT NOT NULL,
                properties   JSONB DEFAULT '{}',
                lineage      JSONB DEFAULT '{}',
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)

            # CREATE RELATIONSHIPS TABLE
            self.graph_cur.execute("""
            CREATE TABLE IF NOT EXISTS public.kg_relationships (
                relationship_id TEXT PRIMARY KEY,
                from_node_id    TEXT NOT NULL REFERENCES public.kg_nodes(node_id) ON DELETE CASCADE,
                to_node_id      TEXT NOT NULL REFERENCES public.kg_nodes(node_id) ON DELETE CASCADE,
                rel_type        TEXT NOT NULL,
                properties      JSONB DEFAULT '{}',
                lineage         JSONB DEFAULT '{}',
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)

            # CREATE INDEXES
            self.graph_cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_nodes_label
                ON public.kg_nodes(label);
            """)

            self.graph_cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_rel_from
                ON public.kg_relationships(from_node_id);
            """)

            self.graph_cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_rel_to
                ON public.kg_relationships(to_node_id);
            """)

            self.graph_conn.commit()
            print("Knowledge Graph schema is ready.")

        except Exception as e:
            self.graph_conn.rollback()
            print(f"Schema creation failed: {e}")
            raise

    # Connection helpers
    def _connect(self):
        try:
            # Connect to source DEV
            dev_driver = PostgresDriver()
            dev_driver.connect()
            self.source_conn = dev_driver.conn

            # Connect to Graph KG
            kg_driver = PostgresDriver()
            kg_driver.connect()
            self.graph_conn = kg_driver.conn

            try:
                sym_driver = PostgresDriver()
                sym_driver.connect()
                self.symbolic_conn = sym_driver.conn
            except Exception as e:
                print(f"Symbolic Connection Error: {e}")
                self.symbolic_conn = None

            self.graph_cur = self.graph_conn.cursor(cursor_factory=RealDictCursor)
            self._create_table()

        except Exception as e:
            print(f"Connection Error: {e}")
            self.source_conn = None
            self.graph_conn = None

    def reconnect(self):
        try:
            if self.source_conn:
                self.source_conn.close()
            if self.graph_conn:
                self.graph_conn.close()
            if self.symbolic_conn:
                self.symbolic_conn.close()
        except Exception:
            pass
        self._connect()

    # Read helper
    def read(self, table_name):
        query = f"SELECT * FROM public.{table_name}"
        return pd.read_sql(query, self.source_conn)

    # GraphService.clean / safe_props
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
        props = {}
        for k, v in rec.items():
            v = self.clean(v)
            if v is not None and str(v).strip() != "":
                props[k] = v
        return props

    # GraphService.best_equipment_key
    def best_equipment_key(self, rec):
        for col in ("normalized_asset", "equipment_id", "asset_code", "equipment_tag"):
            val = rec.get(col)
            if val is not None and str(val).strip() != "":
                return str(val).strip().upper()
        return None

    # GraphService.init_graph
    def init_graph(self):
        self.nodes = {}
        self.relationships = []
        self.node_map = {}
        self.business_map = {}
        self.seen_rels = set()
        self.edge_map = {}
        self.seen_rids = set()

    # GraphService.update_node_props
    def update_node_props(self, nid, props, source=None):
        if nid not in self.nodes:
            return
        ex_node = self.nodes[nid]
        ex_props = ex_node.setdefault("properties", {})
        ex_lineage = ex_node.setdefault("lineage", {})

        src = source or self.current_source_table or "system_generated"

        if props:
            safe_p = self.safe_props(props)
            for k, v in safe_p.items():
                # PRESERVE: If the property was manually edited from the UI, do not overwrite it!
                if ex_lineage.get(k) == "ui_update":
                    continue
                ex_props[k] = v

                existing_sources = ex_lineage.get(k, [])
                # Convert string lineage to list
                if isinstance(existing_sources, str):
                    existing_sources = [existing_sources]

                # Add new source only if not already present
                if src not in existing_sources:
                    existing_sources.append(src)

                ex_lineage[k] = existing_sources
            if safe_p.get("name"):
                existing_name_lineage = ex_lineage.get("name", [])

                if isinstance(existing_name_lineage, str):
                    existing_name_lineage = [existing_name_lineage]

                if "ui_update" not in existing_name_lineage:
                    ex_props["name"] = safe_p["name"]

                    if src not in existing_name_lineage:
                        existing_name_lineage.append(src)

                    ex_lineage["name"] = existing_name_lineage

    # GraphService.add_node
    def add_node(self, label, key, props=None, persist=False, source=None):
        if key is None:
            return None
        key_str = str(key).strip().upper()
        if key_str == "" or key_str.lower() == "nan":
            return None

        db_id = None
        plant_code = ""
        asset_code = ""
        equipment_id = ""

        if props:
            db_id = (
                props.get("pid_uid")
                or props.get("asset_uid")
                or props.get("wo_uid")
                or props.get("task_list_uid")
                or props.get("document_uid")
                or props.get("id")
                or props.get("uid")
            )
            if label == "Plant":
                plant_code = ""
            else:
                plant_code = str(props.get("plant_code") or props.get("plant_code_id") or "").strip().upper()
                asset_code = str(props.get("asset_code", "")).strip().upper()
                equipment_id = str(props.get("equipment_id", "")).strip().upper()

        business_key = (label, key_str, plant_code)
        src = source or self.current_source_table or "system_generated"

        # ── merge / disambiguation ──
        match_ids = []
        if business_key in self.business_map:
            bm = self.business_map[business_key]
            match_ids.extend(bm if isinstance(bm, list) else [bm])
        if plant_code != "" and (label, key_str, "") in self.business_map:
            res = self.business_map[(label, key_str, "")]
            match_ids.extend(res if isinstance(res, list) else [res])

        table_name = (
            str(self.current_source_table).replace("table.", "")
            if self.current_source_table
            else "unknown"
        )
        if label == "FunctionalLocation" and table_name != "functional_location":
            table_name = "functional_location"

        TABLE_PK_MAP = {
            "equipment_pid": "pid_uid",
            "work_order": "wo_uid",
            "task_list": "task_list_uid",
            "timeseries_metadata": "ts_uid",
            "document_metadata": "document_uid",
            "failure_mode": "fm_uid",
            "failure_effect": "fe_uid",
            "failure_cause": "fc_uid",
            "equipment_sap": "sap_uid",
            "functional_location": "floc_uid",
        }

        primary_key_col = TABLE_PK_MAP.get(table_name)
        primary_key = props.get(primary_key_col) if props and primary_key_col else None

        # Explicit exception: Plant and FunctionalLocation nodes are extracted from aggregate fields, so they won't have the table's native PK in their properties.
        if not primary_key and label not in (
            "Plant",
            "FunctionalLocation",
            "Issue",
            "Parameter",
            "Cause",
            "Check",
        ):
            print(
                f"Skipping node without PK "
                f"| table={table_name} "
                f"| label={label} "
                f"| key={key_str}"
            )
            return None

        # Create node ID falling back to key_str if primary_key is missing (e.g. Plant/FunctionalLocation)
        base_id = primary_key if primary_key else key_str
        nid = f"{base_id}#{table_name}"
        identity_id = nid

        if match_ids:
            for eid in match_ids:
                if eid == nid:
                    if props:
                        self.update_node_props(eid, props, source=src)
                    return eid

        if (label, identity_id) in self.node_map:
            if props:
                self.update_node_props(nid, props, source=src)
            return nid

        # ── register ──
        self.node_map[(label, identity_id)] = nid

        # Helper to add an alias to business_map safely
        def add_alias(alias_key):
            if alias_key not in self.business_map:
                self.business_map[alias_key] = []
            bm = self.business_map[alias_key]
            if isinstance(bm, list):
                if nid not in bm:
                    bm.append(nid)
            else:
                if bm != nid:
                    self.business_map[alias_key] = [bm, nid]

        add_alias(business_key)

        # Register Equipment Aliases so child tables can link correctly
        if label == "Equipment":
            if equipment_id and equipment_id != "NONE":
                add_alias(("Equipment", equipment_id, plant_code))
            if asset_code and asset_code != "NONE":
                add_alias(("Equipment", asset_code, plant_code))

        # Strictly map the UI 'name' field to 'normalized_asset' for traceability
        node_name = key_str
        if props and label == "Equipment":
            norm_asset = props.get("normalized_asset")
            if norm_asset and str(norm_asset).strip() != "":
                node_name = str(norm_asset).strip()

        new_props = {"name": node_name}
        new_lineage = {"name": [src]}
        if label == "Equipment":
            new_props["equipment_id"] = key_str
            new_lineage["equipment_id"] = [src]
        if plant_code:
            new_props["plant_code"] = plant_code
            new_lineage["plant_code"] = [src]
        if asset_code:
            new_props["asset_code"] = asset_code
            new_lineage["asset_code"] = [src]

        safe_p = self.safe_props(props or {})
        new_props.update(safe_p)
        for k in safe_p:
            new_lineage[k] = [src]

        if nid not in self.nodes:
            self.nodes[nid] = {
                "id": nid,
                "labels": [label],
                "properties": new_props,
                "lineage": new_lineage,
            }
        else:
            self.update_node_props(nid, props, source=src)

        return nid

    # GraphService.find_eq
    def find_eq(self, raw, plant=None, db_id=None, validators=None):
        if raw is None:
            return None
        k = str(raw).strip().upper()
        p = str(plant or "").strip().upper()

        def resolve_from_list(res):
            if not res:
                return None
            if not isinstance(res, list):
                return res
            if db_id:
                for nid in res:
                    ex_db_id = self.nodes[nid].get("properties", {}).get(
                        "pid_uid"
                    ) or self.nodes[nid].get("properties", {}).get("id")
                    if ex_db_id and str(ex_db_id) == str(db_id):
                        return nid
            if validators and len(res) > 1:
                best_nid, max_matches = res[0], -1
                for nid in res:
                    matches = sum(
                        1
                        for vk, vv in validators.items()
                        if vv and self.nodes[nid].get("properties", {}).get(vk) == vv
                    )
                    if matches > max_matches:
                        max_matches, best_nid = matches, nid
                if max_matches > 0:
                    return best_nid
            return res[0]

        res = self.business_map.get(("Equipment", k, p))
        if res is not None:
            v = resolve_from_list(res)
            if v is not None:
                return v
        if p:
            res = self.business_map.get(("Equipment", k, ""))
            if res is not None:
                v = resolve_from_list(res)
                if v is not None:
                    return v
        for (l, n, pc), nid_list in self.business_map.items():
            if l == "Equipment" and n == k:
                v = resolve_from_list(nid_list)
                if v is not None:
                    return v
        return None

    # GraphService.ensure_eq_placeholder
    def ensure_eq_placeholder(self, raw, plant=None, db_id=None, validators=None):
        if raw is None:
            return None
        k = str(raw).strip().upper()
        if not k:
            return None
        nid = self.find_eq(k, plant, db_id, validators)
        if nid is not None:
            return nid
        props = {"equipment_id": k, "plant_code": plant, "inferred": True}
        if db_id:
            props["pid_uid"] = db_id
        if validators:
            for vk, vv in validators.items():
                if vv:
                    props[vk] = vv
        return self.add_node("Equipment", k, props)

    # GraphService.add_rel
    def add_rel(self, rel_type, src, tgt, props=None, priority=2, lineage_info=None):
        if src is None or tgt is None:
            return
        if src == tgt:
            return
        rel_type = str(rel_type).strip().upper().replace(" ", "_")
        edge_key = (src, tgt)
        rev_key = (tgt, src)

        if rev_key in self.edge_map:
            existing_rev = self.edge_map[rev_key]
            if existing_rev["type"] == rel_type:
                if rel_type in (
                    "CONNECTED_TO",
                    "PROCESS_FLOW",
                    "FLOW_TO",
                    "HAS_EQUIPMENT",
                    "LOCATED_AT",
                ):
                    return

        if edge_key in self.edge_map:
            existing = self.edge_map[edge_key]
            if existing["type"] == rel_type:
                for rel in self.relationships:
                    if rel["id"] == existing["rel_id"]:
                        for k, v in (props or {}).items():
                            rel["properties"][k] = v
                        break
                if priority > existing["priority"]:
                    self.edge_map[edge_key]["priority"] = priority
                return
            if priority > existing["priority"]:
                for rel in self.relationships:
                    if rel["id"] == existing["rel_id"]:
                        rel["type"] = rel_type
                        rel["properties"] = props or {}
                        break
                self.edge_map[edge_key] = {
                    "rel_id": existing["rel_id"],
                    "type": rel_type,
                    "priority": priority,
                }
            return

        rid = None
        if props:
            tbl_name = (
                str(self.current_source_table).replace("table.", "")
                if self.current_source_table
                else None
            )
            if tbl_name == "equipment_connectivity" and props.get("connectivity_uid"):
                rid = f"{props['connectivity_uid']}#equipment_connectivity"
            elif tbl_name == "asset_relationship" and props.get("relationship_uid"):
                rid = f"{props['relationship_uid']}#asset_relationship"

        if not rid:
            # Enforce strict uid#tablename format without extra suffixes
            tbl_name = (
                str(self.current_source_table).replace("table.", "")
                if self.current_source_table
                else ""
            )

            # Since Plant nodes now end in #equipment, prevent the script from extracting the Plant Code as the base UID
            base_uid = tgt.split("#")[0] if "#" in tgt else tgt
            if (
                tbl_name
                and src.endswith(f"#{tbl_name}")
                and not (src.startswith("CP") and "plant" not in src.lower())
            ):
                base_uid = src.split("#")[0] if "#" in src else src
            elif tbl_name and tgt.endswith(f"#{tbl_name}"):
                base_uid = tgt.split("#")[0] if "#" in tgt else tgt

            # Use the actual source table name so the frontend can trace it back for updates
            actual_table = tbl_name if tbl_name else "unknown"
            rid = f"{base_uid}#{actual_table}"

        # Deduplicate identical IDs generated from the same row (e.g. an equipment having two PART_OF relationships)
        if not hasattr(self, "seen_rids"):
            self.seen_rids = set(r["id"] for r in self.relationships)

        # Force rid to lowercase to prevent Postgres case-insensitive constraint collisions
        rid = str(rid).lower()
        original_rid = rid
        counter = 1
        while rid in self.seen_rids:
            parts = original_rid.split("#", 1)
            if len(parts) == 2:
                rid = f"{parts[0]}_{counter}#{parts[1]}"
            else:
                rid = f"{original_rid}_{counter}"
            counter += 1

        self.seen_rids.add(rid)

        self.relationships.append(
            {
                "id": rid,
                "type": rel_type,
                "source": src,
                "target": tgt,
                "properties": props or {},
                "lineage": {"sources": lineage_info or []},
            }
        )

        self.edge_map[edge_key] = {
            "rel_id": rid,
            "type": rel_type,
            "priority": priority,
        }

    # build_graph  (reads from DecisionOps DEV)
    def build_graph(self):
        self.init_graph()

        table_data = {}
        target_tables = [
            "equipment_pid",
            "equipment_connectivity",
            "work_order",
            "task_list",
            "timeseries_metadata",
            "document_metadata",
            "asset_relationship",
            "failure_mode",
            "failure_effect",
            "failure_cause",
            "equipment_sap",
            "functional_location",
        ]
        for t in target_tables:
            try:
                table_data[t] = self.read(t)
            except Exception as e:
                self.source_conn.rollback()
                print(f"  Skipping table {t}: {e}")
                table_data[t] = pd.DataFrame()

        equipment = table_data["equipment_pid"]
        timeseries = table_data["timeseries_metadata"]
        work_order = table_data["work_order"]
        task_list = table_data["task_list"]
        documents = table_data["document_metadata"]
        connectivity = table_data["equipment_connectivity"]
        asset_rel = table_data["asset_relationship"]
        failure_mode = table_data["failure_mode"]
        failure_effect = table_data["failure_effect"]
        failure_cause = table_data["failure_cause"]
        sap = table_data["equipment_sap"]
        floc = table_data["functional_location"]

        eq_map = {}
        plant_map = {}
        self.all_equipment = []
        self.equipment_with_incoming = set()

        # ── Equipment + Plant ───────────────────────────────────────────
        self.current_source_table = "table.equipment_pid"
        for _, row in equipment.iterrows():
            rec = row.to_dict()
            key = self.best_equipment_key(rec)
            if not key:
                continue
            # props = self.safe_props(rec)
            # eq_id = self.add_node("Equipment", key, props)
            props = self.safe_props(rec)

            props["_source_table"] = "equipment_pid"
            props["_source_pk"] = rec.get("pid_uid")

            eq_id = self.add_node("Equipment", key, props)
            eq_map[key] = eq_id

            plant = rec.get("plant_code") or rec.get("plant_code_id")
            if plant and str(plant).strip():
                plant_code = str(plant).strip().upper()
                if plant_code not in plant_map:
                    plant_map[plant_code] = self.add_node(
                        "Plant", plant_code, {"plant_code": plant_code}
                    )
                self.all_equipment.append((plant_map[plant_code], eq_id))

            sup_eq = rec.get("superior_equipment_id")
            if sup_eq and str(sup_eq).strip():
                sup_id = self.find_eq(str(sup_eq).strip(), plant)
                if sup_id:
                    self.add_rel("PART_OF", eq_id, sup_id, priority=10)

            sup_floc = rec.get("superior_functional_location")
            if sup_floc and str(sup_floc).strip():
                fid = self.add_node(
                    "FunctionalLocation", str(sup_floc).strip(), {"plant_code": plant}
                )
                self.add_rel("PART_OF", eq_id, fid, priority=5)

        # ── Sensors ──────────────────────────────────────────────────────
        self.current_source_table = "table.timeseries_metadata"
        for _, row in timeseries.iterrows():
            rec = row.to_dict()
            tag = rec.get("tag_name")
            if not tag or str(tag).strip() == "":
                continue
            sid = self.add_node("Sensor", str(tag).strip(), self.safe_props(rec))
            eq_id = self.find_eq(
                rec.get("equipment_id"), rec.get("plant_code"), rec.get("pid_uid")
            ) or self.find_eq(
                rec.get("normalized_asset"),
                rec.get("plant_code"),
                rec.get("pid_uid"),
            )
            if eq_id is not None:
                self.add_rel("HAS_TAG", eq_id, sid)

        # ── Work Orders ───────────────────────────────────────────────────
        self.current_source_table = "table.work_order"
        for _, row in work_order.iterrows():
            rec = row.to_dict()
            key = rec.get("wo_number") or rec.get("wo_uid")
            if not key:
                continue
            wid = self.add_node("WorkOrder", str(key), self.safe_props(rec))
            eq_id = self.find_eq(
                rec.get("equipment_ref"),
                rec.get("plant_code"),
                rec.get("pid_uid"),
            ) or self.find_eq(
                rec.get("equipment_id"), rec.get("plant_code"), rec.get("pid_uid")
            )
            if eq_id is not None:
                self.add_rel("HAS_WORK_ORDER", eq_id, wid)

        # ── Task Lists ────────────────────────────────────────────────────
        self.current_source_table = "table.task_list"
        for _, row in task_list.iterrows():
            rec = row.to_dict()
            key = rec.get("task_list_id") or rec.get("task_list_uid")
            if not key:
                continue
            tid = self.add_node("TaskList", str(key), self.safe_props(rec))
            eq_id = self.find_eq(
                rec.get("equipment_ref"),
                rec.get("plant_code"),
                rec.get("pid_uid"),
            ) or self.find_eq(
                rec.get("equipment_id"), rec.get("plant_code"), rec.get("pid_uid")
            )
            if eq_id is not None:
                self.add_rel("HAS_TASK_LIST", eq_id, tid)

        # ── Documents ─────────────────────────────────────────────────────
        self.current_source_table = "table.document_metadata"
        for _, row in documents.iterrows():
            rec = row.to_dict()
            key = rec.get("document_id") or rec.get("document_uid")
            if not key:
                continue
            raw_type = str(rec.get("document_type") or "Document").strip()
            label = "".join(ch for ch in raw_type if ch.isalnum()) or "Document"
            did = self.add_node(label, str(key), self.safe_props(rec))
            validators = {
                "asset_code": rec.get("asset_code"),
                "floc": rec.get("floc") or rec.get("functional_location"),
                "drawing_number": rec.get("drawing_number"),
            }
            eq_id = self.find_eq(
                rec.get("equipment_id"),
                rec.get("plant_code"),
                rec.get("pid_uid"),
                validators=validators,
            ) or self.find_eq(
                rec.get("equipment_ref"),
                rec.get("plant_code"),
                rec.get("pid_uid"),
                validators=validators,
            )
            if eq_id is not None:
                self.add_rel("HAS_DOCUMENT", eq_id, did)

        # ── SAP (Merged directly into Parent Equipment!) ──────────────
        for df, tbl in [
            (sap, "table.equipment_sap"),
        ]:
            self.current_source_table = tbl
            for _, row in df.iterrows():
                rec = row.to_dict()
                key = self.best_equipment_key(rec)
                if not key:
                    continue
                validators = {
                    "asset_code": rec.get("asset_code"),
                    "floc": rec.get("floc") or rec.get("functional_location"),
                    "drawing_number": rec.get("drawing_number"),
                }
                eq_id = self.find_eq(
                    rec.get("equipment_id"),
                    rec.get("plant_code"),
                    rec.get("pid_uid"),
                    validators=validators,
                ) or self.find_eq(
                    rec.get("normalized_asset"),
                    rec.get("plant_code"),
                    rec.get("pid_uid"),
                    validators=validators,
                )
                if eq_id is None:
                    # Create inferred parent Equipment placeholder if not exists
                    # eq_id = self.ensure_eq_placeholder(key, rec.get("plant_code"), rec.get("pid_uid"), validators=validators)
                    if not eq_id:
                        print(f"Equipment not found for PID/SAP merge: {key}")
                        continue
                if eq_id is not None:
                    # Merge properties directly into the parent Equipment node
                    self.update_node_props(eq_id, rec, source=tbl)

        # ── Failure Modes / Effects / Causes ─────────────────────────────
        for df, label, rel, tbl in [
            (failure_mode, "FailureMode", "HAS_FAILURE_MODE", "table.failure_mode"),
            (
                failure_effect,
                "FailureEffect",
                "HAS_FAILURE_EFFECT",
                "table.failure_effect",
            ),
            (failure_cause, "FailureCause", "HAS_FAILURE_CAUSE", "table.failure_cause"),
        ]:
            self.current_source_table = tbl
            for _, row in df.iterrows():
                rec = row.to_dict()
                code_col = next(
                    (c for c in rec if c.endswith("_code") and c != "plant_code"), None
                )
                uid_col = next((c for c in rec if c.endswith("_uid")), None)
                key = str(
                    (rec.get(code_col) if code_col and rec.get(code_col) else None)
                    or (rec.get(uid_col) if uid_col and rec.get(uid_col) else None)
                    or rec.get("id")
                    or "unknown"
                )
                nid = self.add_node(label, key, self.safe_props(rec))
                eq_id = self.find_eq(
                    rec.get("equipment_id"),
                    rec.get("plant_code"),
                    rec.get("pid_uid"),
                ) or self.find_eq(
                    rec.get("equipment_id_readable"),
                    rec.get("plant_code"),
                    rec.get("pid_uid"),
                )
                if eq_id is not None:
                    self.add_rel(rel, eq_id, nid)

        # ── Functional Location ───────────────────────────────────────────
        self.current_source_table = "table.functional_location"
        for _, row in floc.iterrows():
            rec = row.to_dict()
            floc_key = rec.get("floc")
            if not floc_key:
                continue
            fid = self.add_node(
                "FunctionalLocation", str(floc_key), self.safe_props(rec)
            )
            eq_id = self.find_eq(
                rec.get("normalized_asset"),
                rec.get("plant_code"),
                rec.get("pid_uid"),
            )
            if eq_id is not None:
                self.add_rel("LOCATED_AT", eq_id, fid)

        # ── Connectivity ──────────────────────────────────────────────────
        self.current_source_table = "table.equipment_connectivity"
        for _, row in connectivity.iterrows():
            rec = row.to_dict()
            src = self.find_eq(
                rec.get("from_equipment_ref"),
                rec.get("plant_code"),
                rec.get("from_pid_uid"),
            )

            tgt = self.find_eq(
                rec.get("to_equipment_ref"),
                rec.get("plant_code"),
                rec.get("to_pid_uid"),
            )

            # Skip invalid refs
            if not src or not tgt:
                continue
            self.equipment_with_incoming.add(tgt)

            rel_name = str(rec.get("connection_type") or "CONNECTED_TO").strip().upper()

            self.add_rel(
                rel_name,
                src,
                tgt,
                self.safe_props(rec),
                lineage_info=[
                    {
                        "table": "table.equipment_connectivity",
                        "from_column": "from_equipment_ref",
                        "to_column": "to_equipment_ref",
                        "logic": rel_name,
                    }
                ],
            )

        # ── Asset Relationship ────────────────────────────────────────────
        self.current_source_table = "table.asset_relationship"
        for _, row in asset_rel.iterrows():
            rec = row.to_dict()
            src = self.find_eq(rec.get("parent_ref"), rec.get("plant_code"))

            tgt = self.find_eq(rec.get("child_ref"), rec.get("plant_code"))
            # Skip invalid references
            if not src or not tgt:
                continue
            self.equipment_with_incoming.add(tgt)

            rel_name = str(rec.get("relationship_type") or "FLOW_TO").strip().upper()

            self.add_rel(
                rel_name,
                src,
                tgt,
                self.safe_props(rec),
                lineage_info=[
                    {
                        "table": "table.asset_relationship",
                        "from_column": "parent_ref",
                        "to_column": "child_ref",
                        "logic": rel_name,
                    }
                ],
            )

        # ── Connect Plant to Starting Points ──────────────────────────────
        for plant_id, eq_id in self.all_equipment:
            if eq_id not in self.equipment_with_incoming:
                self.add_rel("HAS_EQUIPMENT", plant_id, eq_id)

    # Persist in-memory graph into kg_nodes / kg_relationships
    def _persist_nodes(self):
        data = []
        for nid, node in self.nodes.items():
            label = node["labels"][0]
            name = node["properties"].get("name", str(nid))
            props = node["properties"]
            lineage = node.get("lineage", {})
            data.append(
                (
                    str(nid),
                    label,
                    name,
                    json.dumps(props, default=json_serial),
                    json.dumps(lineage, default=json_serial),
                    datetime.datetime.now(),
                )
            )

        if not data:
            print("No nodes to persist.")
            return

        insert_query = """
            INSERT INTO public.kg_nodes
                (node_id, label, name, properties, lineage, updated_at)
            VALUES %s
            ON CONFLICT (node_id) DO UPDATE SET
                name       = EXCLUDED.name,
                properties = public.kg_nodes.properties || EXCLUDED.properties,
                lineage    = public.kg_nodes.lineage    || EXCLUDED.lineage,
                updated_at = CURRENT_TIMESTAMP
        """
        try:
            execute_values(self.graph_cur, insert_query, data, page_size=5000)
            self.graph_conn.commit()
        except Exception as e:
            self.graph_conn.rollback()
            print(f"Bulk node persist error: {e}")

    def _persist_relationships(self):
        data = []
        for rel in self.relationships:
            rid = str(rel["id"])
            src_id = str(rel["source"])
            tgt_id = str(rel["target"])
            rel_type = rel["type"]
            props = rel["properties"]
            lineage = rel.get("lineage") or {"source": "decisionops_cdm_dev"}
            data.append(
                (
                    rid,
                    src_id,
                    tgt_id,
                    rel_type,
                    json.dumps(props, default=json_serial),
                    json.dumps(lineage, default=json_serial),
                    datetime.datetime.now(),
                )
            )

        # DEBUG CHECK: Ensure no duplicates in data before persist!
        rid_counts = {}
        for row in data:
            rid_counts[row[0]] = rid_counts.get(row[0], 0) + 1
        dups = [r for r, count in rid_counts.items() if count > 1]
        if dups:
            print(
                f"FATAL: Duplicate relationship IDs detected in data array! {dups}"
            )

        if not data:
            print("No relationships to persist.")
            return

        insert_query = """
            INSERT INTO public.kg_relationships
                (relationship_id, from_node_id, to_node_id, rel_type, properties, lineage, updated_at)
            VALUES %s
            ON CONFLICT (relationship_id) DO UPDATE SET
                properties = public.kg_relationships.properties || EXCLUDED.properties,
                lineage    = public.kg_relationships.lineage    || EXCLUDED.lineage,
                updated_at = CURRENT_TIMESTAMP
        """
        try:
            execute_values(self.graph_cur, insert_query, data, page_size=5000)
            self.graph_conn.commit()
        except Exception as e:
            self.graph_conn.rollback()
            print(f"Bulk relationship persist error: {e}")

    def _clear_tables(self):
        try:
            # Delete only relationships that do not have 'ui_update' in their lineage
            self.graph_cur.execute("""
                DELETE FROM public.kg_relationships 
                WHERE NOT (
                    lineage::jsonb @> '{"source": "ui_update"}' OR 
                    lineage::jsonb @> '{"relationship": "ui_update"}' OR 
                    EXISTS (
                        SELECT 1 FROM jsonb_each_text(lineage::jsonb) WHERE value = 'ui_update'
                    )
                );
            """)
            # Delete only nodes that do not have any property marked with 'ui_update'
            self.graph_cur.execute("""
                DELETE FROM public.kg_nodes 
                WHERE NOT (
                    lineage::jsonb @> '{"source": "ui_update"}' OR 
                    EXISTS (
                        SELECT 1 FROM jsonb_each_text(lineage::jsonb) WHERE value = 'ui_update'
                    )
                );
            """)
            self.graph_conn.commit()
        except Exception as e:
            self.graph_conn.rollback()
            print(f"Error clearing existing data: {e}")

    def _load_ui_updates(self):
        try:
            # Load UI-updated nodes
            self.graph_cur.execute("""
                SELECT node_id, label, name, properties, lineage 
                FROM public.kg_nodes 
                WHERE 
                    lineage::jsonb @> '{"source": "ui_update"}' OR 
                    EXISTS (
                        SELECT 1 FROM jsonb_each_text(lineage::jsonb) WHERE value = 'ui_update'
                    );
            """)
            ui_nodes = self.graph_cur.fetchall()
            for r in ui_nodes:
                nid = r["node_id"]
                props = (
                    r["properties"]
                    if isinstance(r["properties"], dict)
                    else json.loads(r["properties"] or "{}")
                )
                lineage = (
                    r["lineage"]
                    if isinstance(r["lineage"], dict)
                    else json.loads(r["lineage"] or "{}")
                )
                self.nodes[nid] = {
                    "id": nid,
                    "labels": [r["label"]],
                    "properties": props,
                    "lineage": lineage,
                }

            # Load UI-updated relationships
            self.graph_cur.execute("""
                SELECT relationship_id, from_node_id, to_node_id, rel_type, properties, lineage 
                FROM public.kg_relationships 
                WHERE 
                    lineage::jsonb @> '{"source": "ui_update"}' OR 
                    lineage::jsonb @> '{"relationship": "ui_update"}' OR 
                    EXISTS (
                        SELECT 1 FROM jsonb_each_text(lineage::jsonb) WHERE value = 'ui_update'
                    );
            """)
            ui_rels = self.graph_cur.fetchall()
            for r in ui_rels:
                rid = r["relationship_id"]
                props = (
                    r["properties"]
                    if isinstance(r["properties"], dict)
                    else json.loads(r["properties"] or "{}")
                )
                lineage = (
                    r["lineage"]
                    if isinstance(r["lineage"], dict)
                    else json.loads(r["lineage"] or "{}")
                )
                self.relationships.append(
                    {
                        "id": rid,
                        "source": r["from_node_id"],
                        "target": r["to_node_id"],
                        "type": r["rel_type"],
                        "properties": props,
                        "lineage": lineage,
                    }
                )

        except Exception as e:
            self.graph_conn.rollback()
            print(f"Error loading UI updates: {e}")

    def read_symbolic(self, table_name):
        query = f'SELECT * FROM public."{table_name}"'
        return pd.read_sql(query, self.symbolic_conn)

    def build_symbolic_graph(self):
        if not getattr(self, "symbolic_conn", None):
            print("No symbolic_ai connection. Skipping symbolic graph build.")
            return

        self.current_source_table = "table.symbolic_ai"
        try:
            demo_df = self.read_symbolic("symbolic_ai")
        except Exception as e:
            print(f"Failed to read from symbolic_ai demo table: {e}")
            return

        # Standardize column names (e.g., 'equipment_id' -> 'Equipment Id')
        col_map = {c: str(c).title().replace("_", " ") for c in demo_df.columns}
        demo_df = demo_df.rename(columns=col_map)
        demo_df = demo_df.fillna("")

        if "Equipment Id" not in demo_df.columns or "Issue" not in demo_df.columns:
            print(f"Missing required columns in symbolic_ai table. Found: {demo_df.columns.tolist()}")
            return

        # Precompute uniqueness counts
        iss_counts = demo_df.groupby(["Equipment Id", "Issue"]).size().to_dict()
        param_counts = (
            demo_df.groupby(["Equipment Id", "Issue", "Parameter"]).size().to_dict()
        )
        cause_counts = (
            demo_df.groupby(["Equipment Id", "Issue", "Parameter", "Cause"])
            .size()
            .to_dict()
        )

        for _, row in demo_df.iterrows():
            eq_id_raw = str(row.get("Equipment Id", "")).strip()
            if not eq_id_raw:
                continue

            eq_nid = self.find_eq(eq_id_raw)
            if not eq_nid:
                continue

            iss = str(row.get("Issue", "")).strip()
            if not iss:
                continue

            param = str(row.get("Parameter", "")).strip()
            cause = str(row.get("Cause", "")).strip()
            check = str(row.get("Check", "")).strip()

            c_iss = iss_counts.get((eq_id_raw, iss), 0)
            c_param = param_counts.get((eq_id_raw, iss, param), 0)
            c_cause = cause_counts.get((eq_id_raw, iss, param, cause), 0)

            depth = 1
            if c_iss > 1 and param:
                depth = 2
                if c_param > 1 and cause:
                    depth = 3
                    if c_cause > 1 and check:
                        depth = 4

            # Level 1: Issue
            row_id = str(row.get("id", "")).strip()
            iss_key = f"{row_id}"
            iss_props = {"name": iss, "issue": iss}
            if depth == 1:
                if param:
                    iss_props["parameter"] = param
                if cause:
                    iss_props["cause"] = cause
                if check:
                    iss_props["check"] = check

            iss_nid = self.add_node("Issue", iss_key, iss_props, source="symbolic_ai")
            self.add_rel(
                "HAS_SYMBOLIC",
                eq_nid,
                iss_nid,
                priority=10,
                lineage_info=[{"source": "symbolic_ai"}],
            )

            # Level 2: Parameter
            if depth >= 2:
                param_key = f"{iss_key}_{param}"
                param_props = {"name": param, "parameter": param}
                if depth == 2:
                    if cause:
                        param_props["cause"] = cause
                    if check:
                        param_props["check"] = check

                param_nid = self.add_node(
                    "Parameter", param_key, param_props, source="symbolic_ai"
                )
                self.add_rel(
                    "PARAMETER",
                    iss_nid,
                    param_nid,
                    priority=10,
                    lineage_info=[{"source": "symbolic_ai"}],
                )

                # Level 3: Cause
                if depth >= 3:
                    cause_key = f"{param_key}_{cause}"
                    cause_props = {"name": cause, "cause": cause}
                    if depth == 3:
                        if check:
                            cause_props["check"] = check

                    cause_nid = self.add_node(
                        "Cause", cause_key, cause_props, source="symbolic_ai"
                    )
                    self.add_rel(
                        "HAS_CAUSE",
                        param_nid,
                        cause_nid,
                        priority=10,
                        lineage_info=[{"source": "symbolic_ai"}],
                    )

                    # Level 4: Check
                    if depth == 4:
                        check_key = f"{cause_key}_{check}"
                        check_props = {"name": check, "check": check}

                        check_nid = self.add_node(
                            "Check", check_key, check_props, source="symbolic_ai"
                        )
                        self.add_rel(
                            "HAS_CHECK",
                            cause_nid,
                            check_nid,
                            priority=10,
                            lineage_info=[{"source": "symbolic_ai"}],
                        )

    # Single sync run
    def run_sync(self):
        self.reconnect()
        if not self.source_conn or not self.graph_conn:
            print("Sync aborted — connection failed.")
            return

        self._clear_tables()
        self._load_ui_updates()
        self.build_graph()
        self.build_symbolic_graph()
        self._persist_nodes()
        self._persist_relationships()


    # 2-hour scheduler
    def start_scheduler(self, interval_hours=2):
        while True:
            try:
                self.run_sync()
            except Exception as e:
                print(f"Sync error: {e}")
            time.sleep(interval_hours * 3600)


if __name__ == "__main__":
    import sys

    converter = GraphConverter()
    if "--once" in sys.argv:
        # Single immediate sync (for testing)
        converter.run_sync()
    else:
        # 2-hour repeating scheduler
        converter.start_scheduler(interval_hours=2)
