import json
import time
import datetime
import os
import sys
from psycopg2.extras import RealDictCursor
from p2.utility.database_driver import PostgresDriver
from psycopg2.extras import execute_values

# Ensure project root is in python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


# Label → DEV table mapping
LABEL_TABLE_MAP = {
    "Equipment": {"table": "equipment", "pk": "equipment_id"},
    "Plant": {"table": "plant", "pk": "plant_code"},
    "WorkOrder": {"table": "work_order", "pk": "wo_number"},
    "TaskList": {"table": "task_list", "pk": "task_list_id"},
    "Sensor": {"table": "timeseries_metadata", "pk": "tag_name"},
    "Document": {"table": "document_metadata", "pk": "document_id"},
    "FailureMode": {"table": "failure_mode", "pk": "failure_mode_code"},
    "FailureEffect": {"table": "failure_effect", "pk": "failure_effect_code"},
    "FailureCause": {"table": "failure_cause", "pk": "failure_cause_code"},
    "FunctionalLocation": {"table": "functional_location", "pk": "floc"},
    "EquipmentPID": {"table": "equipment_pid", "pk": "equipment_id"},
    "EquipmentSAP": {"table": "equipment_sap", "pk": "equipment_id"},
}


class CDMConverter:
    """Reads kg_nodes / kg_relationships and upserts into the CDM_DEV database."""

    def __init__(self):
        self.kg_conn = None
        self.dev_conn = None
        self._connect()

    def _connect(self):
        try:
            # Connect to Knowledge Graph
            kg_driver = PostgresDriver()
            kg_driver.connect()
            self.kg_conn = kg_driver.conn

            # Connect to Dev CDM
            cdm_driver = PostgresDriver()
            cdm_driver.connect()
            self.dev_conn = cdm_driver.conn

            print("Connected to KG DB and DEV DB using PostgresDriver.")
        except Exception as e:
            print(f"Connection Error: {e}")
            self.kg_conn = None
            self.dev_conn = None

    def reconnect(self):
        try:
            if self.kg_conn:
                self.kg_conn.close()
            if self.dev_conn:
                self.dev_conn.close()
        except Exception:
            pass
        self._connect()

    # Helpers
    def _get_valid_columns(self, cdm_cur, table: str) -> set:
        cdm_cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
        """,
            (table,),
        )
        return {row[0] for row in cdm_cur.fetchall()}

    def _props(self, row) -> dict:
        p = row["properties"]
        if isinstance(p, str):
            p = json.loads(p or "{}")
        return p or {}

    # Sync nodes → CDM tables
    def sync_nodes(self):
        kg_cur = self.kg_conn.cursor(cursor_factory=RealDictCursor)
        cdm_cur = self.dev_conn.cursor()
        synced = 0
        skipped = 0

        # Build column to tables lookup
        cdm_cur.execute("""
            SELECT table_name, column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public';
        """)
        col_tables = {}
        for r in cdm_cur.fetchall():
            tbl, col = r[0], r[1]
            col_tables.setdefault(col, set()).add(tbl)

        # Permitted tables by label
        LABEL_ALLOWED_TABLES = {
            "Equipment": {
                "equipment",
                "equipment_pid",
                "equipment_sap",
                "functional_location",
            },
            "Plant": {"plant"},
            "WorkOrder": {"work_order"},
            "TaskList": {"task_list"},
            "Sensor": {"timeseries_metadata"},
            "Document": {"document_metadata"},
            "FailureMode": {"failure_mode"},
            "FailureEffect": {"failure_effect"},
            "FailureCause": {"failure_cause"},
            "FunctionalLocation": {"functional_location"},
        }

        TABLE_PK_MAP = {
            "equipment": "equipment_uid",
            "work_order": "wo_uid",
            "task_list": "task_list_uid",
            "timeseries_metadata": "ts_uid",
            "document_metadata": "document_uid",
            "failure_mode": "fm_uid",
            "failure_effect": "fe_uid",
            "failure_cause": "fc_uid",
            "equipment_pid": "pid_uid",
            "equipment_sap": "sap_uid",
            "functional_location": "floc_uid",
        }

        kg_cur.execute("SELECT * FROM public.kg_nodes")
        rows = kg_cur.fetchall()

        for row in rows:
            label = row["label"]
            props = self._props(row)
            lineage = row.get("lineage") or {}
            if isinstance(lineage, str):
                lineage = json.loads(lineage or "{}")

            # Check if there are any UI updates
            ui_updated_keys = []
            for k, v in lineage.items():
                if v == "ui_update" and k in props:
                    ui_updated_keys.append(k)

            # Fallback to the primary mapped table if no explicit property-level ui_update is found
            # but the entire node is marked as ui_update
            if not ui_updated_keys and lineage.get("source") == "ui_update":
                node_id = row["node_id"]
                if "#" in node_id:
                    pk_val, table_name = node_id.split("#", 1)
                    pk_col = TABLE_PK_MAP.get(table_name)

                    if pk_col and pk_val:
                        valid_cols = self._get_valid_columns(cdm_cur, table_name)
                        cols, vals = [], []
                        for k, v in props.items():
                            if k in valid_cols and v is not None:
                                cols.append(k)
                                vals.append(v)
                        if cols:
                            try:
                                cdm_cur.execute(
                                    f'SELECT "{pk_col}" FROM public.{table_name} WHERE "{pk_col}" = %s',
                                    (pk_val,),
                                )
                                exists = cdm_cur.fetchone()
                                if exists:
                                    update_parts = [
                                        f'"{c}" = %s' for c in cols if c != pk_col
                                    ]
                                    if update_parts:
                                        update_vals = [
                                            v for c, v in zip(cols, vals) if c != pk_col
                                        ]
                                        update_vals.append(pk_val)
                                        cdm_cur.execute(
                                            f'UPDATE public.{table_name} SET {", ".join(update_parts)} WHERE "{pk_col}" = %s',
                                            tuple(update_vals),
                                        )
                                synced += 1
                            except Exception as e:
                                self.dev_conn.rollback()
                                print(
                                    f"Fallback sync error ({table_name}, pk={pk_val}): {e}"
                                )
                continue

            if not ui_updated_keys:
                skipped += 1
                continue

            # EFFICIENT TRACEABILITY & BULK UPDATE LOGIC
            # Group the changed properties by which table they belong to
            table_updates = {}
            for k in ui_updated_keys:
                val = props[k]

                # --- EXPLICIT UI -> DEV MAPPING ---
                # If the UI updated the "name" of an Equipment, map it to "normalized_asset" in the dev db
                db_col = k
                if label == "Equipment" and k == "name":
                    db_col = "normalized_asset"

                candidate_tables = col_tables.get(db_col, set())
                allowed = LABEL_ALLOWED_TABLES.get(label, set())
                target_tables = candidate_tables.intersection(allowed)

                for t in target_tables:
                    if t not in table_updates:
                        table_updates[t] = {}
                    table_updates[t][db_col] = val

            # Execute exactly ONE update query per affected table
            for t, updates in table_updates.items():
                pk_col = TABLE_PK_MAP.get(t)
                if not pk_col:
                    continue
                node_id = row["node_id"]

                if "#" not in node_id:
                    continue

                pk_val, table_name = node_id.split("#", 1)

                real_pk_col = TABLE_PK_MAP.get(table_name)
                real_pk_val = pk_val

                if not real_pk_col:
                    continue
                try:
                    # Build the combined SET clause
                    set_cols = []
                    set_vals = []
                    for k, v in updates.items():
                        set_cols.append(f'"{k}" = %s')
                        set_vals.append(v)

                    set_vals.append(real_pk_val)  # For the WHERE clause

                    update_sql = f'UPDATE public.{t} SET {", ".join(set_cols)} WHERE "{real_pk_col}" = %s'

                    # Verify the row exists first
                    cdm_cur.execute(
                        f'SELECT 1 FROM public.{t} WHERE "{real_pk_col}" = %s',
                        (real_pk_val,),
                    )
                    if cdm_cur.fetchone():
                        cdm_cur.execute(update_sql, tuple(set_vals))
                        print(
                            f"Traceback Sync: Bulk updated public.{t} {list(updates.keys())} for {real_pk_col}={real_pk_val}"
                        )
                        synced += 1
                except Exception as e:
                    self.dev_conn.rollback()
                    print(
                        f"Traceback Sync error (table={t}, pk={real_pk_val}): {e}"
                    )

        self.dev_conn.commit()

    # Sync relationships → CDM tables (Highly Optimized)
    def sync_relationships(self):
        kg_cur = self.kg_conn.cursor(cursor_factory=RealDictCursor)
        cdm_cur = self.dev_conn.cursor()
        synced = 0

        # 1. Build node_id → (label, name, props) lookup
        kg_cur.execute("SELECT node_id, label, name, properties FROM public.kg_nodes")
        node_lookup = {
            r["node_id"]: {
                "label": r["label"],
                "name": r["name"],
                "props": self._props(r),
            }
            for r in kg_cur.fetchall()
        }

        # 2. Bulk fetch existing relationships from DEV to avoid N+1 queries
        existing_connectivity = set()
        existing_asset_rel = set()

        try:
            cdm_cur.execute(
                "SELECT from_equipment_ref, to_equipment_ref FROM public.equipment_connectivity"
            )
            existing_connectivity = {(str(r[0]), str(r[1])) for r in cdm_cur.fetchall()}
        except Exception:
            self.dev_conn.rollback()

        try:
            cdm_cur.execute(
                "SELECT parent_ref, child_ref FROM public.asset_relationship"
            )
            existing_asset_rel = {(str(r[0]), str(r[1])) for r in cdm_cur.fetchall()}
        except Exception:
            self.dev_conn.rollback()

        # 3. Process KG relationships and prepare batch inserts
        kg_cur.execute("SELECT * FROM public.kg_relationships")
        rels = kg_cur.fetchall()

        valid_conn_cols = self._get_valid_columns(cdm_cur, "equipment_connectivity")
        valid_asset_cols = self._get_valid_columns(cdm_cur, "asset_relationship")

        conn_inserts = []
        asset_inserts = []

        for rel in rels:
            rel_type = rel["rel_type"]
            src_info = node_lookup.get(rel["from_node_id"])
            tgt_info = node_lookup.get(rel["to_node_id"])
            if not src_info or not tgt_info:
                continue

            src_val = str(
                src_info["props"].get("equipment_id")
                or src_info["props"].get("normalized_asset")
                or src_info["name"]
            )
            tgt_val = str(
                tgt_info["props"].get("equipment_id")
                or tgt_info["props"].get("normalized_asset")
                or tgt_info["name"]
            )

            if not src_val or not tgt_val:
                continue

            if rel_type == "CONNECTED_TO":
                if (src_val, tgt_val) not in existing_connectivity:
                    if (
                        "from_equipment_ref" in valid_conn_cols
                        and "to_equipment_ref" in valid_conn_cols
                        and "connection_type" in valid_conn_cols
                    ):
                        conn_inserts.append((src_val, tgt_val, rel_type))
                        existing_connectivity.add(
                            (src_val, tgt_val)
                        )  # Prevent duplicates in same batch
            else:
                if (src_val, tgt_val) not in existing_asset_rel:
                    if (
                        "parent_ref" in valid_asset_cols
                        and "child_ref" in valid_asset_cols
                        and "relationship_type" in valid_asset_cols
                    ):
                        asset_inserts.append((src_val, tgt_val, rel_type))
                        existing_asset_rel.add(
                            (src_val, tgt_val)
                        )  # Prevent duplicates in same batch

        # 4. Execute bulk inserts
        try:
            if conn_inserts:
                execute_values(
                    cdm_cur,
                    "INSERT INTO public.equipment_connectivity (from_equipment_ref, to_equipment_ref, connection_type) VALUES %s",
                    conn_inserts,
                )
                synced += len(conn_inserts)

            if asset_inserts:
                execute_values(
                    cdm_cur,
                    "INSERT INTO public.asset_relationship (parent_ref, child_ref, relationship_type) VALUES %s",
                    asset_inserts,
                )
                synced += len(asset_inserts)
        except Exception as e:
            self.dev_conn.rollback()
            print(f"Bulk rel sync error: {e}")

        self.dev_conn.commit()

    # Single sync run
    def run_sync(self):

        self.reconnect()
        if not self.kg_conn or not self.dev_conn:
            print("Sync aborted — connection failed.")
            return
        self.sync_nodes()
        self.sync_relationships()

    # 2-minute scheduler
    def start_scheduler(self, interval_minutes: float = 2):

        while True:
            try:
                self.run_sync()
            except Exception as e:
                print(f"Sync error: {e}")
            time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    import sys

    converter = CDMConverter()
    if "--once" in sys.argv:
        converter.run_sync()
    else:
        converter.start_scheduler(interval_minutes=2)
