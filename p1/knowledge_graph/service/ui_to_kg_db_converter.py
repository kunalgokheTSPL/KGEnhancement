from http import HTTPStatus
import json
import os
import sys
from fastapi.responses import JSONResponse
from psycopg2.extras import RealDictCursor
from p2.utility.database_driver import PostgresDriver

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


class UItoKGConverter:
    """Handles persisting UI changes directly to the Knowledge Graph DB.
    Called by graph_service.py to enforce separation of concerns."""

    def __init__(self):
        self.kg_driver = PostgresDriver()
        self.kg_driver.connect()
        self.conn = self.kg_driver.conn
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
        self.initialize_schema()

    def initialize_schema(self):
        try:
            cur = self.conn.cursor()

            # Create kg_nodes table if it doesn't exist
            cur.execute("""
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

            # Create kg_relationships table if it doesn't exist
            cur.execute("""
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

            # Create indexes 
            cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_nodes_label
            ON public.kg_nodes(label);
            """)

            cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_rel_from
            ON public.kg_relationships(from_node_id);
            """)

            cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_rel_to
            ON public.kg_relationships(to_node_id);
            """)
            self.conn.commit()
        except Exception as exc:
            self.conn.rollback()
            print(f"KG upload failed, transaction rolled back: {exc}")
            return JSONResponse(
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                content={
                    "success": False,
                    "message": "Internal server error",
                    "errors": [{"field": "server", "message": f"Import failed, transaction rolled back: {exc}"}],
                },
            )

    def _parse_json_field(self, field_val):
        """Helper to parse a field that might be a JSON string or already a dict."""
        if not field_val:
            return {}
        return json.loads(field_val) if isinstance(field_val, str) else field_val

    def _replace_props_and_lineage(self, existing_props, existing_lineage, new_props, extra_updates=None):
        """Performs a full replace, keeping properties clean but tracking deletions in lineage."""
        final_props = dict(new_props or {})
        final_lineage = dict(existing_lineage or {})
        
        if extra_updates:
            final_lineage.update(extra_updates)
            
        all_keys = set(list(existing_props.keys()) + list(final_props.keys()))

        for k in all_keys:
            if k not in final_props:
                # Key was deleted 
                final_lineage[k] = "ui_update"
            elif k not in existing_props or str(existing_props[k]) != str(final_props[k]):
                final_lineage[k] = "ui_update"

        return final_props, final_lineage

    def upsert_node(self, label, nid, name, props):
        try:
            node_id = str(nid)
            self.cur.execute(
                "SELECT properties, lineage FROM public.kg_nodes WHERE node_id = %s",
                (node_id,),
            )
            row = self.cur.fetchone() or {}

            existing_props = self._parse_json_field(row.get("properties"))
            existing_lineage = self._parse_json_field(row.get("lineage"))
 
            final_props, final_lineage = self._replace_props_and_lineage(
                existing_props, existing_lineage, props, {"label": label}
            )

            self.cur.execute(
                """
                INSERT INTO public.kg_nodes (node_id, label, name, properties, lineage, updated_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (node_id) DO UPDATE SET
                    name       = EXCLUDED.name,
                    properties = EXCLUDED.properties,
                    lineage    = EXCLUDED.lineage,
                    updated_at = CURRENT_TIMESTAMP
            """,
                (
                    node_id,
                    label,
                    name,
                    json.dumps(final_props),
                    json.dumps(final_lineage),
                ),
            )

            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"UI Converter -> KG Node Upsert Error: {e}")
            raise e

    def upsert_rel(self, rel_type, src_nid, tgt_nid, props):
        try:
            rel_type = str(rel_type).strip().upper().replace(" ", "_")

            self.cur.execute(
                """
                SELECT relationship_id, properties, lineage
                FROM public.kg_relationships 
                WHERE (from_node_id = %s AND to_node_id = %s AND upper(rel_type) = %s)
                   OR (from_node_id = %s AND to_node_id = %s AND upper(rel_type) = %s)
                LIMIT 1
            """,
                (
                    str(src_nid),
                    str(tgt_nid),
                    rel_type,
                    str(tgt_nid),
                    str(src_nid),
                    rel_type,
                ),
            )
            existing = self.cur.fetchone()

            if existing:
                rel_id = existing["relationship_id"]
                existing_props = self._parse_json_field(existing.get("properties"))
                existing_lineage = self._parse_json_field(existing.get("lineage"))
            else:
                rel_id = (props or {}).get(
                    "relationship_id"
                ) or f"{src_nid}:{rel_type}:{tgt_nid}"
                existing_props = {}
                existing_lineage = {}

            final_props, final_lineage = self._replace_props_and_lineage(
                existing_props, existing_lineage, props, {"source": "ui_update"}
            )

            self.cur.execute(
                """
                INSERT INTO public.kg_relationships
                    (relationship_id, from_node_id, to_node_id, rel_type, properties, lineage, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (relationship_id) DO UPDATE SET
                    properties = EXCLUDED.properties,
                    lineage    = EXCLUDED.lineage,
                    updated_at = CURRENT_TIMESTAMP
            """,
                (
                    rel_id,
                    str(src_nid),
                    str(tgt_nid),
                    rel_type,
                    json.dumps(final_props),
                    json.dumps(final_lineage),
                ),
            )

            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"UI Converter -> KG Rel Upsert Error: {e}")
            raise e

    def delete_node(self, nid):
        try:
            if not nid:
                return False
            self.cur.execute(
                "DELETE FROM public.kg_nodes WHERE node_id = %s", (str(nid),)
            )
            deleted_count = self.cur.rowcount
            if deleted_count > 0:
                print(f"UI Converter -> KG deleted node {nid}")
            self.conn.commit()
            return deleted_count > 0
        except Exception as e:
            self.conn.rollback()
            print(f"UI Converter -> KG Node Delete Error: {e}")
            return False

    def delete_rel(self, src_nid, tgt_nid, rel_type):
        try:
            rel_type_clean = str(rel_type).strip().upper().replace(" ", "_")
            self.cur.execute(
                """
                DELETE FROM public.kg_relationships
                WHERE from_node_id = %s AND to_node_id = %s AND rel_type = %s
            """,
                (str(src_nid), str(tgt_nid), rel_type_clean),
            )

            deleted = self.cur.rowcount > 0
            self.conn.commit()
            if deleted:
                print(
                    f"UI Converter -> KG deleted relationship {src_nid} -[{rel_type_clean}]-> {tgt_nid}"
                )
            return deleted
        except Exception as e:
            self.conn.rollback()
            print(f"UI Converter -> KG Rel Delete Error: {e}")
            return False

    def check_existing_rel(self, src_nid, tgt_nid):
        """Helper to check for existing relationships to prevent duplicates/reverses."""
        self.cur.execute(
            """
            SELECT from_node_id, to_node_id, rel_type FROM public.kg_relationships 
            WHERE (from_node_id = %s AND to_node_id = %s) 
               OR (from_node_id = %s AND to_node_id = %s)
            LIMIT 1
        """,
            (str(src_nid), str(tgt_nid), str(tgt_nid), str(src_nid)),
        )
        return self.cur.fetchone()

    def close(self):
        if self.conn:
            self.conn.close()
