from psycopg2.extras import RealDictCursor
from p2.utility.database_driver import PostgresDriver

class GraphRegistry:
    def __init__(self):

        # Connect to your new Database
        self.db = PostgresDriver()
        self.db.connect()

        self.conn = self.db.conn
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)

        self.setup_db()

    def setup_db(self):
        """Creates the optimized tables and indexes for the Knowledge Graph."""

        # 1. CREATE NODES TABLE
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.kg_nodes (
            node_id      TEXT PRIMARY KEY,       -- Hashed Unique ID
            label        TEXT NOT NULL,          -- Equipment, Plant, etc.
            name         TEXT NOT NULL,          -- Display Name
            properties   JSONB DEFAULT '{}',     -- Fused Data
            lineage      JSONB DEFAULT '{}',     -- Trace-back Map
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # 2. CREATE RELATIONSHIPS TABLE
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.kg_relationships (
            relationship_id TEXT PRIMARY KEY,    -- Hashed Unique ID
            from_node_id    TEXT NOT NULL REFERENCES public.kg_nodes(node_id) ON DELETE CASCADE,
            to_node_id      TEXT NOT NULL REFERENCES public.kg_nodes(node_id) ON DELETE CASCADE,
            rel_type        TEXT NOT NULL,       -- CONNECTED_TO, HAS_TAG, etc.
            properties      JSONB DEFAULT '{}',
            lineage         JSONB DEFAULT '{}',
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # 3. CREATE PERFORMANCE INDEXES
        self.cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_nodes_label ON public.kg_nodes(label);"
        )
        self.cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_rel_from ON public.kg_relationships(from_node_id);"
        )
        self.cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_rel_to ON public.kg_relationships(to_node_id);"
        )

        self.conn.commit()
        print("Knowledge Graph Schema is ready.")

    def close(self):
        self.db.close()


if __name__ == "__main__":
    try:
        registry = GraphRegistry()
        registry.close()
    except Exception as e:
        print(f"Error: {e}")
