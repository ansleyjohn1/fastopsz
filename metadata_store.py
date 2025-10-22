import psycopg2
import json
from datetime import datetime


class MetadataStore:
    """
    Stores schema metadata in PostgreSQL.
    Fast lookup for FK relationships.
    """

    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)
        self.ensure_tables_exist()

    def register_connection(self, connection_info):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO connections
                (connection_id, name, type, host, port, database, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (connection_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    type = EXCLUDED.type,
                    host = EXCLUDED.host,
                    port = EXCLUDED.port,
                    database = EXCLUDED.database,
                    status = EXCLUDED.status
                RETURNING (xmax = 0) AS inserted
            """, (
                connection_info["connection_id"],
                connection_info.get("name", connection_info["connection_id"]),
                connection_info["type"],
                connection_info["host"],
                connection_info["port"],
                connection_info["database"],
                "active"
            ))
            
            was_inserted = cur.fetchone()[0]
            
            if was_inserted:
                print(f"✓ New connection registered: {connection_info['connection_id']}")
            else:
                print(f"✓ Connection updated: {connection_info['connection_id']}")

    def ensure_tables_exist(self):
        """Create metadata tables if they don't exist."""
        with self.conn.cursor() as cur:
            # Connections table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS connections (
                    connection_id VARCHAR(255) PRIMARY KEY,
                    name VARCHAR(255),
                    type VARCHAR(50),
                    host VARCHAR(255),
                    port INTEGER,
                    database VARCHAR(255),
                    status VARCHAR(50),
                    last_synced TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # Tables metadata
            cur.execute("""
                CREATE TABLE IF NOT EXISTS table_metadata (
                    id SERIAL PRIMARY KEY,
                    connection_id VARCHAR(255) REFERENCES connections(connection_id),
                    table_name VARCHAR(255),
                    schema_data JSONB,
                    schema_hash VARCHAR(32),
                    row_count INTEGER,
                    last_analyzed TIMESTAMP DEFAULT NOW(),
                    UNIQUE(connection_id, table_name)
                )
            """)

            # Foreign keys cache (for fast lookup)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS foreign_keys (
                    id SERIAL PRIMARY KEY,
                    connection_id VARCHAR(255),
                    from_table VARCHAR(255),
                    from_columns TEXT[],
                    to_table VARCHAR(255),
                    to_columns TEXT[],
                    confidence FLOAT DEFAULT 1.0,
                    source VARCHAR(50),
                    UNIQUE(connection_id, from_table, to_table)
                )
            """)

            # Create indexes for fast lookup
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_fk_lookup
                ON foreign_keys(connection_id, from_table)
            """)

            self.conn.commit()

    def store_table_schema(self, schema, connection_info):
        """Store or update table schema."""
        with self.conn.cursor() as cur:
            

            cur.execute("""
                INSERT INTO table_metadata
                (connection_id, table_name, schema_data, schema_hash, row_count)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (connection_id, table_name)
                DO UPDATE SET
                    schema_data = EXCLUDED.schema_data,
                    schema_hash = EXCLUDED.schema_hash,
                    row_count = EXCLUDED.row_count,
                    last_analyzed = NOW()
            """, (
                schema["connection_id"],
                schema["table_name"],
                json.dumps(schema),
                schema["schema_hash"],
                schema.get("estimated_rows")
            ))

            # Store foreign keys separately for fast lookup
            if "foreign_keys" in schema:
                for fk in schema["foreign_keys"]:
                    cur.execute("""
                        INSERT INTO foreign_keys
                        (connection_id, from_table, from_columns, to_table, to_columns, source)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (connection_id, from_table, to_table) DO NOTHING
                    """, (
                        schema["connection_id"],
                        schema["table_name"],
                        fk["from_columns"],
                        fk["to_table"],
                        fk["to_columns"],
                        "constraint"
                    ))

            self.conn.commit()

    def get_table_schema(self, connection_id, table_name):
        """Get cached schema for a table."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT schema_data, schema_hash
                FROM table_metadata
                WHERE connection_id = %s AND table_name = %s
            """, (connection_id, table_name))

            row = cur.fetchone()
            if row:
                return {"schema": row[0], "hash": row[1]}
            return None

    def get_fk_map(self, connection_id, table_names):
        """
        Get FK map for specific tables (FAST!).
        This is what Joinability Sheriff uses.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT from_table, to_table, from_columns, to_columns, confidence, source
                FROM foreign_keys
                WHERE connection_id = %s
                  AND from_table = ANY(%s)
                  AND to_table = ANY(%s)
            """, (connection_id, table_names, table_names))

            fk_map = {}
            for row in cur.fetchall():
                from_table, to_table, from_cols, to_cols, confidence, source = row

                if from_table not in fk_map:
                    fk_map[from_table] = {}

                fk_map[from_table][to_table] = {
                    "from_columns": from_cols,
                    "to_columns": to_cols,
                    "confidence": confidence,
                    "source": source
                }

            return fk_map

    def update_connection_sync_time(self, connection_id):
        """Update last sync timestamp."""
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE connections
                SET last_synced = NOW()
                WHERE connection_id = %s
            """, (connection_id,))
            self.conn.commit()