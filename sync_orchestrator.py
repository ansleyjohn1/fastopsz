from schema_inspector import UniversalSchemaInspector
import json


class SchemaSyncOrchestrator:
    """
    Orchestrates the entire sync process.
    """

    def __init__(self, metadata_store, embedding_manager):
        self.metadata_store = metadata_store
        self.embedding_manager = embedding_manager

    def sync_connection(self, connection_info):
        """
        Sync one database connection.
        """
        print(f"Syncing {connection_info['connection_id']}...")

        # Connect to database
        inspector = UniversalSchemaInspector(connection_info)
        inspector.connect()

        # Get all tables
        tables = inspector.get_all_tables()
        print(f"Found {len(tables)} tables")

        synced_count = 0
        skipped_count = 0

        for table_name in tables:
            try:
                # Get schema
                schema = inspector.get_table_schema(table_name)
                print('1')
                # Check if schema changed
                cached = self.metadata_store.get_table_schema(
                    connection_info["connection_id"],
                    table_name
                )
                print('2')
                if cached and cached["hash"] == schema["schema_hash"]:
                    # Schema unchanged, skip
                    skipped_count += 1
                    continue
                print('3')
                # Store metadata
                self.metadata_store.store_table_schema(schema, connection_info)
                print('4')
                # Check if embedding exists and is up-to-date
                exists, existing_embedding = self.embedding_manager.embedding_exists(
                    schema["connection_id"],
                    schema["table_name"],
                    schema["schema_hash"]
                )
                print('5')
                if not exists:
                    # Generate new embedding
                    print('6')
                    embedding = self.embedding_manager.generate_embedding(schema)
                    print('7')
                    # Build schema text description
                    schema_text = self.embedding_manager.build_table_description(schema)
                    print('8')
                    # Build metadata
                    metadata = {
                        "connection_id": schema["connection_id"],
                        "table_name": schema["table_name"],
                        "db_type": schema["db_type"],
                        "column_count": len(schema.get("columns", [])),
                        "has_foreign_keys": len(schema.get("foreign_keys", [])) > 0,
                        "estimated_rows": schema.get("estimated_rows")
                    }
                    print('9')
                    # Store in Milvus
                    self.embedding_manager.store_embedding(
                        schema["connection_id"],
                        schema["table_name"],
                        schema["schema_hash"],
                        embedding,
                        schema_text,
                        metadata
                    )
                    print('10')
                synced_count += 1

            except Exception as e:
                print(f"Failed to sync {table_name}: {e}")

        # Update sync timestamp
        self.metadata_store.update_connection_sync_time(connection_info["connection_id"])

        print(f"Synced: {synced_count}, Skipped: {skipped_count}")

        return {"synced": synced_count, "skipped": skipped_count}