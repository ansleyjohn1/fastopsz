from metadata_store import MetadataStore
from embedding_manager import EmbeddingManager
from sync_orchestrator import SchemaSyncOrchestrator
from schema_scout import SchemaScout
from joinability_sheriff import JoinabilitySheriff
from config import METADATA_DB_CONFIG, MILVUS_CONFIG, MYSQL_CONNECTION


def main():
    """
    Main script to sync MySQL database schema to Milvus.
    """
    print("=" * 60)
    print("Step 0: Connection & Metadata Management")
    print("=" * 60)

    # Initialize components
    print("\n1. Initializing metadata store (PostgreSQL)...")
    metadata_store = MetadataStore(METADATA_DB_CONFIG)
    print("✓ Metadata store ready")

    print("\n2. Initializing embedding manager (Milvus)...")
    embedding_manager = EmbeddingManager(MILVUS_CONFIG)
    print("✓ Embedding manager ready")

    print("\n3. Creating sync orchestrator...")
    
    orchestrator = SchemaSyncOrchestrator(metadata_store, embedding_manager)
    print("✓ Orchestrator ready")

    # Sync MySQL database
    print("\n4. Syncing MySQL database...")
    print(f"   Connection ID: {MYSQL_CONNECTION['connection_id']}")
    print(f"   Database: {MYSQL_CONNECTION['database']}")
    print(f"   Host: {MYSQL_CONNECTION['host']}")
    print("-" * 60)
    metadata_store.register_connection(MYSQL_CONNECTION)
    result = orchestrator.sync_connection(MYSQL_CONNECTION)

    print("\n" + "=" * 60)
    print("Sync Complete!")
    print("=" * 60)
    print(f"Tables synced: {result['synced']}")
    print(f"Tables skipped: {result['skipped']}")
    print("\n✓ Schema metadata stored in PostgreSQL")
    print("✓ Embeddings stored in Milvus with:")
    print("  - schema_text (text description)")
    print("  - embedding (384-dim vectors)")
    print("  - metadata (JSON)")

    schema_scout = SchemaScout(metadata_store, embedding_manager)
    option = True
    while option:
        user_question = input("\nEnter a question about your database schema: ")

        table_information = schema_scout.search_tables(user_question, connection_ids=[MYSQL_CONNECTION['connection_id']], top_k=5)

        print(table_information)

        selected_tables = table_information['tables']

        joinability_sheriff = JoinabilitySheriff(metadata_store)

        join_results = joinability_sheriff.generate_combinations(selected_tables)

        print(join_results)

        q2 = input("\nDo you want to ask another question? (y/n): ").strip().lower()
        if q2 == 'n':
            option = False


if __name__ == "__main__":
    main()