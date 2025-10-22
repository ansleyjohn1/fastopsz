from pymilvus import Collection, connections

class SchemaScout:
    """
    Updated to work with Step 0's metadata system.
    """

    def __init__(self, metadata_store, embedding_manager):
        self.metadata_store = metadata_store
        self.embedding_manager = embedding_manager
        self.collection = embedding_manager.collection
        self.embedding_model = embedding_manager.model

    def search_tables(self, question, connection_ids=None, top_k=2):
        """
        Search for relevant tables.

        Args:
            question: User's natural language query
            connection_ids: Optional filter for specific connections (tenant isolation)
            top_k: How many tables to return
        """

        # Step 1: Embed the question
        question_embedding = self.embedding_model.encode(question).tolist()

        # Step 2: Build Milvus filter expression
        filter_expr = None
        if connection_ids:
            # Tenant isolation: only search within specific connections
            conn_list = ", ".join([f'"{c}"' for c in connection_ids])
            filter_expr = f"connection_id in [{conn_list}]"

        # Step 3: Vector search in Milvus
        search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}}

        results = self.collection.search(
            data=[question_embedding],
            anns_field="embedding",
            param=search_params,
            limit=top_k,
            expr=filter_expr,
            output_fields=["connection_id", "table_name", "schema_hash"]
        )

        # Step 4: Check if embeddings are stale
        validated_results = []
        tables_to_resync = []

        for hit in results[0]:
            # Get current schema from metadata store
            cached_schema = self.metadata_store.get_table_schema(
                hit.entity.get("connection_id"),
                hit.entity.get("table_name")
            )

            if cached_schema:
                # Check if schema hash matches
                if cached_schema["hash"] == hit.entity.get("schema_hash"):
                    # Embedding is up-to-date
                    validated_results.append({
                        "connection_id": hit.entity.get("connection_id"),
                        "table_name": hit.entity.get("table_name"),
                        "similarity_score": hit.distance,
                        "schema": cached_schema["schema"]
                    })
                else:
                    # Schema changed but embedding not updated
                    # Trigger async resync
                    tables_to_resync.append({
                        "connection_id": hit.entity.get("connection_id"),
                        "table_name": hit.entity.get("table_name")
                    })
            else:
                # Schema not in cache (shouldn't happen, but handle it)
                tables_to_resync.append({
                    "connection_id": hit.entity.get("connection_id"),
                    "table_name": hit.entity.get("table_name")
                })

        # Step 5: Trigger async resync for stale embeddings
        if tables_to_resync:
            self.trigger_async_resync(tables_to_resync)

        # Step 6: Apply elbow detection for dynamic k selection
        if len(validated_results) == 0:
            return {"tables": [], "k": 0}

        scores = [t["similarity_score"] for t in validated_results]
        k = self.find_score_elbow(scores)

        # Apply bounds (5-15)
        k = max(5, min(15, k))

        return {
            "tables": validated_results[:k],
            "k": k,
            "total_searched": len(results[0]),
            "stale_embeddings": len(tables_to_resync)
        }

    def find_score_elbow(self, scores):
        """
        Find where scores drop sharply.
        (Same logic as before)
        """
        if len(scores) < 3:
            return len(scores)

        drops = []
        for i in range(len(scores) - 1):
            drop = scores[i] - scores[i + 1]
            drops.append(drop)

        max_drop_idx = drops.index(max(drops))
        elbow_point = max_drop_idx + 1

        if elbow_point <= 5:
            return 5
        elif elbow_point <= 10:
            return 10
        else:
            return 15

    def trigger_async_resync(self, tables):
        """
        Trigger background job to resync stale embeddings.
        """
        # Queue async job (using Celery, RQ, or similar)
        from tasks import resync_table_embeddings
        for table in tables:
            resync_table_embeddings.delay(
                table["connection_id"],
                table["table_name"]
            )

    def ensure_embeddings_exist(self, connection_id, table_names):
        """
        Check if embeddings exist for tables.
        Generate if missing (synchronous, for critical tables).
        """
        missing = []

        for table_name in table_names:
            # Check Milvus
            expr = f'connection_id == "{connection_id}" and table_name == "{table_name}"'
            results = self.collection.query(expr=expr, output_fields=["table_name"])

            if not results:
                missing.append(table_name)

        if missing:
            # Generate embeddings synchronously
            from schema_inspector import UniversalSchemaInspector
            from sync_orchestrator import SchemaSyncOrchestrator

            # Get connection info
            conn_info = self.metadata_store.get_connection_info(connection_id)

            # Sync missing tables
            inspector = UniversalSchemaInspector(conn_info)
            inspector.connect()

            for table_name in missing:
                try:
                    schema = inspector.get_table_schema(table_name)
                    self.metadata_store.store_table_schema(schema)

                    embedding = self.embedding_manager.generate_embedding(schema)
                    self.embedding_manager.store_embedding(
                        connection_id,
                        table_name,
                        schema["schema_hash"],
                        embedding
                    )
                except Exception as e:
                    print(f"Failed to generate embedding for {table_name}: {e}")

        return missing