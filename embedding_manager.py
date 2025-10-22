from sentence_transformers import SentenceTransformer
from pymilvus import Collection, connections, utility, FieldSchema, CollectionSchema, DataType
import json


class EmbeddingManager:
    """
    Manages table embeddings in Milvus.
    Checks if embeddings exist before generating.
    """

    def __init__(self, milvus_config):
        connections.connect(**milvus_config)
        self.collection_name = "table_embeddings"
        self._ensure_collection_exists()
        self.collection = Collection(self.collection_name)
        self.collection.load()
        self.model = SentenceTransformer('all-MiniLM-L6-v2')

    def _ensure_collection_exists(self):
        """Create collection if it doesn't exist."""
        if utility.has_collection(self.collection_name):
            return

        # Define schema with 3 columns: text (schema info), vectors, metadata
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="connection_id", dtype=DataType.VARCHAR, max_length=255),
            FieldSchema(name="table_name", dtype=DataType.VARCHAR, max_length=255),
            FieldSchema(name="schema_hash", dtype=DataType.VARCHAR, max_length=32),
            FieldSchema(name="schema_text", dtype=DataType.VARCHAR, max_length=65535),  # Text (schema info)
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=384),  # Vectors
            FieldSchema(name="metadata", dtype=DataType.VARCHAR, max_length=65535)  # Metadata (JSON)
        ]

        schema = CollectionSchema(fields, description="Table schema embeddings")
        collection = Collection(self.collection_name, schema)

        # Create index for vector search
        index_params = {
            "index_type": "IVF_FLAT",
            "metric_type": "COSINE",
            "params": {"nlist": 128}
        }
        collection.create_index("embedding", index_params)

    def embedding_exists(self, connection_id, table_name, schema_hash):
        """
        Check if embedding exists and is up-to-date.
        """
        # Query Milvus
        expr = f'connection_id == "{connection_id}" and table_name == "{table_name}"'
        results = self.collection.query(
            expr=expr,
            output_fields=["schema_hash", "embedding"]
        )

        if not results:
            return False, None

        # Check if schema hash matches (schema unchanged)
        existing_hash = results[0]["schema_hash"]
        if existing_hash == schema_hash:
            return True, results[0]["embedding"]
        else:
            # Schema changed, need to regenerate
            return False, None

    def generate_embedding(self, schema):
        """
        Generate embedding for a table schema.
        """
        # Build text description
        description = self.build_table_description(schema)

        # Generate embedding
        embedding = self.model.encode(description)

        return embedding.tolist()

    def build_table_description(self, schema):
        """
        Build text description for embedding.
        """
        desc = f"Table: {schema['table_name']}\n"

        # Add columns
        if "columns" in schema:
            col_desc = ", ".join([
                f"{col['name']} ({col['type']})"
                for col in schema["columns"][:20]  # Limit to first 20
            ])
            desc += f"Columns: {col_desc}\n"

        # Add foreign keys (important for understanding relationships)
        if "foreign_keys" in schema:
            fk_desc = ", ".join([
                f"→ {fk['to_table']}"
                for fk in schema["foreign_keys"]
            ])
            desc += f"Relationships: {fk_desc}\n"

        # Add MongoDB fields
        if "fields" in schema:
            field_desc = ", ".join([
                f"{field['name']} ({'/'.join(field['types'])})"
                for field in schema["fields"][:20]
            ])
            desc += f"Fields: {field_desc}\n"

        return desc

    def store_embedding(self, connection_id, table_name, schema_hash, embedding, schema_text, metadata):
        """
        Store or update embedding in Milvus.
        """
        # Check if exists
        expr = f'connection_id == "{connection_id}" and table_name == "{table_name}"'
        existing = self.collection.query(expr=expr, output_fields=["id"])

        if existing:
            # Delete old embedding
            ids = [item["id"] for item in existing]
            self.collection.delete(f"id in {ids}")

        # Insert new embedding with 3 main data points: schema_text, embedding (vectors), metadata
        data = [{
            "connection_id": connection_id,
            "table_name": table_name,
            "schema_hash": schema_hash,
            "schema_text": schema_text,  # Text (schema info)
            "embedding": embedding,  # Vectors
            "metadata": json.dumps(metadata)  # Metadata (JSON string)
        }]

        self.collection.insert(data)
        self.collection.flush()