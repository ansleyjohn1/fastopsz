from sqlalchemy import create_engine, inspect, MetaData
from pymongo import MongoClient
import hashlib
import json


class UniversalSchemaInspector:
    """
    Database-agnostic schema inspector.
    Uses helper packages: SQLAlchemy, pymongo
    """

    def __init__(self, connection_info):
        self.conn_info = connection_info
        self.db_type = connection_info["type"]
        self.connection_id = connection_info["connection_id"]

    def connect(self):
        """Connect to database using appropriate driver."""
        if self.db_type in ["postgresql", "mysql", "mssql", "oracle", "sqlite"]:
            # Use SQLAlchemy for SQL databases
            conn_string = self.build_connection_string()
            self.engine = create_engine(conn_string)
            self.inspector = inspect(self.engine)

        elif self.db_type == "mongodb":
            # Use pymongo for MongoDB
            self.client = MongoClient(self.conn_info["uri"])
            self.db = self.client[self.conn_info["database"]]

        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def build_connection_string(self):
        """Build SQLAlchemy connection string."""
        user = self.conn_info["username"]
        pwd = self.conn_info["password"]
        host = self.conn_info["host"]
        port = self.conn_info["port"]
        db = self.conn_info["database"]

        if self.db_type == "postgresql":
            return f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
        elif self.db_type == "mysql":
            return f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}"
        elif self.db_type == "mssql":
            return f"mssql+pyodbc://{user}:{pwd}@{host}:{port}/{db}"
        # ... other types

    def get_all_tables(self):
        """Get list of all tables/collections."""
        if self.db_type == "mongodb":
            return self.db.list_collection_names()
        else:
            return self.inspector.get_table_names()

    def get_table_schema(self, table_name):
        """
        Get complete schema for a table.
        Returns standardized format regardless of DB type.
        """
        if self.db_type == "mongodb":
            return self.get_mongodb_schema(table_name)
        else:
            return self.get_sql_schema(table_name)

    def get_sql_schema(self, table_name):
        """Get schema for SQL table using SQLAlchemy."""
        schema = {
            "connection_id": self.connection_id,
            "table_name": table_name,
            "db_type": self.db_type,
            "columns": [],
            "primary_key": [],
            "foreign_keys": [],  # CRITICAL for Joinability Sheriff!
            "indexes": [],
            "estimated_rows": 0
        }

        # Get columns
        for col in self.inspector.get_columns(table_name):
            schema["columns"].append({
                "name": col["name"],
                "type": str(col["type"]),
                "nullable": col["nullable"],
                "default": str(col.get("default")) if col.get("default") else None
            })

        # Get primary key
        pk_constraint = self.inspector.get_pk_constraint(table_name)
        if pk_constraint:
            schema["primary_key"] = pk_constraint["constrained_columns"]

        # Get foreign keys (CRITICAL!)
        for fk in self.inspector.get_foreign_keys(table_name):
            schema["foreign_keys"].append({
                "from_columns": fk["constrained_columns"],
                "to_table": fk["referred_table"],
                "to_columns": fk["referred_columns"],
                "name": fk.get("name")
            })

        # Get indexes
        for idx in self.inspector.get_indexes(table_name):
            schema["indexes"].append({
                "name": idx["name"],
                "columns": idx["column_names"],
                "unique": idx.get("unique", False)
            })

        # Estimate row count
        try:
            result = self.engine.execute(f"SELECT COUNT(*) FROM {table_name}")
            schema["estimated_rows"] = result.fetchone()[0]
        except:
            schema["estimated_rows"] = None

        # Calculate schema hash (for change detection)
        schema["schema_hash"] = self.calculate_schema_hash(schema)

        return schema

    def get_mongodb_schema(self, collection_name):
        """Get schema for MongoDB collection."""
        collection = self.db[collection_name]

        # Sample documents to infer schema
        sample = list(collection.find().limit(100))

        schema = {
            "connection_id": self.connection_id,
            "table_name": collection_name,
            "db_type": "mongodb",
            "fields": [],
            "indexes": [],
            "estimated_rows": collection.estimated_document_count()
        }

        # Infer fields from sample
        field_stats = {}
        for doc in sample:
            for key, value in doc.items():
                if key not in field_stats:
                    field_stats[key] = {"types": set(), "count": 0}

                field_stats[key]["types"].add(type(value).__name__)
                field_stats[key]["count"] += 1

        # Convert to schema
        for field_name, stats in field_stats.items():
            schema["fields"].append({
                "name": field_name,
                "types": list(stats["types"]),
                "frequency": stats["count"] / len(sample) if sample else 0
            })

        # Get indexes
        for idx in collection.list_indexes():
            schema["indexes"].append({
                "name": idx["name"],
                "keys": list(idx["key"].keys())
            })

        schema["schema_hash"] = self.calculate_schema_hash(schema)

        return schema

    def calculate_schema_hash(self, schema):
        """
        Calculate hash of schema for change detection.
        """
        # Create deterministic string representation
        schema_str = json.dumps(schema, sort_keys=True, default=str)
        return hashlib.md5(schema_str.encode()).hexdigest()