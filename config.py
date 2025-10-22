import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL metadata store configuration
METADATA_DB_CONFIG = {
    "host": os.getenv("METADATA_DB_HOST", "localhost"),
    "port": int(os.getenv("METADATA_DB_PORT", 5432)),
    "database": os.getenv("METADATA_DB_NAME", "metadata_store"),
    "user": os.getenv("METADATA_DB_USER", "postgres"),
    "password": os.getenv("METADATA_DB_PASSWORD", "password")
}

# Milvus configuration
# Supports both self-hosted and Zilliz Cloud
MILVUS_CONFIG = {
    "host": os.getenv("MILVUS_HOST", "localhost"),
    "port": os.getenv("MILVUS_PORT", "19530"),
    "alias": "default"
}

# Add Zilliz Cloud authentication if API key is provided
if os.getenv("MILVUS_API_KEY"):
    MILVUS_CONFIG["token"] = os.getenv("MILVUS_API_KEY")
    MILVUS_CONFIG["secure"] = True

# Example MySQL connection configuration
# In production, this will be provided by the client via API
MYSQL_CONNECTION = {
    "connection_id": "mysql_test_001",
    "name": "Test MySQL Database",
    "type": "mysql",
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
    "database": os.getenv("MYSQL_DATABASE", "test_db"),
    "username": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "password")
}
