import os
from dotenv import load_dotenv
from datetime import datetime

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
if os.getenv("MILVUS_URI"):
    # Milvus Lite mode (embedded, file-based)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    MILVUS_CONFIG = {
        "uri": os.getenv("MILVUS_URI", f"./milvus_data_{timestamp}.db"),
        "alias": "default"
    }
elif os.getenv("MILVUS_HOST"):
    # Standalone Milvus mode (separate service)
    MILVUS_CONFIG = {
        "host": os.getenv("MILVUS_HOST", "localhost"),
        "port": int(os.getenv("MILVUS_PORT", "19530")),
        "alias": "default"
    }
else:
    # Default to Milvus Lite for development
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    MILVUS_CONFIG = {
        "uri": os.getenv("MILVUS_URI", f"./milvus_data_{timestamp}.db"),
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


