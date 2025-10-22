from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Dict, Any
from contextlib import asynccontextmanager

from metadata_store import MetadataStore
from embedding_manager import EmbeddingManager
from sync_orchestrator import SchemaSyncOrchestrator
from config import METADATA_DB_CONFIG, MILVUS_CONFIG

# Global variables for component instances
metadata_store = None
embedding_manager = None
orchestrator = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize components on startup and cleanup on shutdown."""
    global metadata_store, embedding_manager, orchestrator
    
    print("Initializing components...")
    metadata_store = MetadataStore(METADATA_DB_CONFIG)
    embedding_manager = EmbeddingManager(MILVUS_CONFIG)
    orchestrator = SchemaSyncOrchestrator(metadata_store, embedding_manager)
    print("Components initialized successfully")
    
    yield
    
    # Cleanup (if needed)
    print("Shutting down...")


app = FastAPI(
    title="Schema Sync API",
    description="API for syncing database schemas to Milvus",
    version="1.0.0",
    lifespan=lifespan
)


class ConnectionConfig(BaseModel):
    """Model for database connection configuration."""
    connection_id: str = Field(..., description="Unique identifier for the connection")
    name: str = Field(..., description="Human-readable name for the connection")
    type: str = Field(..., description="Database type (e.g., 'mysql', 'postgresql')")
    host: str = Field(..., description="Database host address")
    port: int = Field(..., description="Database port number")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")

    class Config:
        json_schema_extra = {
            "example": {
                "connection_id": "mysql_prod_001",
                "name": "Production MySQL Database",
                "type": "mysql",
                "host": "db.example.com",
                "port": 3306,
                "database": "production_db",
                "username": "dbuser",
                "password": "securepassword"
            }
        }


class SyncRequest(BaseModel):
    """Model for sync request containing multiple connections."""
    connections: List[ConnectionConfig] = Field(..., description="List of database connections to sync")

    class Config:
        json_schema_extra = {
            "example": {
                "connections": [
                    {
                        "connection_id": "mysql_prod_001",
                        "name": "Production MySQL",
                        "type": "mysql",
                        "host": "db.example.com",
                        "port": 3306,
                        "database": "production_db",
                        "username": "dbuser",
                        "password": "securepassword"
                    }
                ]
            }
        }


class SyncResult(BaseModel):
    """Model for sync operation result."""
    connection_id: str
    name: str
    success: bool
    synced: int = 0
    skipped: int = 0
    error: str = None


class SyncResponse(BaseModel):
    """Model for overall sync response."""
    total_connections: int
    successful: int
    failed: int
    results: List[SyncResult]


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Schema Sync API",
        "version": "1.0.0",
        "endpoints": {
            "/sync": "POST - Sync multiple database connections",
            "/health": "GET - Health check endpoint",
            "/docs": "GET - API documentation"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        # Check if components are initialized
        if metadata_store is None or embedding_manager is None or orchestrator is None:
            return {
                "status": "unhealthy",
                "message": "Components not initialized"
            }
        
        return {
            "status": "healthy",
            "metadata_store": "connected",
            "embedding_manager": "connected",
            "orchestrator": "ready"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }


@app.post("/sync", response_model=SyncResponse)
async def sync_connections(request: SyncRequest):
    """
    Sync multiple database connections to Milvus.
    
    This endpoint:
    1. Registers each connection in the metadata store
    2. Syncs the schema to Milvus with embeddings
    
    Args:
        request: SyncRequest containing list of database connections
        
    Returns:
        SyncResponse with results for each connection
    """
    if metadata_store is None or orchestrator is None:
        raise HTTPException(
            status_code=503,
            detail="Service not ready. Components not initialized."
        )
    
    results = []
    successful_count = 0
    failed_count = 0
    
    for connection_config in request.connections:
        connection_dict = connection_config.model_dump()
        connection_id = connection_dict['connection_id']
        connection_name = connection_dict['name']
        
        try:
            print(f"\nProcessing connection: {connection_id} ({connection_name})")
            
            # Step 1: Register connection
            print(f"  - Registering connection in metadata store...")
            metadata_store.register_connection(connection_dict)
            
            # Step 2: Sync connection
            print(f"  - Syncing schema to Milvus...")
            sync_result = orchestrator.sync_connection(connection_dict)
            
            # Create success result
            result = SyncResult(
                connection_id=connection_id,
                name=connection_name,
                success=True,
                synced=sync_result.get('synced', 0),
                skipped=sync_result.get('skipped', 0)
            )
            
            successful_count += 1
            print(f"  ✓ Success: {sync_result['synced']} tables synced, {sync_result['skipped']} skipped")
            
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")
            result = SyncResult(
                connection_id=connection_id,
                name=connection_name,
                success=False,
                error=str(e)
            )
            failed_count += 1
        
        results.append(result)
    
    return SyncResponse(
        total_connections=len(request.connections),
        successful=successful_count,
        failed=failed_count,
        results=results
    )


@app.post("/sync-single", response_model=SyncResult)
async def sync_single_connection(connection: ConnectionConfig):
    """
    Sync a single database connection to Milvus.
    
    Convenience endpoint for syncing one connection at a time.
    
    Args:
        connection: ConnectionConfig for a single database
        
    Returns:
        SyncResult for the connection
    """
    if metadata_store is None or orchestrator is None:
        raise HTTPException(
            status_code=503,
            detail="Service not ready. Components not initialized."
        )
    
    connection_dict = connection.model_dump()
    connection_id = connection_dict['connection_id']
    connection_name = connection_dict['name']
    
    try:
        print(f"\nProcessing connection: {connection_id} ({connection_name})")
        
        # Step 1: Register connection
        print(f"  - Registering connection in metadata store...")
        metadata_store.register_connection(connection_dict)
        
        # Step 2: Sync connection
        print(f"  - Syncing schema to Milvus...")
        sync_result = orchestrator.sync_connection(connection_dict)
        
        print(f"  ✓ Success: {sync_result['synced']} tables synced, {sync_result['skipped']} skipped")
        
        return SyncResult(
            connection_id=connection_id,
            name=connection_name,
            success=True,
            synced=sync_result.get('synced', 0),
            skipped=sync_result.get('skipped', 0)
        )
        
    except Exception as e:
        print(f"  ✗ Error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to sync connection: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)