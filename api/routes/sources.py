"""
/sources route — inspect data sources before running the pipeline.
"""

from fastapi import APIRouter, HTTPException
from api.schemas import ListTablesRequest, ListTablesResponse
from connectors import get_connector, ConnectionConfig

router = APIRouter(prefix="/sources", tags=["Sources"])


@router.post("/tables", response_model=ListTablesResponse)
async def list_tables(request: ListTablesRequest):
    """
    Connect to a data source and return all available table names.
    Call this first so the UI can show a table picker.
    """
    try:
        config = ConnectionConfig(**request.source_config.model_dump())
        connector = get_connector(config)
        connector.connect()

        if not connector.is_connected():
            return ListTablesResponse(
                success=False,
                tables=[],
                source_name=config.name,
                error="Could not connect to data source.",
            )

        tables = connector.list_tables()
        connector.disconnect()

        return ListTablesResponse(
            success=True,
            tables=tables,
            source_name=config.name,
        )

    except Exception as e:
        return ListTablesResponse(
            success=False,
            tables=[],
            source_name=request.source_config.name,
            error=str(e),
        )


@router.post("/test-connection")
async def test_connection(request: ListTablesRequest):
    """
    Ping a data source and return latency.
    Useful for checking connection before running a full profile.
    """
    try:
        config = ConnectionConfig(**request.source_config.model_dump())
        connector = get_connector(config)
        connector.connect()
        health = connector.test_connection()
        connector.disconnect()
        return {"source_name": config.name, **health}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))