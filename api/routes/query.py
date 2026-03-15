"""
/query route — natural language to SQL endpoint.
"""

from fastapi import APIRouter
from api.schemas import QueryRequest, QueryResponse
from agents.nl_sql_agent import run_nl_sql_agent

router = APIRouter(prefix="/query", tags=["NL→SQL"])


@router.post("", response_model=QueryResponse)
async def natural_language_query(request: QueryRequest):
    """
    Convert a natural language question into SQL and execute it.

    The LLM reads the table schema automatically — no SQL knowledge needed.

    Examples:
    - "show me orders where customer_id is missing"
    - "how many duplicate rows are in this table?"
    - "what is the average price grouped by category?"
    - "find all transactions with negative amounts"
    """
    try:
        source_cfg = request.source_config.model_dump()

        result = run_nl_sql_agent(
            natural_language=request.natural_language,
            source_config=source_cfg,
            target_tables=[request.table_name],
            max_rows=request.max_rows,
        )

        # Serialize DataFrame to list of dicts
        data_rows = []
        if result.data is not None and len(result.data) > 0:
            # Replace NaN/None with None for JSON safety
            data_rows = result.data.where(
                result.data.notna(), other=None
            ).to_dict(orient="records")

        return QueryResponse(
            success=result.success,
            natural_language=result.natural_language,
            generated_sql=result.generated_sql,
            explanation=result.explanation,
            row_count=result.row_count,
            columns=result.columns,
            data=data_rows,
            execution_time_ms=result.execution_time_ms or 0.0,
            llm_provider=result.llm_provider,
            model_used=result.model_used,
            error=result.error,
        )

    except Exception as e:
        return QueryResponse(
            success=False,
            natural_language=request.natural_language,
            generated_sql=None,
            explanation=None,
            row_count=0,
            columns=[],
            data=[],
            execution_time_ms=0.0,
            llm_provider="",
            model_used="",
            error=str(e),
        )