"""
/pipeline route — runs the full orchestrator in one API call.
This is the primary endpoint Streamlit uses.
"""

from fastapi import APIRouter
from api.schemas import (
    PipelineRequest, PipelineResponse,
    ProfileResponse, QueryResponse, CheckResultSchema
)
from orchestrator import run_dq_pipeline

router = APIRouter(prefix="/pipeline", tags=["Pipeline"])


@router.post("", response_model=PipelineResponse)
async def run_pipeline(request: PipelineRequest):
    """
    Run the complete DQ Engine pipeline in one call:
    1. Profile the table (8 DQ checks + scoring)
    2. Run NL→SQL if a question is provided
    3. Generate HTML report

    This is the main endpoint — Streamlit calls this for everything.
    """
    try:
        source_cfg = request.source_config.model_dump()

        result = run_dq_pipeline(
            source_config=source_cfg,
            table_name=request.table_name,
            nl_question=request.nl_question,
            max_rows=request.max_rows,
            skip_report=request.skip_report,
        )

        if not result.success:
            return PipelineResponse(
                success=False,
                overall_score=0.0,
                score_label="Unknown",
                stages_completed=[],
                total_time_ms=result.total_time_ms,
                error=result.error,
            )

        # Serialize profile
        profile_out = None
        if result.dq_report:
            r = result.dq_report
            checks_out = [
                CheckResultSchema(
                    check_type=c.check_type.value,
                    column=c.column,
                    passed=c.passed,
                    severity=c.severity.value,
                    score=c.score,
                    message=c.message,
                    detail=c.detail,
                )
                for c in r.checks
            ]
            profile_out = ProfileResponse(
                success=True,
                table_name=r.table_name,
                source_name=r.source_name,
                source_type=r.source_type,
                row_count=r.row_count,
                column_count=r.column_count,
                overall_dq_score=r.overall_dq_score,
                score_label=r.score_label(),
                dimension_scores=r.dimension_scores,
                critical_issues=r.critical_issues,
                warnings=r.warnings,
                checks=checks_out,
                passed_checks=len(r.passed_checks()),
                failed_checks=len(r.failed_checks()),
                total_checks=len(r.checks),
                report_path=result.report_path,
                profiling_time_ms=r.profiling_time_ms,
            )

        # Serialize NL→SQL result
        query_out = None
        if result.nl_sql_result:
            nl = result.nl_sql_result
            data_rows = []
            if nl.data is not None and len(nl.data) > 0:
                data_rows = nl.data.where(
                    nl.data.notna(), other=None
                ).to_dict(orient="records")

            query_out = QueryResponse(
                success=nl.success,
                natural_language=nl.natural_language,
                generated_sql=nl.generated_sql,
                explanation=nl.explanation,
                row_count=nl.row_count,
                columns=nl.columns,
                data=data_rows,
                execution_time_ms=nl.execution_time_ms or 0.0,
                llm_provider=nl.llm_provider,
                model_used=nl.model_used,
                error=nl.error,
            )

        return PipelineResponse(
            success=True,
            overall_score=result.overall_score,
            score_label=result.score_label,
            stages_completed=result.stages_completed,
            total_time_ms=result.total_time_ms,
            report_path=result.report_path,
            profile=profile_out,
            query=query_out,
        )

    except Exception as e:
        return PipelineResponse(
            success=False,
            overall_score=0.0,
            score_label="Unknown",
            stages_completed=[],
            total_time_ms=0.0,
            error=str(e),
        )