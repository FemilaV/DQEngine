"""
/profile route — run the profiling agent on a table.
"""

from fastapi import APIRouter
from api.schemas import ProfileRequest, ProfileResponse, CheckResultSchema
from agents.profiling_agent import run_profiling_agent
from agents.report_agent import run_report_agent

router = APIRouter(prefix="/profile", tags=["Profiling"])


@router.post("", response_model=ProfileResponse)
async def profile_table(request: ProfileRequest):
    """
    Run all 8 DQ checks on a table and return the full report.

    Returns:
    - overall_dq_score (0–100)
    - per-dimension scores
    - all individual check results
    - critical issues and warnings list
    - optional HTML report path
    """
    try:
        source_cfg = request.source_config.model_dump()

        # Run profiling
        dq_report = run_profiling_agent(
            source_config=source_cfg,
            table_name=request.table_name,
        )

        # Optionally generate report
        report_path = None
        if not request.skip_report:
            result = run_report_agent(dq_report)
            if result["success"]:
                report_path = result["file_path"]

        # Serialize checks
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
            for c in dq_report.checks
        ]

        return ProfileResponse(
            success=True,
            table_name=dq_report.table_name,
            source_name=dq_report.source_name,
            source_type=dq_report.source_type,
            row_count=dq_report.row_count,
            column_count=dq_report.column_count,
            overall_dq_score=dq_report.overall_dq_score,
            score_label=dq_report.score_label(),
            dimension_scores=dq_report.dimension_scores,
            critical_issues=dq_report.critical_issues,
            warnings=dq_report.warnings,
            checks=checks_out,
            passed_checks=len(dq_report.passed_checks()),
            failed_checks=len(dq_report.failed_checks()),
            total_checks=len(dq_report.checks),
            report_path=report_path,
            profiling_time_ms=dq_report.profiling_time_ms,
        )

    except Exception as e:
        return ProfileResponse(
            success=False,
            table_name=request.table_name,
            source_name=request.source_config.name,
            source_type=request.source_config.source_type,
            row_count=0, column_count=0,
            overall_dq_score=0.0, score_label="Unknown",
            dimension_scores={}, critical_issues=[], warnings=[],
            checks=[], passed_checks=0, failed_checks=0, total_checks=0,
            profiling_time_ms=0.0, error=str(e),
        )