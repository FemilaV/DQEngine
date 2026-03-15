"""
/report route — generate and serve HTML reports.
"""

import os
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from api.schemas import ReportRequest, ReportResponse
from agents.profiling_agent import run_profiling_agent
from agents.report_agent import run_report_agent

router = APIRouter(prefix="/report", tags=["Reports"])


@router.post("", response_model=ReportResponse)
async def generate_report(request: ReportRequest):
    """
    Profile a table and generate a full HTML report with charts.

    Set return_html=true to get the HTML string directly
    (useful for Streamlit iframe embedding).
    """
    try:
        source_cfg = request.source_config.model_dump()

        # Profile first
        dq_report = run_profiling_agent(
            source_config=source_cfg,
            table_name=request.table_name,
        )

        # Generate report
        result = run_report_agent(
            report=dq_report,
            return_html=request.return_html,
        )

        if not result["success"]:
            return ReportResponse(
                success=False,
                table_name=request.table_name,
                overall_dq_score=0.0,
                score_label="Unknown",
                report_path=None,
                error=result.get("error"),
            )

        return ReportResponse(
            success=True,
            table_name=request.table_name,
            overall_dq_score=dq_report.overall_dq_score,
            score_label=dq_report.score_label(),
            report_path=result.get("file_path"),
            html=result.get("html") if request.return_html else None,
        )

    except Exception as e:
        return ReportResponse(
            success=False,
            table_name=request.table_name,
            overall_dq_score=0.0,
            score_label="Unknown",
            report_path=None,
            error=str(e),
        )


@router.get("/download/{filename}")
async def download_report(filename: str):
    """
    Download a previously generated HTML report by filename.
    Example: GET /report/download/dq_report_orders_20240315.html
    """
    reports_dir = os.path.join(os.path.dirname(__file__), "../../reports")
    file_path = os.path.abspath(os.path.join(reports_dir, filename))

    # Security: ensure path stays inside reports directory
    if not file_path.startswith(os.path.abspath(reports_dir)):
        raise HTTPException(status_code=403, detail="Access denied.")

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"Report '{filename}' not found.")

    return FileResponse(
        path=file_path,
        media_type="text/html",
        filename=filename,
    )


@router.get("/list")
async def list_reports():
    """List all generated reports available for download."""
    reports_dir = os.path.join(os.path.dirname(__file__), "../../reports")
    os.makedirs(reports_dir, exist_ok=True)

    files = [
        {
            "filename": f,
            "size_kb": round(os.path.getsize(os.path.join(reports_dir, f)) / 1024, 1),
            "created": os.path.getmtime(os.path.join(reports_dir, f)),
        }
        for f in sorted(os.listdir(reports_dir))
        if f.endswith(".html")
    ]
    return {"reports": files, "count": len(files)}