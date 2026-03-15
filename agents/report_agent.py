"""
Report Agent — Generates beautiful HTML reports from profiling results.

Takes a TableDQReport and produces:
  - Radar chart (dimension scores)
  - Null % bar chart per column
  - Interactive checks table with filters
  - Issues summary
  - Sample data preview
  - Overall DQ score with colour-coded grade

Output: a single self-contained HTML file.
"""

import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional

from models.profiling_models import TableDQReport, CheckResult, Severity

logger = logging.getLogger(__name__)

# ── Try importing Plotly and Jinja2 ──────────────────────────────
try:
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    logger.warning("Plotly not installed — charts will be skipped. Run: pip install plotly")

try:
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False
    logger.warning("Jinja2 not installed — Run: pip install jinja2")

TEMPLATE_DIR = Path(__file__).parent.parent / "templates"
REPORTS_DIR  = Path(__file__).parent.parent / "reports"


# ─────────────────────────────────────────────────────────────────
# CHART BUILDERS
# ─────────────────────────────────────────────────────────────────

def _score_to_color(score: float) -> str:
    if score >= 90: return "#26a69a"
    if score >= 75: return "#66bb6a"
    if score >= 50: return "#ffa726"
    if score >= 30: return "#ff7043"
    return "#ef5350"


def build_radar_chart(report: TableDQReport) -> str:
    """Radar/spider chart showing all 8 dimension scores."""
    if not PLOTLY_AVAILABLE:
        return "<p style='color:#7b80a0'>Install plotly for charts: pip install plotly</p>"

    dims   = list(report.dimension_scores.keys())
    scores = list(report.dimension_scores.values())
    labels = [d.replace("_", " ").title() for d in dims]

    # Close the polygon
    labels_closed = labels + [labels[0]]
    scores_closed = scores + [scores[0]]

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=scores_closed,
        theta=labels_closed,
        fill="toself",
        fillcolor="rgba(92,107,192,0.15)",
        line=dict(color="#5c6bc0", width=2),
        marker=dict(color=[_score_to_color(s) for s in scores_closed], size=7),
        name="DQ Score",
    ))

    fig.update_layout(
        polar=dict(
            bgcolor="#1a1d27",
            radialaxis=dict(
                visible=True, range=[0, 100],
                gridcolor="#2e3248", linecolor="#2e3248",
                tickfont=dict(color="#7b80a0", size=9),
            ),
            angularaxis=dict(
                gridcolor="#2e3248", linecolor="#2e3248",
                tickfont=dict(color="#e8eaf6", size=10),
            ),
        ),
        paper_bgcolor="#1a1d27",
        plot_bgcolor="#1a1d27",
        font=dict(color="#e8eaf6"),
        margin=dict(l=40, r=40, t=20, b=20),
        height=280,
        showlegend=False,
    )
    return fig.to_html(full_html=False, include_plotlyjs="cdn", config={"displayModeBar": False})


def build_null_chart(report: TableDQReport) -> str:
    """Horizontal bar chart showing null % per column."""
    if not PLOTLY_AVAILABLE:
        return "<p style='color:#7b80a0'>Install plotly for charts: pip install plotly</p>"

    from models.profiling_models import CheckType

    # Extract null % from completeness checks
    cols, nulls = [], []
    for check in report.checks:
        if check.check_type == CheckType.COMPLETENESS and check.column:
            null_pct = check.detail.get("null_pct", 0.0)
            cols.append(check.column)
            nulls.append(null_pct)

    if not cols:
        return "<p style='color:#7b80a0'>No completeness data available.</p>"

    # Sort by null % descending, show top 15
    paired = sorted(zip(nulls, cols), reverse=True)[:15]
    nulls_s = [p[0] for p in paired]
    cols_s  = [p[1] for p in paired]
    colors  = [_score_to_color(100 - n) for n in nulls_s]

    fig = go.Figure(go.Bar(
        x=nulls_s, y=cols_s,
        orientation="h",
        marker=dict(color=colors, line=dict(width=0)),
        text=[f"{n:.1f}%" for n in nulls_s],
        textposition="outside",
        textfont=dict(color="#e8eaf6", size=11),
    ))
    fig.update_layout(
        paper_bgcolor="#1a1d27",
        plot_bgcolor="#1a1d27",
        font=dict(color="#e8eaf6", size=11),
        xaxis=dict(
            range=[0, max(nulls_s) * 1.3 + 5],
            gridcolor="#2e3248", linecolor="#2e3248",
            title="Null %", title_font=dict(size=11),
        ),
        yaxis=dict(gridcolor="#2e3248", linecolor="#2e3248"),
        margin=dict(l=10, r=60, t=10, b=30),
        height=280,
    )
    return fig.to_html(full_html=False, include_plotlyjs=False, config={"displayModeBar": False})


# ─────────────────────────────────────────────────────────────────
# TEMPLATE RENDERING
# ─────────────────────────────────────────────────────────────────

def _get_score_color(score: float) -> str:
    if score >= 90: return "#26a69a"
    if score >= 75: return "#66bb6a"
    if score >= 50: return "#ffa726"
    if score >= 30: return "#ff7043"
    return "#ef5350"


def render_report(report: TableDQReport) -> str:
    """
    Render the full HTML report from a TableDQReport.
    Returns the complete HTML string.
    """
    if not JINJA2_AVAILABLE:
        raise RuntimeError("jinja2 not installed. Run: pip install jinja2")

    # Build charts
    radar_chart = build_radar_chart(report)
    null_chart  = build_null_chart(report)

    # Sort checks: critical first, then warning, then pass
    severity_order = {Severity.CRITICAL: 0, Severity.WARNING: 1, Severity.INFO: 2, Severity.PASS: 3}
    checks_sorted = sorted(
        report.checks,
        key=lambda c: (severity_order.get(c.severity, 99), c.check_type.value)
    )

    # Prepare checks for template (convert enums to strings)
    checks_for_template = []
    for c in checks_sorted:
        checks_for_template.append({
            "check_type": c.check_type.value,
            "column": c.column,
            "passed": c.passed,
            "severity": c.severity.value,
            "score": c.score,
            "message": c.message,
            "detail": c.detail,
        })

    # Unique check types for filter buttons
    check_types = sorted(set(c["check_type"] for c in checks_for_template))

    # Sample data
    sample_rows, sample_columns = [], []
    if report.checks:
        # Get from the report's raw sample if available, else skip
        pass

    # Convert report dimension_scores keys for template
    dim_scores_display = {
        k.replace("_", " ").title(): v
        for k, v in sorted(report.dimension_scores.items(), key=lambda x: x[1])
    }

    # Set up Jinja2
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template("report_template.html")

    html = template.render(
        report=report,
        score_color=_get_score_color(report.overall_dq_score),
        radar_chart=radar_chart,
        null_chart=null_chart,
        checks_sorted=checks_for_template,
        check_types=check_types,
        passed_checks=len(report.passed_checks()),
        failed_checks=len(report.failed_checks()),
        total_checks=len(report.checks),
        sample_rows=sample_rows,
        sample_columns=sample_columns,
    )
    return html


# ─────────────────────────────────────────────────────────────────
# FILE OUTPUT
# ─────────────────────────────────────────────────────────────────

def save_report(report: TableDQReport, output_dir: str = None) -> str:
    """
    Generate and save the HTML report to disk.

    Args:
        report:     TableDQReport from the profiling agent
        output_dir: Where to save (default: ./reports/)

    Returns:
        Absolute path to the saved HTML file

    Example:
        report = run_profiling_agent(config, "orders")
        path = save_report(report)
        print(f"Report saved: {path}")
    """
    out_dir = Path(output_dir) if output_dir else REPORTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename  = f"dq_report_{report.table_name}_{timestamp}.html"
    filepath  = out_dir / filename

    html = render_report(report)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"[ReportAgent] Report saved: {filepath}")
    return str(filepath)


def generate_multi_table_report(reports: List[TableDQReport], output_dir: str = None) -> List[str]:
    """
    Generate individual HTML reports for multiple tables at once.
    Returns list of file paths.
    """
    paths = []
    for report in reports:
        try:
            path = save_report(report, output_dir)
            paths.append(path)
            logger.info(f"[ReportAgent] ✓ {report.table_name} → {Path(path).name}")
        except Exception as e:
            logger.error(f"[ReportAgent] ✗ Failed for {report.table_name}: {e}")
    return paths


# ─────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────

def run_report_agent(
    report: TableDQReport,
    output_dir: str = None,
    return_html: bool = False,
) -> Dict[str, Any]:
    """
    Main entry point for the Report Agent.

    Args:
        report:      TableDQReport from run_profiling_agent()
        output_dir:  Where to save the HTML file
        return_html: If True, also return the HTML string in result

    Returns:
        {
          "success": True,
          "file_path": "/path/to/report.html",
          "html": "..." (if return_html=True),
          "table_name": "orders",
          "score": 82.1,
        }
    """
    try:
        file_path = save_report(report, output_dir)
        result = {
            "success": True,
            "file_path": file_path,
            "table_name": report.table_name,
            "score": report.overall_dq_score,
            "score_label": report.score_label(),
        }
        if return_html:
            result["html"] = render_report(report)
        return result
    except Exception as e:
        logger.exception("Report agent failed")
        return {
            "success": False,
            "error": str(e),
            "table_name": report.table_name,
        }