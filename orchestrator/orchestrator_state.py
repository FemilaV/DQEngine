"""
Orchestrator State — the single object that flows through
every node in the master LangGraph pipeline.

Think of it as a "job ticket" — it starts with just the user's
request, and each node fills in its section before passing it on.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from models.profiling_models import TableDQReport
from models.nl_sql_models import NLSQLResult


@dataclass
class PipelineRequest:
    """Everything the user provides to kick off the pipeline."""

    # Required
    source_config: Dict[str, Any]      # ConnectionConfig dict
    table_name: str                    # Which table to analyse

    # Optional — if provided, NL→SQL agent runs
    nl_question: Optional[str] = None  # "show me rows with missing customer id"

    # Optional overrides
    max_rows: int = 100                # NL→SQL result limit
    output_dir: Optional[str] = None   # Where to save the HTML report
    skip_nl_sql: bool = False          # Force-skip NL→SQL even if question given
    skip_report: bool = False          # Skip HTML report generation


@dataclass
class PipelineResult:
    """The complete output of the pipeline — everything in one place."""

    # From profiling agent
    dq_report: Optional[TableDQReport] = None
    overall_score: float = 0.0
    score_label: str = ""

    # From NL→SQL agent (None if no question was asked)
    nl_sql_result: Optional[NLSQLResult] = None

    # From report agent
    report_path: Optional[str] = None  # Path to generated HTML file
    report_html: Optional[str] = None  # HTML string (for Streamlit embedding)

    # Pipeline metadata
    success: bool = False
    total_time_ms: float = 0.0
    stages_completed: List[str] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "overall_score": round(self.overall_score, 1),
            "score_label": self.score_label,
            "report_path": self.report_path,
            "total_time_ms": self.total_time_ms,
            "stages_completed": self.stages_completed,
            "error": self.error,
            "dq_report": self.dq_report.to_dict() if self.dq_report else None,
            "nl_sql_result": self.nl_sql_result.to_dict() if self.nl_sql_result else None,
        }


@dataclass
class OrchestratorState:
    """
    LangGraph state — passed between every node in the master pipeline.
    Each node reads what it needs and writes its output back here.
    """

    # Input — set at the start, never changed
    request: Optional[PipelineRequest] = None

    # ── Stage outputs — filled in as pipeline progresses ──
    profiling_done: bool = False
    dq_report: Optional[TableDQReport] = None

    nl_sql_done: bool = False
    nl_sql_result: Optional[NLSQLResult] = None

    report_done: bool = False
    report_path: Optional[str] = None
    report_html: Optional[str] = None

    # ── Final packaged result ──
    result: Optional[PipelineResult] = None

    # ── Control flow ──
    error: Optional[str] = None
    current_step: str = "init"
    logs: List[str] = field(default_factory=list)

    def log(self, msg: str):
        import logging
        self.logs.append(msg)
        logging.getLogger("orchestrator").info(msg)