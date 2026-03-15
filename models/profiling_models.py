"""
Data models for the Profiling Agent output.
Every check, issue, and score is typed — no raw dicts flying around.
These models are what LangGraph passes between nodes.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum


class Severity(str, Enum):
    """How bad is this issue?"""
    CRITICAL = "critical"    # >30% affected  — red
    WARNING  = "warning"     # 5–30% affected — orange
    INFO     = "info"        # <5% affected   — yellow
    PASS     = "pass"        # No issue found — green


class CheckType(str, Enum):
    """The 8 DQ check categories."""
    COMPLETENESS  = "completeness"   # Null / missing values
    UNIQUENESS    = "uniqueness"     # Duplicate rows / values
    VALIDITY      = "validity"       # Format, type, range checks
    CONSISTENCY   = "consistency"    # Cross-column logic
    TIMELINESS    = "timeliness"     # Date freshness
    ACCURACY      = "accuracy"       # Outlier detection
    SCHEMA        = "schema"         # Column type mismatches
    EMPTY_STRINGS = "empty_strings"  # Blanks that aren't NULL


@dataclass
class CheckResult:
    """Result of a single DQ check on a single column (or table-level)."""
    check_type: CheckType
    column: Optional[str]           # None = table-level check (e.g. duplicates)
    passed: bool
    severity: Severity
    score: float                    # 0–100, contribution to overall DQ score
    
    # Human-readable summary
    message: str                    # e.g. "12.5% null values detected"
    detail: Dict[str, Any] = field(default_factory=dict)  # Raw numbers

    def to_dict(self) -> Dict:
        return {
            "check_type": self.check_type.value,
            "column": self.column,
            "passed": self.passed,
            "severity": self.severity.value,
            "score": self.score,
            "message": self.message,
            "detail": self.detail,
        }


@dataclass
class TableDQReport:
    """
    Complete DQ report for one table.
    This is what the Profiling Agent produces and LangGraph carries forward.
    """
    source_name: str
    source_type: str
    table_name: str
    row_count: int
    column_count: int

    # All individual check results
    checks: List[CheckResult] = field(default_factory=list)

    # Aggregated scores per dimension (0–100 each)
    dimension_scores: Dict[str, float] = field(default_factory=dict)

    # The headline number — weighted average of all dimension scores
    overall_dq_score: float = 0.0

    # Issues grouped by severity for quick summary
    critical_issues: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    # Metadata
    profiled_at: str = ""
    profiling_time_ms: float = 0.0

    def passed_checks(self) -> List[CheckResult]:
        return [c for c in self.checks if c.passed]

    def failed_checks(self) -> List[CheckResult]:
        return [c for c in self.checks if not c.passed]

    def checks_by_type(self, check_type: CheckType) -> List[CheckResult]:
        return [c for c in self.checks if c.check_type == check_type]

    def score_label(self) -> str:
        """Human-readable grade for the DQ score."""
        s = self.overall_dq_score
        if s >= 90: return "Excellent"
        if s >= 75: return "Good"
        if s >= 60: return "Fair"
        if s >= 40: return "Poor"
        return "Critical"

    def to_dict(self) -> Dict:
        return {
            "source_name": self.source_name,
            "source_type": self.source_type,
            "table_name": self.table_name,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "overall_dq_score": round(self.overall_dq_score, 1),
            "score_label": self.score_label(),
            "dimension_scores": {k: round(v, 1) for k, v in self.dimension_scores.items()},
            "critical_issues": self.critical_issues,
            "warnings": self.warnings,
            "total_checks": len(self.checks),
            "passed_checks": len(self.passed_checks()),
            "failed_checks": len(self.failed_checks()),
            "checks": [c.to_dict() for c in self.checks],
            "profiled_at": self.profiled_at,
            "profiling_time_ms": self.profiling_time_ms,
        }


@dataclass
class ProfilingAgentState:
    """
    LangGraph state object — passed between every node in the profiling graph.
    Each node reads from this, adds its results, and passes it forward.
    """
    # Input
    source_config: Dict[str, Any] = field(default_factory=dict)
    table_name: str = ""

    # Built during execution
    raw_profile: Dict[str, Any] = field(default_factory=dict)   # From connector.get_full_profile()
    check_results: List[CheckResult] = field(default_factory=list)

    # Final output
    report: Optional[TableDQReport] = None

    # Control flow
    error: Optional[str] = None
    current_step: str = "init"
    logs: List[str] = field(default_factory=list)

    def log(self, msg: str):
        import logging
        self.logs.append(msg)
        logging.getLogger("profiling_agent").info(msg)