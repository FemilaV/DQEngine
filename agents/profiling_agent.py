"""
Profiling Agent — LangGraph state machine.

Graph flow:
  [load_profile] → [run_checks] → [score_and_summarise] → [finalise]
       ↓ (on error)
    [handle_error]

Each node gets the full state, does its work, returns updated state.
LangGraph manages the transitions automatically.
"""

import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

# LangGraph
try:
    from langgraph.graph import StateGraph, END
    LANGGRAPH_AVAILABLE = True
except ImportError:
    LANGGRAPH_AVAILABLE = False

from models.profiling_models import (
    CheckResult, CheckType, Severity,
    TableDQReport, ProfilingAgentState
)
from agents.dq_checks import (
    check_completeness,
    check_empty_strings,
    check_uniqueness,
    check_validity,
    check_timeliness,
    check_consistency,
    check_schema,
)
from connectors import get_connector, ConnectionConfig

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# DIMENSION WEIGHTS — must sum to 100
# Tweak these to change what matters most for scoring
# ─────────────────────────────────────────────────────────────────
DIMENSION_WEIGHTS = {
    CheckType.COMPLETENESS.value:  30,   # Nulls are the #1 DQ issue
    CheckType.UNIQUENESS.value:    20,   # Duplicates corrupt analysis
    CheckType.VALIDITY.value:      15,   # Bad values mislead
    CheckType.ACCURACY.value:      10,   # Outliers skew metrics
    CheckType.TIMELINESS.value:    10,   # Stale data = wrong decisions
    CheckType.CONSISTENCY.value:   10,   # Logic errors
    CheckType.SCHEMA.value:         3,   # Structural issues
    CheckType.EMPTY_STRINGS.value:  2,   # Minor but common
}


# ─────────────────────────────────────────────────────────────────
# NODE 1: Load raw profile from connector
# ─────────────────────────────────────────────────────────────────

def node_load_profile(state: ProfilingAgentState) -> ProfilingAgentState:
    """
    Connects to the data source and fetches the full raw profile
    (schema, row count, per-column statistics).
    Stores everything in state.raw_profile.
    """
    state.current_step = "load_profile"
    state.log(f"[1/4] Loading profile for table: '{state.table_name}'")

    try:
        config = ConnectionConfig(**state.source_config)
        connector = get_connector(config)
        connector.connect()

        if not connector.is_connected():
            state.error = f"Failed to connect to source '{config.name}'"
            return state

        tables = connector.list_tables()
        if state.table_name not in tables:
            state.error = f"Table '{state.table_name}' not found. Available: {tables}"
            connector.disconnect()
            return state

        state.raw_profile = connector.get_full_profile(state.table_name)
        # Stash connector in extra for later nodes to reuse
        state.source_config["_connector_ref"] = connector
        state.log(f"    ✓ Profile loaded: {state.raw_profile['row_count']:,} rows, {state.raw_profile['column_count']} columns")

    except Exception as e:
        state.error = f"Profile loading failed: {str(e)}"
        logger.exception("node_load_profile error")

    return state


# ─────────────────────────────────────────────────────────────────
# NODE 2: Run all 8 DQ checks
# ─────────────────────────────────────────────────────────────────

def node_run_checks(state: ProfilingAgentState) -> ProfilingAgentState:
    """
    Runs all 8 DQ check categories against the loaded profile.
    Each check returns a list of CheckResult objects.
    All results are aggregated into state.check_results.
    """
    state.current_step = "run_checks"
    state.log("[2/4] Running DQ checks...")

    connector = state.source_config.get("_connector_ref")
    profile = state.raw_profile
    all_results: List[CheckResult] = []

    checks_to_run = [
        ("Completeness",   lambda: check_completeness(profile)),
        ("Empty Strings",  lambda: check_empty_strings(profile, connector)),
        ("Uniqueness",     lambda: check_uniqueness(profile, connector)),
        ("Validity",       lambda: check_validity(profile, connector)),
        ("Timeliness",     lambda: check_timeliness(profile, connector)),
        ("Consistency",    lambda: check_consistency(profile, connector)),
        ("Schema",         lambda: check_schema(profile, connector)),
    ]

    for check_name, check_fn in checks_to_run:
        try:
            results = check_fn()
            all_results.extend(results)
            passed = sum(1 for r in results if r.passed)
            state.log(f"    ✓ {check_name}: {len(results)} checks ({passed} passed)")
        except Exception as e:
            logger.warning(f"Check '{check_name}' failed: {e}")
            state.log(f"    ✗ {check_name}: ERROR — {e}")

    state.check_results = all_results
    state.log(f"    Total: {len(all_results)} checks run")
    return state


# ─────────────────────────────────────────────────────────────────
# NODE 3: Calculate DQ Score and summarise issues
# ─────────────────────────────────────────────────────────────────

def node_score_and_summarise(state: ProfilingAgentState) -> ProfilingAgentState:
    """
    Aggregates all check results into:
    - Per-dimension scores (0–100 each)
    - Weighted overall DQ score (0–100)
    - Critical issues list
    - Warnings list
    """
    state.current_step = "score_and_summarise"
    state.log("[3/4] Calculating DQ scores...")

    checks = state.check_results

    # Group scores by dimension
    dimension_scores: Dict[str, List[float]] = {k: [] for k in DIMENSION_WEIGHTS}
    for check in checks:
        dim = check.check_type.value
        if dim in dimension_scores:
            dimension_scores[dim].append(check.score)

    # Average score per dimension (100 if no checks ran for that dimension)
    avg_dimension_scores: Dict[str, float] = {}
    for dim, scores in dimension_scores.items():
        avg_dimension_scores[dim] = round(sum(scores) / len(scores), 1) if scores else 100.0

    # Weighted overall score
    total_weight = sum(DIMENSION_WEIGHTS.values())
    overall = sum(
        avg_dimension_scores[dim] * weight
        for dim, weight in DIMENSION_WEIGHTS.items()
    ) / total_weight

    # Collect human-readable issues by severity
    critical_issues = [
        f"[{c.column or 'TABLE'}] {c.message}"
        for c in checks
        if c.severity == Severity.CRITICAL and not c.passed
    ]
    warnings = [
        f"[{c.column or 'TABLE'}] {c.message}"
        for c in checks
        if c.severity == Severity.WARNING and not c.passed
    ]

    state.log(f"    Overall DQ Score: {round(overall, 1)}/100")
    state.log(f"    Critical issues: {len(critical_issues)}")
    state.log(f"    Warnings: {len(warnings)}")

    # Build the report object
    profile = state.raw_profile
    state.report = TableDQReport(
        source_name=profile.get("source", ""),
        source_type=profile.get("source_type", ""),
        table_name=state.table_name,
        row_count=profile.get("row_count", 0),
        column_count=profile.get("column_count", 0),
        checks=checks,
        dimension_scores=avg_dimension_scores,
        overall_dq_score=round(overall, 1),
        critical_issues=critical_issues,
        warnings=warnings,
        profiled_at=datetime.now().isoformat(),
    )

    return state


# ─────────────────────────────────────────────────────────────────
# NODE 4: Finalise — cleanup and set profiling time
# ─────────────────────────────────────────────────────────────────

def node_finalise(state: ProfilingAgentState) -> ProfilingAgentState:
    """Disconnect from source, record total time, mark complete."""
    state.current_step = "finalise"

    connector = state.source_config.pop("_connector_ref", None)
    if connector:
        connector.disconnect()

    state.log("[4/4] Profiling complete.")
    return state


# ─────────────────────────────────────────────────────────────────
# ERROR NODE
# ─────────────────────────────────────────────────────────────────

def node_handle_error(state: ProfilingAgentState) -> ProfilingAgentState:
    logger.error(f"Profiling agent error: {state.error}")
    state.current_step = "error"
    state.log(f"✗ Aborted: {state.error}")
    return state


# ─────────────────────────────────────────────────────────────────
# ROUTING — after load_profile, should we continue or handle error?
# ─────────────────────────────────────────────────────────────────

def route_after_load(state: ProfilingAgentState) -> Literal["run_checks", "handle_error"]:
    return "handle_error" if state.error else "run_checks"


# ─────────────────────────────────────────────────────────────────
# BUILD THE LANGGRAPH (if available) or fallback pipeline
# ─────────────────────────────────────────────────────────────────

def build_profiling_graph():
    """
    Assembles the LangGraph state machine.
    Falls back to a simple sequential pipeline if LangGraph isn't installed yet.
    """
    if not LANGGRAPH_AVAILABLE:
        logger.warning("LangGraph not installed — using sequential fallback pipeline.")
        return None

    graph = StateGraph(ProfilingAgentState)

    # Register nodes
    graph.add_node("load_profile",       node_load_profile)
    graph.add_node("run_checks",         node_run_checks)
    graph.add_node("score_and_summarise",node_score_and_summarise)
    graph.add_node("finalise",           node_finalise)
    graph.add_node("handle_error",       node_handle_error)

    # Set entry point
    graph.set_entry_point("load_profile")

    # Edges
    graph.add_conditional_edges("load_profile", route_after_load)
    graph.add_edge("run_checks",          "score_and_summarise")
    graph.add_edge("score_and_summarise", "finalise")
    graph.add_edge("finalise",            END)
    graph.add_edge("handle_error",        END)

    return graph.compile()


# ─────────────────────────────────────────────────────────────────
# FALLBACK — simple sequential pipeline (no LangGraph needed)
# ─────────────────────────────────────────────────────────────────

def _run_sequential(state: ProfilingAgentState) -> ProfilingAgentState:
    """Run all nodes sequentially. Used when LangGraph is not installed."""
    state = node_load_profile(state)
    if state.error:
        return node_handle_error(state)
    state = node_run_checks(state)
    state = node_score_and_summarise(state)
    state = node_finalise(state)
    return state


# ─────────────────────────────────────────────────────────────────
# PUBLIC API — this is what everything else calls
# ─────────────────────────────────────────────────────────────────

_compiled_graph = None


def run_profiling_agent(
    source_config: Dict[str, Any],
    table_name: str,
) -> TableDQReport:
    """
    Main entry point for the Profiling Agent.
    
    Args:
        source_config: Dict matching ConnectionConfig fields
                       e.g. {"source_type": "sqlite", "name": "Retail", "db_path": "./retail.db"}
        table_name:    Which table to profile
    
    Returns:
        TableDQReport with overall DQ score, all check results, issues list
    
    Example:
        report = run_profiling_agent(
            source_config={"source_type": "sqlite", "name": "Retail DB", "db_path": "./sample_data/retail.db"},
            table_name="orders"
        )
        print(f"DQ Score: {report.overall_dq_score}/100 — {report.score_label()}")
    """
    global _compiled_graph

    start_time = time.time()

    initial_state = ProfilingAgentState(
        source_config=source_config.copy(),
        table_name=table_name,
    )

    if LANGGRAPH_AVAILABLE:
        if _compiled_graph is None:
            _compiled_graph = build_profiling_graph()
        result = _compiled_graph.invoke(initial_state)
        # LangGraph may return a plain dict — handle both cases
        if isinstance(result, dict):
            final_state = initial_state
            for k, v in result.items():
                if hasattr(final_state, k):
                    setattr(final_state, k, v)
        else:
            final_state = result
    else:
        final_state = _run_sequential(initial_state)

    elapsed = round((time.time() - start_time) * 1000, 1)

    report = getattr(final_state, "report", None)
    if report:
        report.profiling_time_ms = elapsed
        return report

    # Error case — return a minimal report
    error_msg = getattr(final_state, "error", None) or "Unknown error"
    return TableDQReport(
        source_name=source_config.get("name", "unknown"),
        source_type=source_config.get("source_type", "unknown"),
        table_name=table_name,
        row_count=0,
        column_count=0,
        overall_dq_score=0.0,
        critical_issues=[error_msg],
        profiled_at=datetime.now().isoformat(),
        profiling_time_ms=elapsed,
    )