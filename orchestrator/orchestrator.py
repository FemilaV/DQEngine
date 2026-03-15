"""
DQ Engine Orchestrator — Master LangGraph pipeline.

Connects all three agents into one automated workflow:

    [validate_input]
          ↓
    [run_profiling]      profiling_agent  → dq_report
          ↓
    [run_nl_sql]         nl_sql_agent     → nl_sql_result  (skipped if no question)
          ↓
    [generate_report]    report_agent     → HTML file
          ↓
    [summarise]          packages everything into PipelineResult
          ↓
         END

Error at any stage → [handle_error] → END

Usage (simplest possible):
    from orchestrator import run_dq_pipeline

    result = run_dq_pipeline(
        source_config={"source_type": "sqlite", "name": "Retail", "db_path": "./retail.db"},
        table_name="orders",
        nl_question="show me orders with missing customer id",
    )
    print(f"Score : {result.overall_score}/100 — {result.score_label}")
    print(f"Report: {result.report_path}")
    print(f"SQL   : {result.nl_sql_result.generated_sql}")
"""

import time
import logging
from typing import Any, Dict, List, Literal, Optional

try:
    from langgraph.graph import StateGraph, END
    LANGGRAPH_AVAILABLE = True
except ImportError:
    LANGGRAPH_AVAILABLE = False

from orchestrator.orchestrator_state import (
    OrchestratorState, PipelineRequest, PipelineResult
)
from agents.profiling_agent import run_profiling_agent
from agents.nl_sql_agent import run_nl_sql_agent
from agents.report_agent import run_report_agent

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# NODE 1 — Validate Input
# ─────────────────────────────────────────────────────────────────

def node_validate_input(state: OrchestratorState) -> OrchestratorState:
    """
    Sanity-check the request before spending time on agents.
    Catches obvious mistakes early with clear error messages.
    """
    state.current_step = "validate_input"
    state.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    state.log("🔍 DQ Engine Pipeline Starting")
    state.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    req = state.request

    if not req:
        state.error = "No request provided to pipeline."
        return state

    if not req.source_config:
        state.error = "source_config is required. Provide at least source_type, name, and connection details."
        return state

    if not req.table_name or not req.table_name.strip():
        state.error = "table_name is required."
        return state

    source_type = req.source_config.get("source_type", "").lower()
    if source_type not in ("sqlite", "postgres", "postgresql", "csv"):
        state.error = f"Unknown source_type '{source_type}'. Use: sqlite, postgres, csv"
        return state

    # Source-specific checks
    if source_type == "sqlite" and not req.source_config.get("db_path"):
        state.error = "SQLite requires 'db_path' in source_config."
        return state
    if source_type == "csv" and not req.source_config.get("file_path"):
        state.error = "CSV requires 'file_path' in source_config."
        return state
    if source_type in ("postgres", "postgresql"):
        for field in ("host", "database", "username", "password"):
            if not req.source_config.get(field):
                state.error = f"PostgreSQL requires '{field}' in source_config."
                return state

    state.log(f"[1/5] ✓ Input validated")
    state.log(f"      Source : {req.source_config.get('name')} ({source_type})")
    state.log(f"      Table  : {req.table_name}")
    if req.nl_question:
        state.log(f"      Query  : \"{req.nl_question}\"")

    return state


# ─────────────────────────────────────────────────────────────────
# NODE 2 — Run Profiling Agent
# ─────────────────────────────────────────────────────────────────

def node_run_profiling(state: OrchestratorState) -> OrchestratorState:
    """
    Runs the full profiling agent — 8 DQ checks, scoring,
    issue detection. Stores the TableDQReport in state.
    """
    state.current_step = "run_profiling"
    state.log(f"[2/5] Running Profiling Agent on '{state.request.table_name}'...")

    try:
        dq_report = run_profiling_agent(
            source_config=state.request.source_config,
            table_name=state.request.table_name,
        )

        state.dq_report = dq_report
        state.profiling_done = True

        score = dq_report.overall_dq_score
        label = dq_report.score_label()
        state.log(f"      ✓ Score   : {score}/100 — {label}")
        state.log(f"      ✓ Rows    : {dq_report.row_count:,}")
        state.log(f"      ✓ Checks  : {len(dq_report.checks)} run, "
                  f"{len(dq_report.failed_checks())} failed")
        state.log(f"      ✓ Critical: {len(dq_report.critical_issues)} issue(s)")

    except Exception as e:
        state.error = f"Profiling agent failed: {str(e)}"
        logger.exception("node_run_profiling error")

    return state


# ─────────────────────────────────────────────────────────────────
# NODE 3 — Run NL→SQL Agent (conditional)
# ─────────────────────────────────────────────────────────────────

def node_run_nl_sql(state: OrchestratorState) -> OrchestratorState:
    """
    Only runs if the user provided a nl_question.
    Uses the same source_config so it queries the same database.
    """
    state.current_step = "run_nl_sql"
    req = state.request

    # Skip if no question or explicitly skipped
    if not req.nl_question or req.skip_nl_sql:
        state.log("[3/5] ⏭  NL→SQL skipped (no question provided)")
        state.nl_sql_done = True
        return state

    state.log(f"[3/5] Running NL→SQL Agent...")
    state.log(f"      Q: \"{req.nl_question}\"")

    try:
        nl_result = run_nl_sql_agent(
            natural_language=req.nl_question,
            source_config=req.source_config,
            target_tables=[req.table_name],
            max_rows=req.max_rows,
        )

        state.nl_sql_result = nl_result
        state.nl_sql_done = True

        if nl_result.success:
            state.log(f"      ✓ SQL     : {nl_result.generated_sql[:80]}...")
            state.log(f"      ✓ Rows    : {nl_result.row_count}")
            state.log(f"      ✓ Model   : {nl_result.model_used}")
        else:
            # NL→SQL failure is non-fatal — pipeline continues
            state.log(f"      ⚠ NL→SQL failed (non-fatal): {nl_result.error}")

    except Exception as e:
        # Non-fatal — log and continue
        state.log(f"      ⚠ NL→SQL error (non-fatal): {str(e)}")
        state.nl_sql_done = True
        logger.warning(f"node_run_nl_sql error: {e}")

    return state


# ─────────────────────────────────────────────────────────────────
# NODE 4 — Generate Report
# ─────────────────────────────────────────────────────────────────

def node_generate_report(state: OrchestratorState) -> OrchestratorState:
    """
    Generates the HTML report from the DQ report.
    Saves to disk and also stores the HTML string for Streamlit.
    """
    state.current_step = "generate_report"

    if state.request.skip_report:
        state.log("[4/5] ⏭  Report generation skipped")
        state.report_done = True
        return state

    state.log("[4/5] Generating HTML Report...")

    try:
        result = run_report_agent(
            report=state.dq_report,
            output_dir=state.request.output_dir,
            return_html=True,     # Also return HTML string for Streamlit
        )

        if result["success"]:
            state.report_path = result["file_path"]
            state.report_html = result.get("html", "")
            state.report_done = True
            state.log(f"      ✓ Saved   : {state.report_path}")
        else:
            state.log(f"      ⚠ Report failed (non-fatal): {result.get('error')}")
            state.report_done = True

    except Exception as e:
        state.log(f"      ⚠ Report error (non-fatal): {str(e)}")
        state.report_done = True
        logger.warning(f"node_generate_report error: {e}")

    return state


# ─────────────────────────────────────────────────────────────────
# NODE 5 — Summarise
# ─────────────────────────────────────────────────────────────────

def node_summarise(state: OrchestratorState, start_time: float = None) -> OrchestratorState:
    """
    Packages everything into the final PipelineResult.
    Prints a clean summary to logs.
    """
    state.current_step = "summarise"
    state.log("[5/5] Packaging final result...")

    elapsed = round((time.time() - (start_time or time.time())) * 1000, 1)

    completed = []
    if state.profiling_done: completed.append("profiling")
    if state.nl_sql_done:    completed.append("nl_sql")
    if state.report_done:    completed.append("report")

    state.result = PipelineResult(
        dq_report=state.dq_report,
        overall_score=state.dq_report.overall_dq_score if state.dq_report else 0.0,
        score_label=state.dq_report.score_label() if state.dq_report else "Unknown",
        nl_sql_result=state.nl_sql_result,
        report_path=state.report_path,
        report_html=state.report_html,
        success=True,
        total_time_ms=elapsed,
        stages_completed=completed,
    )

    state.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    state.log(f"✅ Pipeline complete in {elapsed}ms")
    state.log(f"   Score   : {state.result.overall_score}/100 — {state.result.score_label}")
    state.log(f"   Stages  : {', '.join(completed)}")
    if state.report_path:
        state.log(f"   Report  : {state.report_path}")
    state.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    return state


# ─────────────────────────────────────────────────────────────────
# ERROR NODE
# ─────────────────────────────────────────────────────────────────

def node_handle_error(state: OrchestratorState) -> OrchestratorState:
    state.current_step = "error"
    logger.error(f"[Orchestrator] Pipeline failed: {state.error}")
    state.log(f"✗ Pipeline aborted: {state.error}")

    state.result = PipelineResult(
        success=False,
        error=state.error,
        stages_completed=[s for s in ["profiling", "nl_sql", "report"]
                          if getattr(state, f"{s.replace('_sql','_sql')}_done", False)],
    )
    return state


# ─────────────────────────────────────────────────────────────────
# ROUTING
# ─────────────────────────────────────────────────────────────────

def route_after_validate(state: OrchestratorState) -> Literal["run_profiling", "handle_error"]:
    return "handle_error" if state.error else "run_profiling"

def route_after_profiling(state: OrchestratorState) -> Literal["run_nl_sql", "handle_error"]:
    return "handle_error" if state.error else "run_nl_sql"

def route_after_nl_sql(state: OrchestratorState) -> Literal["generate_report", "handle_error"]:
    # NL→SQL errors are non-fatal — always continue to report
    return "generate_report"

def route_after_report(state: OrchestratorState) -> Literal["summarise", "handle_error"]:
    # Report errors are non-fatal — always continue to summarise
    return "summarise"


# ─────────────────────────────────────────────────────────────────
# BUILD LANGGRAPH
# ─────────────────────────────────────────────────────────────────

def build_orchestrator_graph(start_time: float):
    if not LANGGRAPH_AVAILABLE:
        return None

    graph = StateGraph(OrchestratorState)

    # Wrap summarise to capture start_time in closure
    def summarise_node(state):
        return node_summarise(state, start_time=start_time)

    graph.add_node("validate_input",   node_validate_input)
    graph.add_node("run_profiling",    node_run_profiling)
    graph.add_node("run_nl_sql",       node_run_nl_sql)
    graph.add_node("generate_report",  node_generate_report)
    graph.add_node("summarise",        summarise_node)
    graph.add_node("handle_error",     node_handle_error)

    graph.set_entry_point("validate_input")

    graph.add_conditional_edges("validate_input",  route_after_validate)
    graph.add_conditional_edges("run_profiling",   route_after_profiling)
    graph.add_conditional_edges("run_nl_sql",      route_after_nl_sql)
    graph.add_conditional_edges("generate_report", route_after_report)
    graph.add_edge("summarise",    END)
    graph.add_edge("handle_error", END)

    return graph.compile()


# ─────────────────────────────────────────────────────────────────
# SEQUENTIAL FALLBACK
# ─────────────────────────────────────────────────────────────────

def _run_sequential(state: OrchestratorState, start_time: float) -> OrchestratorState:
    state = node_validate_input(state)
    if state.error: return node_handle_error(state)

    state = node_run_profiling(state)
    if state.error: return node_handle_error(state)

    state = node_run_nl_sql(state)       # Non-fatal
    state = node_generate_report(state)  # Non-fatal
    return node_summarise(state, start_time=start_time)


# ─────────────────────────────────────────────────────────────────
# PUBLIC API — the single function everything else calls
# ─────────────────────────────────────────────────────────────────

def run_dq_pipeline(
    source_config: Dict[str, Any],
    table_name: str,
    nl_question: Optional[str] = None,
    max_rows: int = 100,
    output_dir: Optional[str] = None,
    skip_nl_sql: bool = False,
    skip_report: bool = False,
) -> PipelineResult:
    """
    Run the full Data Quality pipeline on a table.

    This is the ONLY function FastAPI and Streamlit need to call.

    Args:
        source_config : ConnectionConfig dict
                        e.g. {"source_type": "sqlite", "name": "Retail", "db_path": "..."}
        table_name    : Table to analyse
        nl_question   : Optional natural language question
                        e.g. "show me rows with missing customer id"
        max_rows      : Max rows for NL→SQL results (default 100)
        output_dir    : Where to save the HTML report
        skip_nl_sql   : Skip NL→SQL even if question provided
        skip_report   : Skip HTML report generation

    Returns:
        PipelineResult with:
          .overall_score    — DQ score 0–100
          .score_label      — "Excellent" / "Good" / "Fair" / "Poor" / "Critical"
          .dq_report        — Full TableDQReport with all checks
          .nl_sql_result    — SQL + data (if nl_question was given)
          .report_path      — Path to saved HTML report
          .report_html      — HTML string (for Streamlit)
          .stages_completed — ["profiling", "nl_sql", "report"]
          .total_time_ms    — Total wall time

    Examples:
        # Basic profiling + report
        result = run_dq_pipeline(
            source_config={"source_type": "sqlite", "name": "DB", "db_path": "./retail.db"},
            table_name="orders",
        )

        # With NL question
        result = run_dq_pipeline(
            source_config={"source_type": "csv", "name": "Customers", "file_path": "./customers.csv"},
            table_name="customers",
            nl_question="how many customers have missing email addresses?",
        )

        # Profiling only, no report
        result = run_dq_pipeline(
            source_config={...},
            table_name="orders",
            skip_report=True,
        )
    """
    start_time = time.time()

    request = PipelineRequest(
        source_config=source_config,
        table_name=table_name,
        nl_question=nl_question,
        max_rows=max_rows,
        output_dir=output_dir,
        skip_nl_sql=skip_nl_sql,
        skip_report=skip_report,
    )
    initial_state = OrchestratorState(request=request)

    if LANGGRAPH_AVAILABLE:
        graph = build_orchestrator_graph(start_time)
        raw = graph.invoke(initial_state)
        # Handle LangGraph returning dict
        if isinstance(raw, dict):
            final_state = initial_state
            for k, v in raw.items():
                if hasattr(final_state, k):
                    setattr(final_state, k, v)
        else:
            final_state = raw
    else:
        logger.warning("[Orchestrator] LangGraph not installed — using sequential fallback.")
        final_state = _run_sequential(initial_state, start_time)

    return final_state.result or PipelineResult(
        success=False,
        error="Pipeline produced no result.",
        total_time_ms=round((time.time() - start_time) * 1000, 1),
    )