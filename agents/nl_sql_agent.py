"""
NL→SQL Agent — LangGraph pipeline that turns natural language into SQL.

Graph flow:
  [load_schema] → [generate_sql] → [validate_sql] → [execute_sql] → [finalise]
                        ↓ (on bad SQL, retry once)
                   [generate_sql]  (retry node)
                        ↓ (error at any point)
                   [handle_error]

What each node does:
  load_schema   — connects to DB, fetches all table schemas as context for LLM
  generate_sql  — sends schema + question to LLM, gets back SQL + explanation
  validate_sql  — checks SQL is safe (no DROP/DELETE/UPDATE), fixes common issues
  execute_sql   — runs the SQL, returns DataFrame results
  finalise      — packages everything into NLSQLResult
"""

import re
import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

try:
    from langgraph.graph import StateGraph, END
    LANGGRAPH_AVAILABLE = True
except ImportError:
    LANGGRAPH_AVAILABLE = False

from models.nl_sql_models import (
    NLSQLRequest, NLSQLResult, GeneratedSQL, NLSQLAgentState
)
from agents.llm_provider import call_llm, parse_llm_json_response, get_active_provider
from connectors import get_connector, ConnectionConfig

logger = logging.getLogger(__name__)

# Max retries if SQL fails validation or execution
MAX_RETRIES = 1

# SQL operations that are never allowed (read-only enforcement)
FORBIDDEN_SQL = re.compile(
    r'\b(DROP|DELETE|UPDATE|INSERT|ALTER|TRUNCATE|CREATE|REPLACE|MERGE)\b',
    re.IGNORECASE
)


# ─────────────────────────────────────────────────────────────────
# SYSTEM PROMPT — the instructions we give the LLM
# ─────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert SQL query generator for a Data Quality Engine.
Your job is to convert natural language questions into precise, safe SQL queries.

RULES:
1. Generate READ-ONLY queries only (SELECT). Never use DROP, DELETE, UPDATE, INSERT.
2. Always use double quotes around table and column names: SELECT "column" FROM "table"
3. Always add LIMIT {limit} unless the user asks for counts/aggregations
4. Use standard SQL that works in SQLite AND PostgreSQL
5. If a column doesn't exist, use the closest match from the schema
6. For null checks: use IS NULL or IS NOT NULL (never = NULL)
7. For duplicate detection: use GROUP BY with HAVING COUNT(*) > 1

RESPONSE FORMAT — return ONLY valid JSON, no markdown, no explanation outside JSON:
{{
  "sql": "SELECT ... FROM ...",
  "explanation": "Plain English: what this query does and what it will show",
  "tables_used": ["table1", "table2"],
  "confidence": 0.95,
  "warnings": []
}}

If you cannot generate a valid query, return:
{{
  "sql": null,
  "explanation": "Reason why the query cannot be generated",
  "tables_used": [],
  "confidence": 0.0,
  "warnings": ["specific issue"]
}}"""


def _build_schema_context(connector, tables: List[str]) -> str:
    """
    Build a compact schema description to feed to the LLM.
    Format: TABLE: orders (500 rows)\n  order_id | INTEGER | not null\n  ...
    """
    lines = ["AVAILABLE DATABASE SCHEMA:\n"]
    for table in tables:
        try:
            profile = connector.get_table_profile(table)
            lines.append(f"TABLE: {table} ({profile.row_count:,} rows)")
            for col in profile.columns:
                nullable = "nullable" if col.get("nullable") == "True" else "not null"
                pk = " [PRIMARY KEY]" if col.get("primary_key") == "True" else ""
                lines.append(f"  {col['name']} | {col['type']} | {nullable}{pk}")
            lines.append("")
        except Exception as e:
            logger.warning(f"Could not get schema for table '{table}': {e}")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────
# NODE 1: Load schema from database
# ─────────────────────────────────────────────────────────────────

def node_load_schema(state: NLSQLAgentState) -> NLSQLAgentState:
    state.current_step = "load_schema"
    state.log(f"[1/5] Loading schema for NL query: '{state.request.natural_language}'")

    try:
        config = ConnectionConfig(**state.request.source_config)
        connector = get_connector(config)
        connector.connect()

        if not connector.is_connected():
            state.error = f"Could not connect to source '{config.name}'"
            return state

        all_tables = connector.list_tables()

        # If user specified target tables, use only those; otherwise use all
        if state.request.target_tables:
            tables = [t for t in state.request.target_tables if t in all_tables]
            if not tables:
                state.error = f"Specified tables {state.request.target_tables} not found. Available: {all_tables}"
                connector.disconnect()
                return state
        else:
            tables = all_tables

        state.available_tables = tables
        state.schema_context = _build_schema_context(connector, tables)
        state.request.source_config["_connector_ref"] = connector

        state.log(f"    ✓ Schema loaded for {len(tables)} table(s): {tables}")

    except Exception as e:
        state.error = f"Schema loading failed: {str(e)}"
        logger.exception("node_load_schema error")

    return state


# ─────────────────────────────────────────────────────────────────
# NODE 2: Generate SQL using LLM
# ─────────────────────────────────────────────────────────────────

def node_generate_sql(state: NLSQLAgentState) -> NLSQLAgentState:
    state.current_step = "generate_sql"
    retry_label = f" (retry {state.retry_count})" if state.retry_count > 0 else ""
    state.log(f"[2/5] Generating SQL{retry_label}...")

    try:
        system = SYSTEM_PROMPT.format(limit=state.request.max_rows)

        # Build user prompt with schema + question
        user_prompt = f"""{state.schema_context}

USER QUESTION: {state.request.natural_language}

Generate the SQL query. Remember: return ONLY JSON."""

        # Add retry context if this is a second attempt
        if state.retry_count > 0 and state.generated:
            user_prompt += f"""

PREVIOUS ATTEMPT FAILED:
SQL: {state.generated.sql}
Issue: The SQL had a problem. Please fix it and try again."""

        raw_response, model_name, tokens = call_llm(
            system_prompt=system,
            user_prompt=user_prompt,
            schema_context=state.schema_context,
        )

        parsed = parse_llm_json_response(raw_response)

        if not parsed.get("sql"):
            state.error = f"LLM could not generate SQL: {parsed.get('explanation', 'Unknown reason')}"
            return state

        state.generated = GeneratedSQL(
            sql=parsed["sql"].strip().rstrip(";"),   # Remove trailing semicolons
            explanation=parsed.get("explanation", ""),
            tables_used=parsed.get("tables_used", []),
            confidence=float(parsed.get("confidence", 0.5)),
            warnings=parsed.get("warnings", []),
        )

        state.log(f"    ✓ SQL generated (confidence: {state.generated.confidence:.0%})")
        state.log(f"    SQL: {state.generated.sql[:100]}{'...' if len(state.generated.sql) > 100 else ''}")

        # Store LLM metadata for the result
        state.request.source_config["_model_used"] = model_name
        state.request.source_config["_tokens_used"] = tokens
        state.request.source_config["_provider"] = get_active_provider()

    except Exception as e:
        state.error = f"SQL generation failed: {str(e)}"
        logger.exception("node_generate_sql error")

    return state


# ─────────────────────────────────────────────────────────────────
# NODE 3: Validate SQL — safety + basic syntax
# ─────────────────────────────────────────────────────────────────

def node_validate_sql(state: NLSQLAgentState) -> NLSQLAgentState:
    state.current_step = "validate_sql"
    state.log("[3/5] Validating SQL...")

    sql = state.generated.sql if state.generated else ""

    if not sql or not sql.strip():
        state.error = "Empty SQL generated."
        return state

    # Security check — read-only enforcement
    if FORBIDDEN_SQL.search(sql):
        forbidden = FORBIDDEN_SQL.search(sql).group(0).upper()
        state.error = f"Security: '{forbidden}' operations are not allowed. Only SELECT is permitted."
        return state

    # Must start with SELECT (or WITH for CTEs)
    sql_upper = sql.strip().upper()
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        state.error = f"SQL must start with SELECT or WITH. Got: {sql[:50]}"
        return state

    # Check referenced tables exist
    available = [t.lower() for t in state.available_tables]
    for table in state.generated.tables_used:
        if table.lower() not in available:
            # Try to auto-correct: find closest table name
            closest = next(
                (t for t in state.available_tables if t.lower() in sql.lower()),
                None
            )
            if closest:
                state.log(f"    ⚠ Auto-corrected table '{table}' → '{closest}'")
                state.generated.sql = re.sub(
                    re.escape(table), closest, state.generated.sql, flags=re.IGNORECASE
                )
            else:
                state.generated.warnings.append(
                    f"Table '{table}' may not exist. Available: {state.available_tables}"
                )

    state.log("    ✓ SQL validation passed")
    return state


# ─────────────────────────────────────────────────────────────────
# NODE 4: Execute SQL
# ─────────────────────────────────────────────────────────────────

def node_execute_sql(state: NLSQLAgentState) -> NLSQLAgentState:
    state.current_step = "execute_sql"
    state.log("[4/5] Executing SQL...")

    connector = state.request.source_config.get("_connector_ref")
    if not connector:
        state.error = "No connector available for execution."
        return state

    try:
        query_result = connector.execute_query(state.generated.sql)

        if not query_result.success:
            # Trigger retry if we haven't exceeded limit
            if state.retry_count < MAX_RETRIES:
                state.retry_count += 1
                state.error = None   # Clear error so retry can proceed
                state.log(f"    ✗ Execution failed: {query_result.error}. Retrying...")
                # Mark for retry — routing will send back to generate_sql
                state.current_step = "needs_retry"
                return state
            else:
                state.error = f"SQL execution failed after {MAX_RETRIES} retries: {query_result.error}"
                return state

        # Store result
        state.request.source_config["_query_result"] = query_result
        state.log(f"    ✓ Query returned {query_result.row_count:,} rows in {query_result.execution_time_ms}ms")

    except Exception as e:
        state.error = f"Execution error: {str(e)}"
        logger.exception("node_execute_sql error")

    return state


# ─────────────────────────────────────────────────────────────────
# NODE 5: Finalise — package into NLSQLResult
# ─────────────────────────────────────────────────────────────────

def node_finalise(state: NLSQLAgentState) -> NLSQLAgentState:
    state.current_step = "finalise"
    state.log("[5/5] Finalising result...")

    query_result = state.request.source_config.pop("_query_result", None)
    connector    = state.request.source_config.pop("_connector_ref", None)
    model_used   = state.request.source_config.pop("_model_used", "unknown")
    tokens_used  = state.request.source_config.pop("_tokens_used", 0)
    provider     = state.request.source_config.pop("_provider", "unknown")

    if connector:
        connector.disconnect()

    state.result = NLSQLResult(
        success=True,
        natural_language=state.request.natural_language,
        generated_sql=state.generated.sql if state.generated else None,
        explanation=state.generated.explanation if state.generated else None,
        data=query_result.data if query_result else None,
        row_count=query_result.row_count if query_result else 0,
        columns=query_result.columns if query_result else [],
        execution_time_ms=query_result.execution_time_ms if query_result else 0.0,
        llm_provider=provider,
        model_used=model_used,
        tokens_used=tokens_used,
    )

    state.log(f"    ✓ Done — {state.result.row_count} rows returned")
    return state


# ─────────────────────────────────────────────────────────────────
# ERROR NODE
# ─────────────────────────────────────────────────────────────────

def node_handle_error(state: NLSQLAgentState) -> NLSQLAgentState:
    state.current_step = "error"
    logger.error(f"NL→SQL agent error: {state.error}")

    connector = state.request.source_config.pop("_connector_ref", None)
    if connector:
        try: connector.disconnect()
        except: pass

    state.result = NLSQLResult(
        success=False,
        natural_language=state.request.natural_language if state.request else "",
        generated_sql=state.generated.sql if state.generated else None,
        explanation=None,
        error=state.error,
    )
    return state


# ─────────────────────────────────────────────────────────────────
# ROUTING
# ─────────────────────────────────────────────────────────────────

def route_after_load(state: NLSQLAgentState) -> Literal["generate_sql", "handle_error"]:
    return "handle_error" if state.error else "generate_sql"

def route_after_generate(state: NLSQLAgentState) -> Literal["validate_sql", "handle_error"]:
    return "handle_error" if state.error else "validate_sql"

def route_after_validate(state: NLSQLAgentState) -> Literal["execute_sql", "handle_error"]:
    return "handle_error" if state.error else "execute_sql"

def route_after_execute(state: NLSQLAgentState) -> Literal["finalise", "generate_sql", "handle_error"]:
    if state.error:
        return "handle_error"
    if state.current_step == "needs_retry":
        return "generate_sql"   # Loop back for retry
    return "finalise"


# ─────────────────────────────────────────────────────────────────
# BUILD LANGGRAPH
# ─────────────────────────────────────────────────────────────────

def build_nl_sql_graph():
    if not LANGGRAPH_AVAILABLE:
        return None

    graph = StateGraph(NLSQLAgentState)

    graph.add_node("load_schema",   node_load_schema)
    graph.add_node("generate_sql",  node_generate_sql)
    graph.add_node("validate_sql",  node_validate_sql)
    graph.add_node("execute_sql",   node_execute_sql)
    graph.add_node("finalise",      node_finalise)
    graph.add_node("handle_error",  node_handle_error)

    graph.set_entry_point("load_schema")

    graph.add_conditional_edges("load_schema",  route_after_load)
    graph.add_conditional_edges("generate_sql", route_after_generate)
    graph.add_conditional_edges("validate_sql", route_after_validate)
    graph.add_conditional_edges("execute_sql",  route_after_execute)
    graph.add_edge("finalise",     END)
    graph.add_edge("handle_error", END)

    return graph.compile()


# ─────────────────────────────────────────────────────────────────
# SEQUENTIAL FALLBACK
# ─────────────────────────────────────────────────────────────────

def _run_sequential(state: NLSQLAgentState) -> NLSQLAgentState:
    state = node_load_schema(state)
    if state.error: return node_handle_error(state)

    state = node_generate_sql(state)
    if state.error: return node_handle_error(state)

    state = node_validate_sql(state)
    if state.error: return node_handle_error(state)

    state = node_execute_sql(state)
    if state.current_step == "needs_retry":
        state = node_generate_sql(state)
        if state.error: return node_handle_error(state)
        state = node_validate_sql(state)
        if state.error: return node_handle_error(state)
        state = node_execute_sql(state)
        if state.error: return node_handle_error(state)

    if state.error: return node_handle_error(state)
    return node_finalise(state)


# ─────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────

_compiled_graph = None


def run_nl_sql_agent(
    natural_language: str,
    source_config: Dict[str, Any],
    target_tables: List[str] = None,
    max_rows: int = 100,
) -> NLSQLResult:
    """
    Main entry point. Convert a natural language question to SQL and run it.

    Args:
        natural_language: Plain English question
                          e.g. "show me all orders where customer is missing"
        source_config:    ConnectionConfig dict
                          e.g. {"source_type": "sqlite", "name": "Retail", "db_path": "..."}
        target_tables:    Optional list of tables to limit scope
        max_rows:         Max rows to return (default 100)

    Returns:
        NLSQLResult with generated SQL, explanation, and data

    Example:
        result = run_nl_sql_agent(
            natural_language="how many null values are in the orders table?",
            source_config={"source_type": "sqlite", "name": "Retail", "db_path": "./retail.db"},
        )
        if result.success:
            print(result.explanation)
            print(result.generated_sql)
            print(result.data)
    """
    global _compiled_graph

    request = NLSQLRequest(
        natural_language=natural_language,
        source_config=source_config.copy(),
        target_tables=target_tables or [],
        max_rows=max_rows,
    )

    initial_state = NLSQLAgentState(request=request)

    if LANGGRAPH_AVAILABLE:
        if _compiled_graph is None:
            _compiled_graph = build_nl_sql_graph()
        result = _compiled_graph.invoke(initial_state)
        # Handle LangGraph returning dict
        if isinstance(result, dict):
            final_state = initial_state
            for k, v in result.items():
                if hasattr(final_state, k):
                    setattr(final_state, k, v)
        else:
            final_state = result
    else:
        final_state = _run_sequential(initial_state)

    return final_state.result or NLSQLResult(
        success=False,
        natural_language=natural_language,
        generated_sql=None,
        explanation=None,
        error="Agent produced no result",
    )