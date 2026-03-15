"""
NL Query Page — Ask questions in plain English, get SQL + results.
"""

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import streamlit as st
import pandas as pd

from connectors import get_connector, ConnectionConfig
from agents.nl_sql_agent import run_nl_sql_agent
from agents.llm_provider import get_active_provider


# ── Example questions per table ───────────────────────────────────
EXAMPLE_QUESTIONS = {
    "orders":      [
        "Show me all orders where customer_id is missing",
        "How many orders are there per status?",
        "What is the average unit price?",
        "Show orders with null region",
        "Find orders placed in 2023",
    ],
    "products":    [
        "Show products with missing category",
        "What are the top 5 most expensive products?",
        "How many products are in each category?",
        "Show products with zero stock",
    ],
    "employees":   [
        "Show employees with missing department",
        "What is the average salary by department?",
        "How many active vs inactive employees are there?",
        "Show employees hired before 2020",
    ],
    "customers":   [
        "Show customers with missing email address",
        "How many customers are there per city?",
        "Find customers with no phone number",
        "What is the average lifetime value?",
    ],
    "transactions": [
        "Show transactions with negative amounts",
        "What is the total amount by currency?",
        "Find flagged transactions",
        "Show transactions with missing category",
    ],
}


def render_nlquery():
    st.markdown("# 💬 Natural Language Query")
    st.markdown("Ask questions about your data in plain English.")
    st.markdown("---")

    source_cfg = st.session_state.get("source_config", {})
    if not source_cfg or not source_cfg.get("name"):
        st.warning("⚠️ Configure a data source in the sidebar first.")
        return

    provider = get_active_provider()
    if provider == "mock":
        st.warning("""⚠️ **No LLM API key detected** — using rule-based mock provider.
        Set `OPENAI_API_KEY` or `GROQ_API_KEY` in your `.env` file for full NL→SQL capability.
        Free Groq key: https://console.groq.com""")

    # ── Table picker ──────────────────────────────────────────────
    try:
        config    = ConnectionConfig(**source_cfg)
        connector = get_connector(config)
        connector.connect()
        tables    = connector.list_tables()
        connector.disconnect()
    except Exception as e:
        st.error(f"Connection error: {e}")
        return

    col1, col2 = st.columns([2, 2])
    with col1:
        table_name = st.selectbox("Target Table", tables, key="nl_table")
    with col2:
        max_rows = st.slider("Max rows to return", 10, 500, 100, key="nl_max_rows")

    # ── Example questions ─────────────────────────────────────────
    examples = EXAMPLE_QUESTIONS.get(table_name, [
        f"Show me the first 10 rows from {table_name}",
        f"How many rows are in {table_name}?",
        f"Show rows where any column has null values",
    ])

    st.markdown("**💡 Try an example:**")
    ex_cols = st.columns(len(examples[:3]))
    clicked_example = None
    for i, (col, ex) in enumerate(zip(ex_cols, examples[:3])):
        with col:
            if st.button(f"_{ex[:45]}_", key=f"ex_{i}", use_container_width=True):
                clicked_example = ex

    # ── Question input ────────────────────────────────────────────
    default_q = clicked_example or st.session_state.get("nl_question_input", "")
    question = st.text_area(
        "Your question",
        value=default_q,
        height=80,
        placeholder=f"e.g. Show me rows from {table_name} where customer_id is missing...",
        key="nl_question_input",
    )

    run_col, clear_col, _ = st.columns([1, 1, 4])
    with run_col:
        run_btn = st.button("▶ Run Query", type="primary", use_container_width=True,
                            disabled=not question.strip())
    with clear_col:
        if st.button("🗑 Clear", use_container_width=True):
            st.session_state.nl_question_input = ""
            st.session_state.last_query = None
            st.rerun()

    # ── Execute ───────────────────────────────────────────────────
    if run_btn and question.strip():
        with st.spinner("🤔 Generating SQL and running query..."):
            result = run_nl_sql_agent(
                natural_language=question.strip(),
                source_config=source_cfg,
                target_tables=[table_name],
                max_rows=max_rows,
            )
        st.session_state.last_query = result

    # ── Show results ──────────────────────────────────────────────
    result = st.session_state.get("last_query")
    if not result:
        st.info("Enter a question above and click **▶ Run Query**.")
        return

    st.markdown("---")

    if not result.success:
        st.error(f"❌ Query failed: {result.error}")
        return

    # ── Generated SQL ─────────────────────────────────────────────
    st.markdown("#### Generated SQL")
    col_sql, col_meta = st.columns([3, 1])
    with col_sql:
        st.markdown(f"""
        <div class='sql-block'>{result.generated_sql}</div>
        """, unsafe_allow_html=True)
    with col_meta:
        st.markdown(f"""
        <div style='background:#1a1d27;border:1px solid #2e3248;border-radius:8px;
                    padding:12px;font-size:12px;'>
          <div style='margin-bottom:6px;'>
            <span style='color:#7b80a0'>Provider</span><br/>
            <b>{result.llm_provider}</b>
          </div>
          <div style='margin-bottom:6px;'>
            <span style='color:#7b80a0'>Model</span><br/>
            <b style='font-size:11px'>{result.model_used}</b>
          </div>
          <div>
            <span style='color:#7b80a0'>Time</span><br/>
            <b>{result.execution_time_ms:.0f}ms</b>
          </div>
        </div>
        """, unsafe_allow_html=True)

    # ── Explanation ───────────────────────────────────────────────
    if result.explanation:
        st.info(f"💡 {result.explanation}")

    # ── Results table ─────────────────────────────────────────────
    st.markdown(f"#### Results — {result.row_count:,} rows")

    if result.data is not None and len(result.data) > 0:
        st.dataframe(result.data, use_container_width=True, hide_index=True)

        # Download results
        csv = result.data.to_csv(index=False)
        st.download_button(
            "⬇️ Download as CSV",
            data=csv,
            file_name=f"query_results_{table_name}.csv",
            mime="text/csv",
        )
    else:
        st.info("Query returned 0 rows.")

    # ── Query history ─────────────────────────────────────────────
    if "query_history" not in st.session_state:
        st.session_state.query_history = []

    if run_btn and result.success:
        st.session_state.query_history.append({
            "question": question.strip(),
            "sql": result.generated_sql,
            "rows": result.row_count,
            "table": table_name,
        })

    if st.session_state.query_history:
        with st.expander(f"🕐 Query History ({len(st.session_state.query_history)} queries)"):
            for item in reversed(st.session_state.query_history[-10:]):
                st.markdown(f"""
                <div style='background:#0d1117;border:1px solid #2e3248;border-radius:6px;
                            padding:10px 14px;margin-bottom:8px;font-size:12px;'>
                  <div style='color:#7b80a0;margin-bottom:4px;'>
                    Table: <b>{item['table']}</b> → {item['rows']} rows
                  </div>
                  <div style='color:#e8eaf6;margin-bottom:6px;'>❓ {item['question']}</div>
                  <div style='color:#79c0ff;font-family:monospace;'>
                    {item['sql'][:120]}{'...' if len(item['sql'])>120 else ''}
                  </div>
                </div>
                """, unsafe_allow_html=True)