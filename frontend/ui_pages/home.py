"""
Home Page — Welcome screen + quick stats dashboard.
"""

import os
import streamlit as st
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from connectors import get_connector, ConnectionConfig


def render_home():
    st.markdown("# 🔍 Data Quality Engine")
    st.markdown("##### Automated multi-agent data quality analysis")
    st.markdown("---")

    # ── What is DQ Engine ─────────────────────────────────────────
    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown("### What it does")
        features = [
            ("🧠", "**8 DQ Checks**",       "Completeness, uniqueness, validity, timeliness, accuracy, consistency, schema, empty strings"),
            ("💬", "**Natural Language SQL**","Ask questions in plain English — GPT generates and runs the SQL"),
            ("📊", "**DQ Score (0–100)**",   "Every table gets a weighted quality score with dimension breakdown"),
            ("📄", "**HTML Reports**",        "Auto-generated reports with Plotly charts, issue summaries, all checks"),
            ("🔗", "**Multi-source**",        "SQLite, PostgreSQL, CSV — same interface for all"),
        ]
        for icon, title, desc in features:
            st.markdown(f"""
            <div style='display:flex;gap:12px;align-items:flex-start;
                        margin-bottom:12px;background:#1a1d27;
                        border:1px solid #2e3248;border-radius:8px;padding:12px 16px;'>
              <span style='font-size:22px'>{icon}</span>
              <div>
                <div style='font-weight:600;margin-bottom:2px'>{title}</div>
                <div style='font-size:13px;color:#7b80a0'>{desc}</div>
              </div>
            </div>""", unsafe_allow_html=True)

    with col2:
        st.markdown("### Quick Start")
        st.markdown("""
        <div style='background:#1a1d27;border:1px solid #2e3248;
                    border-radius:10px;padding:20px;font-size:14px;'>
          <div style='margin-bottom:10px;'>
            <span style='color:#5c6bc0;font-weight:700;'>Step 1</span><br/>
            Select a <b>Data Source</b> in the sidebar
          </div>
          <div style='margin-bottom:10px;'>
            <span style='color:#5c6bc0;font-weight:700;'>Step 2</span><br/>
            Go to <b>🔍 Profile Table</b> → pick a table → Run
          </div>
          <div style='margin-bottom:10px;'>
            <span style='color:#5c6bc0;font-weight:700;'>Step 3</span><br/>
            View your <b>DQ Score</b>, issues, and dimension breakdown
          </div>
          <div style='margin-bottom:10px;'>
            <span style='color:#5c6bc0;font-weight:700;'>Step 4</span><br/>
            Ask questions in <b>💬 NL Query</b>
          </div>
          <div>
            <span style='color:#5c6bc0;font-weight:700;'>Step 5</span><br/>
            Download your <b>📊 HTML Report</b>
          </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Live source preview ───────────────────────────────────────
    st.markdown("### 📡 Current Source Preview")
    source_cfg = st.session_state.get("source_config", {})

    if not source_cfg or not source_cfg.get("name"):
        st.info("Configure a data source in the sidebar to see a preview.")
        return

    try:
        config    = ConnectionConfig(**source_cfg)
        connector = get_connector(config)
        connector.connect()

        if not connector.is_connected():
            st.error("Could not connect to data source.")
            return

        tables = connector.list_tables()
        connector.disconnect()

        st.success(f"✅ Connected to **{source_cfg['name']}** ({source_cfg['source_type'].upper()})")

        # Show table cards
        st.markdown(f"**{len(tables)} table(s) available:**")
        cols = st.columns(min(len(tables), 4))
        for i, table in enumerate(tables):
            with cols[i % 4]:
                st.markdown(f"""
                <div style='background:#1a1d27;border:1px solid #2e3248;
                            border-radius:8px;padding:14px;text-align:center;
                            cursor:pointer;'>
                  <div style='font-size:22px'>🗃️</div>
                  <div style='font-weight:600;margin-top:6px;font-size:14px'>{table}</div>
                </div>""", unsafe_allow_html=True)

        # Navigation hint
        st.markdown("")
        if st.button("🔍 Profile a Table →", type="primary", use_container_width=False):
            st.session_state.page = "🔍 Profile Table"
            st.rerun()

    except Exception as e:
        st.error(f"Connection error: {e}")

    # ── Recent history ────────────────────────────────────────────
    history = st.session_state.get("history", [])
    if history:
        st.markdown("---")
        st.markdown("### 🕐 Recent Analyses")
        for item in reversed(history[-5:]):
            score = item.get("score", 0)
            color = "#26a69a" if score >= 90 else "#66bb6a" if score >= 75 else "#ffa726" if score >= 50 else "#ef5350"
            st.markdown(f"""
            <div style='display:flex;justify-content:space-between;align-items:center;
                        background:#1a1d27;border:1px solid #2e3248;border-radius:8px;
                        padding:10px 16px;margin-bottom:8px;font-size:13px;'>
              <span>🗃️ <b>{item.get('table')}</b> — {item.get('source')}</span>
              <span style='color:{color};font-weight:700;'>{score}/100</span>
            </div>""", unsafe_allow_html=True)