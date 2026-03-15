"""
Profile Table Page — Run DQ checks and view full results.
"""

import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import streamlit as st
import pandas as pd

try:
    import plotly.graph_objects as go
    import plotly.express as px
    PLOTLY = True
except ImportError:
    PLOTLY = False

from connectors import get_connector, ConnectionConfig
from orchestrator import run_dq_pipeline


# ── Helpers ───────────────────────────────────────────────────────

def score_color(score):
    if score >= 90: return "#26a69a"
    if score >= 75: return "#66bb6a"
    if score >= 50: return "#ffa726"
    if score >= 30: return "#ff7043"
    return "#ef5350"

def score_css_class(label):
    return f"score-{label.lower()}"

def severity_emoji(sev):
    return {"critical": "🔴", "warning": "🟡", "info": "🔵", "pass": "✅"}.get(sev, "⚪")


# ── Dimension bar chart ───────────────────────────────────────────

def render_dimension_chart(dimension_scores: dict):
    if not PLOTLY:
        for dim, sc in dimension_scores.items():
            st.progress(sc / 100, text=f"{dim.replace('_',' ').title()}: {sc:.0f}")
        return

    dims   = [d.replace("_", " ").title() for d in dimension_scores.keys()]
    scores = list(dimension_scores.values())
    colors = [score_color(s) for s in scores]

    fig = go.Figure(go.Bar(
        x=scores, y=dims, orientation="h",
        marker=dict(color=colors, line=dict(width=0)),
        text=[f"{s:.0f}" for s in scores],
        textposition="outside",
        textfont=dict(color="#e8eaf6", size=12),
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e8eaf6"),
        xaxis=dict(range=[0, 115], gridcolor="#2e3248", linecolor="#2e3248"),
        yaxis=dict(gridcolor="#2e3248", linecolor="#2e3248"),
        margin=dict(l=0, r=40, t=10, b=10),
        height=280,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


# ── Null heatmap ──────────────────────────────────────────────────

def render_null_chart(checks: list):
    null_checks = [c for c in checks if c["check_type"] == "completeness" and c["column"]]
    if not null_checks or not PLOTLY:
        return

    cols   = [c["column"] for c in null_checks]
    nulls  = [c["detail"].get("null_pct", 0) for c in null_checks]
    colors = [score_color(100 - n) for n in nulls]

    paired = sorted(zip(nulls, cols, colors), reverse=True)[:12]
    nulls_s  = [p[0] for p in paired]
    cols_s   = [p[1] for p in paired]
    colors_s = [p[2] for p in paired]

    fig = go.Figure(go.Bar(
        x=nulls_s, y=cols_s, orientation="h",
        marker=dict(color=colors_s, line=dict(width=0)),
        text=[f"{n:.1f}%" for n in nulls_s],
        textposition="outside",
        textfont=dict(color="#e8eaf6", size=11),
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e8eaf6"),
        xaxis=dict(range=[0, max(nulls_s)*1.3+5] if nulls_s else [0,100],
                   gridcolor="#2e3248", linecolor="#2e3248",
                   title="Null %", title_font=dict(size=11)),
        yaxis=dict(gridcolor="#2e3248", linecolor="#2e3248"),
        margin=dict(l=0, r=60, t=10, b=30),
        height=280,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


# ── Main render ───────────────────────────────────────────────────

def render_profile():
    st.markdown("# 🔍 Profile Table")
    st.markdown("Run all 8 Data Quality checks on any table.")
    st.markdown("---")

    source_cfg = st.session_state.get("source_config", {})
    if not source_cfg or not source_cfg.get("name"):
        st.warning("⚠️ Configure a data source in the sidebar first.")
        return

    # ── Table picker ──────────────────────────────────────────────
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown(f"**Source:** `{source_cfg.get('name')}` ({source_cfg.get('source_type','').upper()})")

    # Load tables
    try:
        config    = ConnectionConfig(**source_cfg)
        connector = get_connector(config)
        connector.connect()
        tables    = connector.list_tables()
        connector.disconnect()
    except Exception as e:
        st.error(f"Could not connect: {e}")
        return

    if not tables:
        st.warning("No tables found in this source.")
        return

    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        table_name = st.selectbox("Select Table", tables, key="profile_table")
    with col2:
        skip_report = st.checkbox("Skip HTML report (faster)", value=False)
    with col3:
        st.markdown("<br/>", unsafe_allow_html=True)
        run_btn = st.button("▶ Run Analysis", type="primary", use_container_width=True)

    # ── Run pipeline ──────────────────────────────────────────────
    if run_btn:
        with st.spinner(f"Profiling `{table_name}`..."):
            result = run_dq_pipeline(
                source_config=source_cfg,
                table_name=table_name,
                skip_report=skip_report,
                skip_nl_sql=True,
            )

        if not result.success:
            st.error(f"❌ Pipeline failed: {result.error}")
            return

        st.session_state.last_profile = result
        # Add to history
        st.session_state.history.append({
            "table": table_name,
            "source": source_cfg.get("name"),
            "score": result.overall_score,
        })

    # ── Display results ───────────────────────────────────────────
    result = st.session_state.get("last_profile")
    if not result or not result.dq_report:
        st.info("Select a table and click **▶ Run Analysis** to begin.")
        return

    r = result.dq_report
    sc = result.overall_score
    label = result.score_label
    color = score_color(sc)

    # ── Score hero row ────────────────────────────────────────────
    st.markdown("---")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.markdown(f"""
        <div class='metric-card'>
          <div class='metric-val' style='color:{color}'>{sc}</div>
          <div class='metric-lbl'>DQ Score / 100</div>
          <div style='font-size:12px;font-weight:700;color:{color};margin-top:4px'>{label}</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class='metric-card'>
          <div class='metric-val'>{r.row_count:,}</div>
          <div class='metric-lbl'>Total Rows</div></div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""<div class='metric-card'>
          <div class='metric-val'>{r.column_count}</div>
          <div class='metric-lbl'>Columns</div></div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class='metric-card'>
          <div class='metric-val' style='color:#ef5350'>{len(r.critical_issues)}</div>
          <div class='metric-lbl'>Critical Issues</div></div>""", unsafe_allow_html=True)
    with c5:
        st.markdown(f"""<div class='metric-card'>
          <div class='metric-val' style='color:#ffa726'>{len(r.warnings)}</div>
          <div class='metric-lbl'>Warnings</div></div>""", unsafe_allow_html=True)
    with c6:
        st.markdown(f"""<div class='metric-card'>
          <div class='metric-val' style='color:#26a69a'>{len(r.passed_checks())}</div>
          <div class='metric-lbl'>Checks Passed</div></div>""", unsafe_allow_html=True)

    st.markdown("<br/>", unsafe_allow_html=True)

    # ── Charts row ────────────────────────────────────────────────
    ch1, ch2 = st.columns(2)
    with ch1:
        st.markdown("#### Dimension Scores")
        render_dimension_chart(r.dimension_scores)
    with ch2:
        st.markdown("#### Null % by Column")
        render_null_chart([c.to_dict() for c in r.checks])

    # ── Issues ────────────────────────────────────────────────────
    st.markdown("---")
    i1, i2 = st.columns(2)
    with i1:
        st.markdown(f"#### 🔴 Critical Issues ({len(r.critical_issues)})")
        if r.critical_issues:
            for issue in r.critical_issues:
                st.markdown(f"""
                <div style='background:#1a1d27;border-left:3px solid #ef5350;
                            border-radius:0 8px 8px 0;padding:10px 14px;
                            margin-bottom:8px;font-size:13px;'>{issue}</div>
                """, unsafe_allow_html=True)
        else:
            st.success("✅ No critical issues found!")

    with i2:
        st.markdown(f"#### 🟡 Warnings ({len(r.warnings)})")
        if r.warnings:
            for w in r.warnings:
                st.markdown(f"""
                <div style='background:#1a1d27;border-left:3px solid #ffa726;
                            border-radius:0 8px 8px 0;padding:10px 14px;
                            margin-bottom:8px;font-size:13px;'>{w}</div>
                """, unsafe_allow_html=True)
        else:
            st.success("✅ No warnings!")

    # ── All checks table ──────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### All Checks")

    # Build DataFrame for display
    checks_data = []
    for c in r.checks:
        checks_data.append({
            "Type":     c.check_type.value.replace("_", " ").title(),
            "Column":   c.column or "— table —",
            "Status":   "✓ Pass" if c.passed else "✗ Fail",
            "Severity": c.severity.value.upper(),
            "Score":    c.score,
            "Message":  c.message,
        })

    df = pd.DataFrame(checks_data)

    # Filter controls
    f1, f2, _ = st.columns([1, 1, 2])
    with f1:
        sev_filter = st.selectbox("Filter by Severity",
            ["All", "CRITICAL", "WARNING", "INFO", "PASS"], key="sev_filter")
    with f2:
        type_filter = st.selectbox("Filter by Type",
            ["All"] + sorted(df["Type"].unique().tolist()), key="type_filter")

    filtered = df.copy()
    if sev_filter != "All":
        filtered = filtered[filtered["Severity"] == sev_filter]
    if type_filter != "All":
        filtered = filtered[filtered["Type"] == type_filter]

    # Colour the Status column
    def colour_status(val):
        if "Pass" in val:
            return "color: #26a69a"
        return "color: #ef5350"

    def colour_severity(val):
        colors = {"CRITICAL": "color:#ef5350", "WARNING": "color:#ffa726",
                  "INFO": "color:#42a5f5", "PASS": "color:#26a69a"}
        return colors.get(val, "")

    styled = filtered.style\
        .applymap(colour_status, subset=["Status"])\
        .applymap(colour_severity, subset=["Severity"])\
        .format({"Score": "{:.1f}"})\
        .hide(axis="index")

    st.dataframe(filtered, use_container_width=True, hide_index=True,
                 column_config={
                     "Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%.0f"),
                 })

    # ── Report actions ────────────────────────────────────────────
    st.markdown("---")
    act1, act2, act3 = st.columns(3)

    with act1:
        if result.report_path and os.path.exists(result.report_path):
            with open(result.report_path, "r", encoding="utf-8") as f:
                html_content = f.read()
            st.download_button(
                "⬇️ Download HTML Report",
                data=html_content,
                file_name=os.path.basename(result.report_path),
                mime="text/html",
                use_container_width=True,
            )
        elif not skip_report:
            if st.button("📄 Generate Report", use_container_width=True):
                with st.spinner("Generating report..."):
                    from agents.report_agent import run_report_agent
                    res = run_report_agent(r, return_html=True)
                    if res["success"]:
                        st.session_state.last_profile.report_path = res["file_path"]
                        st.success(f"Report saved: {os.path.basename(res['file_path'])}")
                        st.rerun()

    with act2:
        if st.button("💬 Ask a Question About This Table →", use_container_width=True):
            st.session_state.page = "💬 NL Query"
            st.rerun()

    with act3:
        st.markdown(f"*Profiled in {r.profiling_time_ms:.0f}ms*")