"""
Reports Page — Browse, view and download all generated HTML reports.
"""

import os, sys, glob
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import streamlit as st
from datetime import datetime

from connectors import get_connector, ConnectionConfig
from orchestrator import run_dq_pipeline


REPORTS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../reports")
)


def get_reports():
    """Return sorted list of report files with metadata."""
    os.makedirs(REPORTS_DIR, exist_ok=True)
    files = glob.glob(os.path.join(REPORTS_DIR, "*.html"))
    reports = []
    for f in sorted(files, key=os.path.getmtime, reverse=True):
        name = os.path.basename(f)
        size_kb = round(os.path.getsize(f) / 1024, 1)
        mtime = datetime.fromtimestamp(os.path.getmtime(f))
        # Parse table name from filename: dq_report_TABLENAME_TIMESTAMP.html
        parts = name.replace(".html", "").split("_")
        table = parts[2] if len(parts) >= 3 else "unknown"
        reports.append({
            "filename": name,
            "path": f,
            "table": table,
            "size_kb": size_kb,
            "created": mtime.strftime("%Y-%m-%d %H:%M"),
        })
    return reports


def render_reports():
    st.markdown("# 📊 Reports")
    st.markdown("Browse and download all generated DQ reports.")
    st.markdown("---")

    source_cfg = st.session_state.get("source_config", {})

    # ── Generate new report ───────────────────────────────────────
    with st.expander("➕ Generate a New Report", expanded=False):
        if not source_cfg or not source_cfg.get("name"):
            st.info("Configure a data source in the sidebar first.")
        else:
            try:
                config    = ConnectionConfig(**source_cfg)
                connector = get_connector(config)
                connector.connect()
                tables    = connector.list_tables()
                connector.disconnect()

                col1, col2 = st.columns([2, 1])
                with col1:
                    table_sel = st.selectbox("Select Table", tables, key="report_table_sel")
                with col2:
                    st.markdown("<br/>", unsafe_allow_html=True)
                    gen_btn = st.button("📄 Generate Report", type="primary",
                                        use_container_width=True)

                if gen_btn:
                    with st.spinner(f"Profiling `{table_sel}` and generating report..."):
                        result = run_dq_pipeline(
                            source_config=source_cfg,
                            table_name=table_sel,
                            skip_nl_sql=True,
                        )
                    if result.success and result.report_path:
                        st.success(f"✅ Report generated: `{os.path.basename(result.report_path)}`")
                        st.info(f"Score: **{result.overall_score}/100** — {result.score_label}")
                        st.rerun()
                    else:
                        st.error(f"Failed: {result.error}")
            except Exception as e:
                st.error(f"Error: {e}")

    # ── Report list ───────────────────────────────────────────────
    reports = get_reports()

    if not reports:
        st.info("""No reports generated yet.
        Go to **🔍 Profile Table** and run an analysis — reports are saved automatically.""")
        return

    st.markdown(f"### {len(reports)} Report(s) Found")

    # ── Summary stats ─────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Reports", len(reports))
    with col2:
        total_size = sum(r["size_kb"] for r in reports)
        st.metric("Total Size", f"{total_size:.0f} KB")
    with col3:
        latest = reports[0]["created"] if reports else "—"
        st.metric("Latest Report", latest)

    st.markdown("---")

    # ── Report cards ──────────────────────────────────────────────
    for report in reports:
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 1, 1, 1])

            with col1:
                st.markdown(f"""
                <div style='padding:4px 0;'>
                  <span style='font-weight:600;font-size:15px;'>🗃️ {report['table'].title()}</span>
                  <br/>
                  <span style='font-size:12px;color:#7b80a0;font-family:monospace;'>
                    {report['filename']}
                  </span>
                </div>
                """, unsafe_allow_html=True)

            with col2:
                st.markdown(f"""
                <div style='text-align:center;padding-top:6px;'>
                  <div style='font-size:12px;color:#7b80a0;'>Size</div>
                  <div style='font-weight:600;'>{report['size_kb']} KB</div>
                </div>
                """, unsafe_allow_html=True)

            with col3:
                st.markdown(f"""
                <div style='text-align:center;padding-top:6px;'>
                  <div style='font-size:12px;color:#7b80a0;'>Created</div>
                  <div style='font-size:12px;font-weight:600;'>{report['created']}</div>
                </div>
                """, unsafe_allow_html=True)

            with col4:
                # Download button
                with open(report["path"], "r", encoding="utf-8") as f:
                    html_content = f.read()
                st.download_button(
                    "⬇️ Download",
                    data=html_content,
                    file_name=report["filename"],
                    mime="text/html",
                    key=f"dl_{report['filename']}",
                    use_container_width=True,
                )

            # Inline preview toggle
            if st.toggle(f"👁 Preview", key=f"preview_{report['filename']}"):
                st.components.v1.html(html_content, height=600, scrolling=True)

            st.markdown("<hr style='border-color:#2e3248;margin:8px 0;'>",
                        unsafe_allow_html=True)

    # ── Clear all button ──────────────────────────────────────────
    st.markdown("")
    with st.expander("⚠️ Danger Zone"):
        if st.button("🗑 Delete All Reports", type="secondary"):
            for r in reports:
                os.remove(r["path"])
            st.success("All reports deleted.")
            st.rerun()