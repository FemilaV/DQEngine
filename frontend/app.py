"""
DQ Engine — Streamlit Frontend
Run: streamlit run frontend/app.py
"""

import streamlit as st

st.set_page_config(
    page_title="DQ Engine",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  [data-testid="stSidebar"] { background: #0f1117; border-right: 1px solid #2e3248; }
  .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }
  .metric-card { background: #1a1d27; border: 1px solid #2e3248; border-radius: 10px; padding: 16px 20px; text-align: center; }
  .metric-val { font-size: 28px; font-weight: 800; }
  .metric-lbl { font-size: 12px; color: #7b80a0; margin-top: 4px; }
  .sql-block { background:#0d1117; border:1px solid #2e3248; border-radius:8px; padding:14px 18px; font-family:monospace; font-size:13px; color:#79c0ff; overflow-x:auto; white-space:pre-wrap; }
  .pill-critical { background:rgba(239,83,80,0.15); color:#ef5350; border:1px solid rgba(239,83,80,0.3); border-radius:12px; padding:2px 10px; font-size:12px; font-weight:600; }
  .pill-warning  { background:rgba(255,167,38,0.15); color:#ffa726; border:1px solid rgba(255,167,38,0.3); border-radius:12px; padding:2px 10px; font-size:12px; font-weight:600; }
  .pill-pass     { background:rgba(38,166,154,0.15); color:#26a69a; border:1px solid rgba(38,166,154,0.3); border-radius:12px; padding:2px 10px; font-size:12px; font-weight:600; }
  div[data-testid="stMetric"] label { font-size:12px !important; }
</style>
""", unsafe_allow_html=True)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from frontend.ui_pages.home    import render_home
from frontend.ui_pages.profile import render_profile
from frontend.ui_pages.nlquery import render_nlquery
from frontend.ui_pages.reports import render_reports


def init_session():
    defaults = {
        "page": "🏠 Home",
        "source_config": None,
        "last_profile": None,
        "last_query": None,
        "history": [],
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session()


def handle_csv_upload(uploaded_file) -> str:
    """Save uploaded CSV to sample_data/ and return its path."""
    save_dir = os.path.join(os.path.dirname(__file__), "../sample_data")
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, uploaded_file.name)
    with open(save_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return save_path


with st.sidebar:
    st.markdown("## 🔍 DQ Engine")
    st.markdown("<hr style='border-color:#2e3248;margin:8px 0 16px'>", unsafe_allow_html=True)

    pages = ["🏠 Home", "🔍 Profile Table", "💬 NL Query", "📊 Reports"]
    page  = st.radio("Navigation", pages, index=pages.index(st.session_state.page), label_visibility="collapsed")
    st.session_state.page = page

    st.markdown("<hr style='border-color:#2e3248;margin:16px 0'>", unsafe_allow_html=True)
    st.markdown("#### ⚙️ Data Source")

    source_type = st.selectbox("Source Type", ["sqlite", "csv", "postgres"], key="src_type")
    source_cfg  = {"source_type": source_type, "name": ""}

    if source_type == "sqlite":
        db_path = st.text_input("DB File Path", value="./sample_data/retail.db", key="db_path",
                                help="Path to .db file relative to DQEngine root")
        name    = st.text_input("Source Name", value="Retail DB", key="src_name_sqlite")
        source_cfg.update({"db_path": db_path, "name": name})

    elif source_type == "csv":
        st.markdown("**Input method:**")
        input_method = st.radio("input_method", ["📁 Enter file path", "⬆️ Upload CSV file"],
                                key="csv_input_method", label_visibility="collapsed")

        if input_method == "📁 Enter file path":
            file_path = st.text_input("CSV File / Directory Path",
                                      value="./sample_data/customers.csv", key="csv_path",
                                      help="Single .csv file or folder with multiple .csv files")
            name = st.text_input("Source Name", value="CSV Data", key="src_name_csv_path")
            source_cfg.update({"file_path": file_path, "name": name})

        else:
            uploaded = st.file_uploader("Upload CSV", type=["csv"], key="csv_uploader",
                                        label_visibility="collapsed")
            name = st.text_input("Source Name", value="Uploaded CSV", key="src_name_csv_upload")

            if uploaded is not None:
                saved_path = handle_csv_upload(uploaded)
                st.success(f"✅ Saved: `{uploaded.name}`")
                source_cfg.update({"file_path": saved_path, "name": name or uploaded.name})
                st.session_state["last_uploaded_csv"] = saved_path
            elif st.session_state.get("last_uploaded_csv"):
                prev = st.session_state["last_uploaded_csv"]
                st.info(f"Using: `{os.path.basename(prev)}`")
                source_cfg.update({"file_path": prev, "name": name or os.path.basename(prev)})
            else:
                st.info("Upload a CSV file to analyse it.")

    elif source_type == "postgres":
        col1, col2 = st.columns(2)
        with col1:
            host = st.text_input("Host", value="localhost", key="pg_host")
            db   = st.text_input("Database", value="postgres", key="pg_db")
        with col2:
            port = st.number_input("Port", value=5432, step=1, key="pg_port")
            user = st.text_input("Username", value="postgres", key="pg_user")
        pwd  = st.text_input("Password", type="password", key="pg_pwd")
        name = st.text_input("Source Name", value="PostgreSQL", key="src_name_pg")
        source_cfg.update({"host": host, "port": int(port), "database": db,
                           "username": user, "password": pwd, "name": name})

    st.session_state.source_config = source_cfg

    st.markdown("<hr style='border-color:#2e3248;margin:12px 0'>", unsafe_allow_html=True)
    from agents.llm_provider import get_active_provider
    provider = get_active_provider()
    icons  = {"openai": "🟢", "groq": "🟡", "mock": "🔴"}
    labels = {"openai": "OpenAI GPT-4o-mini", "groq": "Groq (Free)", "mock": "Mock (no key)"}
    st.markdown(f"{icons.get(provider,'⚪')} **LLM:** `{labels.get(provider, provider)}`")
    if provider == "mock":
        st.caption("Set OPENAI_API_KEY or GROQ_API_KEY in .env")


if   page == "🏠 Home":          render_home()
elif page == "🔍 Profile Table": render_profile()
elif page == "💬 NL Query":      render_nlquery()
elif page == "📊 Reports":       render_reports()