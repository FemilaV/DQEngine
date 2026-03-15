# 🔍 Data Quality Engine

> An automated multi-agent system that evaluates, detects, and reports on data quality issues across multiple data sources — powered by LangGraph, LLMs, and a modern Streamlit UI.

![Python](https://img.shields.io/badge/Python-3.11-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110-green)
![Streamlit](https://img.shields.io/badge/Streamlit-1.35-red)
![LangGraph](https://img.shields.io/badge/LangGraph-0.1-purple)
![Docker](https://img.shields.io/badge/Docker-ready-blue)
![License](https://img.shields.io/badge/License-MIT-yellow)

---

## 📌 Overview

The **Data Quality Engine** is a multi-agent system that automatically:

- Profiles any table across **SQLite, PostgreSQL, and CSV** sources
- Runs **8 data quality checks** (completeness, uniqueness, validity, timeliness, accuracy, consistency, schema, empty strings)
- Generates a **DQ Score (0–100)** with per-dimension breakdown
- Translates **natural language questions into SQL** using GPT-4o-mini / Groq / Mock
- Produces **beautiful HTML reports** with Plotly charts
- Exposes a **REST API** for programmatic access
- Ships with a **full Streamlit UI** for non-technical users

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Streamlit UI  :8501                  │
│      Home | Profile Table | NL Query | Reports          │
└──────────────────────┬──────────────────────────────────┘
                       │ HTTP
┌──────────────────────▼──────────────────────────────────┐
│                  FastAPI Backend  :8000                 │
│   /pipeline  /profile  /query  /report  /sources/tables │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│              LangGraph Orchestrator                     │
│  validate → profile → nl_sql → report → summarise       │
└──────┬───────────────┬───────────────┬──────────────────┘
       │               │               │
┌──────▼──────┐ ┌──────▼──────┐ ┌─────▼──────────┐
│  Profiling  │ │  NL→SQL     │ │  Report        │
│  Agent      │ │  Agent      │ │  Agent         │
│  8 DQ checks│ │  GPT/Groq   │ │  Jinja2+Plotly │
└──────┬──────┘ └─────────────┘ └────────────────┘
       │
┌──────▼──────────────────────────────────────────────────┐
│                   Connector Layer                       │
│          SQLite  │  PostgreSQL  │  CSV                  │
└─────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Option 1 — Docker (Recommended)

```bash
# 1. Clone the repo
git clone https://github.com/yourusername/DQEngine.git
cd DQEngine

# 2. Add your API key (optional — works without it using mock provider)
cp .env.example .env
# Edit .env and add: OPENAI_API_KEY=sk-... or GROQ_API_KEY=gsk_...

# 3. Start everything
docker compose up --build

# 4. Open browser
# Streamlit UI  → http://localhost:8501
# FastAPI Docs  → http://localhost:8000/docs
```

### Option 2 — Local (Manual)

```bash
# 1. Clone and create virtual environment
git clone https://github.com/yourusername/DQEngine.git
cd DQEngine
python -m venv .venv

# 2. Activate virtual environment
# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Set up environment
cp .env.example .env
# Edit .env with your API keys

# 5. Generate sample data
python sample_data/generate_samples.py

# 6. Terminal 1 — Start FastAPI
uvicorn api.main:app --reload --port 8000

# 7. Terminal 2 — Start Streamlit
streamlit run frontend/app.py
```

---

## 🔑 Environment Variables

Create a `.env` file in the project root:

```env
# Option 1: OpenAI (paid, most accurate)
OPENAI_API_KEY=sk-your-key-here

# Option 2: Groq (FREE — get key at console.groq.com)
GROQ_API_KEY=gsk_your-key-here

# PostgreSQL (optional)
PG_HOST=localhost
PG_PORT=5432
PG_DB=postgres
PG_USER=postgres
PG_PASS=
```

> **Note:** If no API key is set, the system uses a rule-based mock provider. Set a Groq key for free full LLM capability.

---

## 📁 Project Structure

```
DQEngine/
├── connectors/                # Data source connectors
│   ├── base_connector.py      # Abstract base class
│   ├── sqlite_connector.py    # SQLite
│   ├── postgres_connector.py  # PostgreSQL
│   ├── csv_connector.py       # CSV files
│   └── factory.py             # Connector factory
│
├── models/                    # Pydantic data models
│   ├── profiling_models.py    # DQ check models
│   └── nl_sql_models.py       # NL→SQL models
│
├── agents/                    # LLM-powered agents
│   ├── dq_checks.py           # 8 DQ check implementations
│   ├── profiling_agent.py     # Profiling LangGraph pipeline
│   ├── nl_sql_agent.py        # NL→SQL LangGraph pipeline
│   ├── report_agent.py        # HTML report generator
│   └── llm_provider.py        # OpenAI / Groq / Mock fallback
│
├── orchestrator/              # Master pipeline
│   ├── orchestrator.py        # LangGraph orchestrator
│   └── orchestrator_state.py  # Pipeline state model
│
├── api/                       # FastAPI backend
│   ├── main.py                # App entry point
│   ├── schemas.py             # Request/response schemas
│   └── routes/                # Route handlers
│       ├── pipeline.py        # POST /pipeline
│       ├── profile.py         # POST /profile
│       ├── query.py           # POST /query
│       ├── report.py          # POST /report
│       └── sources.py         # POST /sources/tables
│
├── frontend/                  # Streamlit UI
│   ├── app.py                 # Main app + sidebar
│   └── ui_pages/
│       ├── home.py            # Home dashboard
│       ├── profile.py         # Profile table page
│       ├── nlquery.py         # NL Query page
│       └── reports.py         # Reports browser
│
├── templates/
│   └── report_template.html   # Jinja2 HTML report template
│
├── sample_data/               # Sample datasets
│   ├── generate_samples.py    # Generate test data
│   ├── retail.db              # SQLite with DQ issues
│   ├── customers.csv          # CSV with nulls + dupes
│   └── transactions.csv       # CSV with outliers
│
├── reports/                   # Generated HTML reports
├── tests/                     # Test suite
├── docker/
│   └── supervisord.conf       # Process supervisor config
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── requirements.txt
```

---

## 🔍 The 8 DQ Checks

| # | Check | What it detects |
|---|-------|-----------------|
| 1 | **Completeness** | Null / missing values per column |
| 2 | **Uniqueness** | Duplicate rows, non-unique ID columns |
| 3 | **Validity** | Negative values, out-of-range numbers, outliers |
| 4 | **Accuracy** | Statistical outliers (beyond 4σ) |
| 5 | **Timeliness** | Stale dates (>1 year old), future dates |
| 6 | **Consistency** | Case inconsistency, date ordering violations |
| 7 | **Schema** | Dead columns (100% null), dates stored as TEXT |
| 8 | **Empty Strings** | Blank strings that aren't NULL (CSV plague) |

---

## 🌐 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check |
| `POST` | `/pipeline` | Full pipeline — profile + NL→SQL + report |
| `POST` | `/profile` | Run DQ checks on a table |
| `POST` | `/query` | Natural language → SQL |
| `POST` | `/report` | Generate HTML report |
| `POST` | `/sources/tables` | List available tables |
| `GET` | `/report/list` | List generated reports |
| `GET` | `/report/download/{file}` | Download a report |

**Interactive docs:** http://localhost:8000/docs

### Example — Full Pipeline

```bash
curl -X POST http://localhost:8000/pipeline \
  -H "Content-Type: application/json" \
  -d '{
    "source_config": {
      "source_type": "sqlite",
      "name": "Retail DB",
      "db_path": "./sample_data/retail.db"
    },
    "table_name": "orders",
    "nl_question": "show me orders with missing customer id"
  }'
```

---

## 🖥️ UI Pages

| Page | Description |
|------|-------------|
| 🏠 **Home** | Source preview, table cards, recent history |
| 🔍 **Profile Table** | Run DQ checks, view score, charts, issues, download report |
| 💬 **NL Query** | Ask questions in plain English, view SQL + results |
| 📊 **Reports** | Browse, preview and download all generated reports |

---

## 🧪 Running Tests

```bash
# Test connectors
python tests/test_connectors.py

# Test profiling agent
python tests/test_profiling_agent.py

# Test NL→SQL agent
python tests/test_nl_sql_agent.py

# Test report agent
python tests/test_report_agent.py

# Test orchestrator (all agents together)
python tests/test_orchestrator.py

# Test API endpoints
python tests/test_api.py
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | LangGraph |
| LLM | OpenAI GPT-4o-mini / Groq Llama-3 / Mock |
| Backend | FastAPI + Uvicorn |
| Frontend | Streamlit |
| Data Processing | Pandas + NumPy |
| Database | SQLite / PostgreSQL / CSV |
| ORM | SQLAlchemy |
| Reporting | Jinja2 + Plotly |
| Containerization | Docker + Docker Compose |
| Process Management | Supervisord |

---

## 📊 DQ Score Grades

| Score | Grade | Meaning |
|-------|-------|---------|
| 90–100 | 🟢 Excellent | Production ready |
| 75–89 | 🟡 Good | Minor issues to fix |
| 60–74 | 🟠 Fair | Significant issues |
| 40–59 | 🔴 Poor | Major remediation needed |
| 0–39 | ⛔ Critical | Not fit for use |

---

## 🐳 Docker Commands

```bash
# Start (first time — builds image)
docker compose up --build

# Start (subsequent runs)
docker compose up

# Start in background
docker compose up -d

# Stop
docker compose down

# View logs
docker compose logs -f

# Rebuild after code changes
docker compose up --build
```

---

## 📝 Notes

- **No API key required** — mock provider works out of the box
- **Free LLM option** — get a free Groq key at console.groq.com
- **Auto-fallback** — if OpenAI quota runs out, automatically switches to Groq

---

## 👤 Author

**Femila** — Data Quality Engine  
Built with ❤️ using LangGraph, FastAPI, and Streamlit