"""
DQ Engine — FastAPI Backend
Run: uvicorn api.main:app --reload --port 8000

Swagger UI:  http://localhost:8000/docs
ReDoc:       http://localhost:8000/redoc
"""

import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from api.routes import sources, profile, query, report, pipeline
from api.schemas import HealthResponse
from connectors import list_supported_sources
from agents.llm_provider import get_active_provider

# ── Logging ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dq_engine")


# ── Startup / Shutdown ───────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  DQ Engine API starting...")
    logger.info(f"  LLM Provider : {get_active_provider()}")
    logger.info(f"  Sources      : {list_supported_sources()}")
    logger.info("  Docs         : http://localhost:8000/docs")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # Ensure reports directory exists
    os.makedirs("reports", exist_ok=True)

    yield   # App runs here

    logger.info("DQ Engine API shutting down.")


# ── App ──────────────────────────────────────────────────────────
app = FastAPI(
    title="Data Quality Engine",
    description="""
## Data Quality Engine API

Automated multi-agent system for data quality analysis.

### Endpoints
| Endpoint | Description |
|---|---|
| `POST /pipeline` | **Main endpoint** — full pipeline (profile + NL→SQL + report) |
| `POST /profile` | Profile a table — 8 DQ checks + scoring |
| `POST /query` | Natural language → SQL |
| `POST /report` | Generate HTML report |
| `POST /sources/tables` | List tables in a data source |

### Quick Start
1. Call `POST /sources/tables` to list available tables
2. Call `POST /pipeline` with your table and optional question
3. Get back DQ score, check results, SQL data, and report path
    """,
    version="1.0.0",
    lifespan=lifespan,
)

# ── CORS — allow Streamlit frontend to call the API ──────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # In production, restrict to your Streamlit URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ──────────────────────────────────────────────────────
app.include_router(sources.router)
app.include_router(profile.router)
app.include_router(query.router)
app.include_router(report.router)
app.include_router(pipeline.router)


# ── Health check ─────────────────────────────────────────────────
@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Quick health check — confirms API is running and shows config."""
    return HealthResponse(
        status="ok",
        version="1.0.0",
        llm_provider=get_active_provider(),
        supported_sources=list_supported_sources(),
    )


@app.get("/", tags=["Health"])
async def root():
    return {
        "message": "Data Quality Engine API",
        "docs": "/docs",
        "health": "/health",
    }


# ── Global error handler ─────────────────────────────────────────
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Unhandled error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"success": False, "error": str(exc)},
    )