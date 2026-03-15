"""
API Schemas — Pydantic models for all FastAPI request/response bodies.
These are what the frontend sends and what the API returns.
"""

from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional


# ─────────────────────────────────────────────────────────────────
# SHARED
# ─────────────────────────────────────────────────────────────────

class SourceConfigSchema(BaseModel):
    """Connection details for a data source."""
    source_type: str = Field(..., description="sqlite | postgres | csv")
    name: str        = Field(..., description="Human readable label")

    # SQLite
    db_path: Optional[str] = Field(None, description="Path to .db file (SQLite only)")

    # PostgreSQL
    host:     Optional[str] = None
    port:     Optional[int] = 5432
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    # CSV
    file_path: Optional[str] = Field(None, description="Path to .csv file or directory")

    class Config:
        json_schema_extra = {
            "examples": [
                {"source_type": "sqlite", "name": "Retail DB", "db_path": "./sample_data/retail.db"},
                {"source_type": "csv",    "name": "Customers",  "file_path": "./sample_data/customers.csv"},
            ]
        }


# ─────────────────────────────────────────────────────────────────
# /sources/tables
# ─────────────────────────────────────────────────────────────────

class ListTablesRequest(BaseModel):
    source_config: SourceConfigSchema

class ListTablesResponse(BaseModel):
    success: bool
    tables: List[str]
    source_name: str
    error: Optional[str] = None


# ─────────────────────────────────────────────────────────────────
# /profile
# ─────────────────────────────────────────────────────────────────

class ProfileRequest(BaseModel):
    source_config: SourceConfigSchema
    table_name: str
    skip_report: bool = False

    class Config:
        json_schema_extra = {
            "example": {
                "source_config": {"source_type": "sqlite", "name": "Retail DB", "db_path": "./sample_data/retail.db"},
                "table_name": "orders",
                "skip_report": False,
            }
        }


class ColumnStatSchema(BaseModel):
    column: str
    total_rows: int
    null_count: int
    null_pct: float
    distinct_count: int
    uniqueness_pct: float
    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None


class CheckResultSchema(BaseModel):
    check_type: str
    column: Optional[str]
    passed: bool
    severity: str
    score: float
    message: str
    detail: Dict[str, Any] = {}


class ProfileResponse(BaseModel):
    success: bool
    table_name: str
    source_name: str
    source_type: str
    row_count: int
    column_count: int
    overall_dq_score: float
    score_label: str
    dimension_scores: Dict[str, float]
    critical_issues: List[str]
    warnings: List[str]
    checks: List[CheckResultSchema]
    passed_checks: int
    failed_checks: int
    total_checks: int
    report_path: Optional[str] = None
    profiling_time_ms: float
    error: Optional[str] = None


# ─────────────────────────────────────────────────────────────────
# /query  (NL→SQL)
# ─────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    source_config: SourceConfigSchema
    table_name: str
    natural_language: str
    max_rows: int = 100

    class Config:
        json_schema_extra = {
            "example": {
                "source_config": {"source_type": "sqlite", "name": "Retail DB", "db_path": "./sample_data/retail.db"},
                "table_name": "orders",
                "natural_language": "show me orders where customer_id is missing",
                "max_rows": 50,
            }
        }


class QueryResponse(BaseModel):
    success: bool
    natural_language: str
    generated_sql: Optional[str]
    explanation: Optional[str]
    row_count: int
    columns: List[str]
    data: List[Dict[str, Any]]      # Rows as list of dicts
    execution_time_ms: float
    llm_provider: str
    model_used: str
    error: Optional[str] = None


# ─────────────────────────────────────────────────────────────────
# /report
# ─────────────────────────────────────────────────────────────────

class ReportRequest(BaseModel):
    source_config: SourceConfigSchema
    table_name: str
    return_html: bool = False       # If True, include HTML string in response

    class Config:
        json_schema_extra = {
            "example": {
                "source_config": {"source_type": "sqlite", "name": "Retail DB", "db_path": "./sample_data/retail.db"},
                "table_name": "orders",
                "return_html": False,
            }
        }


class ReportResponse(BaseModel):
    success: bool
    table_name: str
    overall_dq_score: float
    score_label: str
    report_path: Optional[str]
    html: Optional[str] = None      # Only populated if return_html=True
    error: Optional[str] = None


# ─────────────────────────────────────────────────────────────────
# /pipeline  (full orchestrator — all agents in one call)
# ─────────────────────────────────────────────────────────────────

class PipelineRequest(BaseModel):
    source_config: SourceConfigSchema
    table_name: str
    nl_question: Optional[str] = None
    max_rows: int = 100
    skip_report: bool = False

    class Config:
        json_schema_extra = {
            "example": {
                "source_config": {"source_type": "sqlite", "name": "Retail DB", "db_path": "./sample_data/retail.db"},
                "table_name": "orders",
                "nl_question": "show me orders with missing customer id",
                "max_rows": 50,
                "skip_report": False,
            }
        }


class PipelineResponse(BaseModel):
    success: bool
    overall_score: float
    score_label: str
    stages_completed: List[str]
    total_time_ms: float
    report_path: Optional[str] = None
    profile: Optional[ProfileResponse] = None
    query: Optional[QueryResponse] = None
    error: Optional[str] = None


# ─────────────────────────────────────────────────────────────────
# /health
# ─────────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str
    version: str
    llm_provider: str
    supported_sources: List[str]