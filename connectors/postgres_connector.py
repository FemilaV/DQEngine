"""
PostgreSQL Connector — Production-grade relational database connector.
Uses SQLAlchemy engine under the hood for connection pooling.
"""

import time
import logging
from typing import Any, Dict, List, Optional

import pandas as pd
try:
    from sqlalchemy import create_engine, text, inspect
    from sqlalchemy.pool import QueuePool
    from sqlalchemy.exc import SQLAlchemyError
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

from .base_connector import BaseConnector, ConnectionConfig, QueryResult, TableProfile

logger = logging.getLogger(__name__)


class PostgreSQLConnector(BaseConnector):
    """
    Connects to a PostgreSQL database using SQLAlchemy.
    Supports connection pooling for multi-threaded FastAPI usage.
    
    Usage:
        config = ConnectionConfig(
            source_type="postgres",
            name="Analytics DB",
            host="localhost", port=5432,
            database="analytics", username="user", password="pass"
        )
        with PostgreSQLConnector(config) as conn:
            tables = conn.list_tables()
    """

    def __init__(self, config: ConnectionConfig):
        super().__init__(config)
        self._engine = None

        required = ["host", "database", "username", "password"]
        missing = [f for f in required if not getattr(config, f, None)]
        if missing:
            raise ValueError(f"PostgreSQLConnector missing required fields: {missing}")

    # ──────────────────────────────────────────────
    # Connection Management
    # ──────────────────────────────────────────────

    def _build_connection_url(self) -> str:
        c = self.config
        return (
            f"postgresql+psycopg2://{c.username}:{c.password}"
            f"@{c.host}:{c.port}/{c.database}"
        )

    def connect(self) -> bool:
        if not SQLALCHEMY_AVAILABLE:
            logger.error("[PostgreSQL] sqlalchemy is not installed. Run: pip install sqlalchemy psycopg2-binary")
            return False
        try:
            url = self._build_connection_url()
            self._engine = create_engine(
                url,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_pre_ping=True,     # Auto-reconnect on stale connections
                echo=False,
            )
            # Verify connection works
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self._connected = True
            logger.info(f"[PostgreSQL] Connected to '{self.config.database}' at {self.config.host}")
            return True
        except Exception as e:
            logger.error(f"[PostgreSQL] Connection failed: {e}")
            self._connected = False
            return False

    def disconnect(self) -> None:
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._connected = False
            logger.info(f"[PostgreSQL] Disconnected from '{self.config.database}'")

    def test_connection(self) -> Dict[str, Any]:
        start = time.time()
        try:
            if not self._connected:
                self.connect()
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            latency = round((time.time() - start) * 1000, 2)
            return {"success": True, "message": "PostgreSQL connection healthy.", "latency_ms": latency}
        except Exception as e:
            return {"success": False, "message": str(e), "latency_ms": None}

    # ──────────────────────────────────────────────
    # Schema Exploration
    # ──────────────────────────────────────────────

    def list_tables(self) -> List[str]:
        try:
            inspector = inspect(self._engine)
            # Only public schema tables (skip system tables)
            return inspector.get_table_names(schema="public")
        except Exception as e:
            logger.error(f"[PostgreSQL] list_tables failed: {e}")
            return []

    def get_table_profile(self, table_name: str) -> TableProfile:
        # Row count
        count_result = self.execute_query(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
        row_count = int(count_result.data["cnt"].iloc[0]) if count_result.success else 0

        # Column metadata via SQLAlchemy inspector
        columns = []
        try:
            inspector = inspect(self._engine)
            for col in inspector.get_columns(table_name, schema="public"):
                columns.append({
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": str(col.get("nullable", True)),
                    "primary_key": "false",  # Checked separately if needed
                })
        except Exception as e:
            logger.warning(f"[PostgreSQL] Could not inspect columns for '{table_name}': {e}")

        # Sample data
        sample_result = self.execute_query(f'SELECT * FROM "{table_name}" LIMIT 5')
        sample_df = sample_result.data if sample_result.success else pd.DataFrame()

        return TableProfile(
            table_name=table_name,
            row_count=row_count,
            column_count=len(columns),
            columns=columns,
            sample_data=sample_df,
            source_name=self.name,
        )

    # ──────────────────────────────────────────────
    # Query Execution
    # ──────────────────────────────────────────────

    def execute_query(self, sql: str, params: Optional[Dict] = None) -> QueryResult:
        start = time.time()
        try:
            if not self._connected or self._engine is None:
                self.connect()

            with self._engine.connect() as conn:
                df = pd.read_sql_query(text(sql), conn, params=params)

            elapsed = round((time.time() - start) * 1000, 2)
            return QueryResult(
                success=True,
                data=df,
                row_count=len(df),
                columns=list(df.columns),
                execution_time_ms=elapsed,
            )
        except SQLAlchemyError as e:
            logger.error(f"[PostgreSQL] Query failed: {e}\nSQL: {sql}")
            return QueryResult(
                success=False,
                data=None,
                row_count=0,
                columns=[],
                error=str(e),
                execution_time_ms=round((time.time() - start) * 1000, 2),
            )

    # ──────────────────────────────────────────────
    # Column Statistics
    # ──────────────────────────────────────────────

    def get_column_stats(self, table_name: str, column_name: str) -> Dict[str, Any]:
        col = f'"{column_name}"'
        tbl = f'"{table_name}"'

        # Null stats
        null_result = self.execute_query(
            f"SELECT COUNT(*) as total, "
            f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as nulls "
            f"FROM {tbl}"
        )
        total = int(null_result.data["total"].iloc[0]) if null_result.success else 0
        nulls = int(null_result.data["nulls"].iloc[0]) if null_result.success else 0
        null_pct = round((nulls / total * 100), 2) if total > 0 else 0.0

        # Distinct count
        distinct_result = self.execute_query(
            f"SELECT COUNT(DISTINCT {col}) as distinct_count FROM {tbl}"
        )
        distinct_count = int(distinct_result.data["distinct_count"].iloc[0]) if distinct_result.success else 0

        # Numeric stats using PostgreSQL's native aggregates
        numeric_stats = {}
        try:
            num_result = self.execute_query(
                f"SELECT "
                f"  MIN({col}::NUMERIC) as min_val, "
                f"  MAX({col}::NUMERIC) as max_val, "
                f"  AVG({col}::NUMERIC) as mean_val, "
                f"  STDDEV({col}::NUMERIC) as std_val "
                f"FROM {tbl} WHERE {col} IS NOT NULL"
            )
            if num_result.success and num_result.data is not None:
                row = num_result.data.iloc[0]
                numeric_stats = {
                    "min": float(row["min_val"]) if row["min_val"] is not None else None,
                    "max": float(row["max_val"]) if row["max_val"] is not None else None,
                    "mean": round(float(row["mean_val"]), 4) if row["mean_val"] is not None else None,
                    "std": round(float(row["std_val"]), 4) if row["std_val"] is not None else None,
                }
        except Exception:
            pass  # Non-numeric column

        return {
            "column": column_name,
            "total_rows": total,
            "null_count": nulls,
            "null_pct": null_pct,
            "distinct_count": distinct_count,
            "uniqueness_pct": round((distinct_count / total * 100), 2) if total > 0 else 0.0,
            **numeric_stats,
        }