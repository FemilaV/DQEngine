"""
SQLite Connector — Zero-setup local database connector.
Perfect for development, testing, and demo datasets.
"""

import sqlite3
import time
import logging
import pandas as pd
from typing import Any, Dict, List, Optional

from .base_connector import BaseConnector, ConnectionConfig, QueryResult, TableProfile

logger = logging.getLogger(__name__)


class SQLiteConnector(BaseConnector):
    """
    Connects to a local SQLite database file.
    
    Usage:
        config = ConnectionConfig(source_type="sqlite", name="Sales DB", db_path="./sales.db")
        with SQLiteConnector(config) as conn:
            tables = conn.list_tables()
            profile = conn.get_full_profile("orders")
    """

    def __init__(self, config: ConnectionConfig):
        super().__init__(config)
        self._connection: Optional[sqlite3.Connection] = None

        if not config.db_path:
            raise ValueError("SQLiteConnector requires 'db_path' in ConnectionConfig.")

    # ──────────────────────────────────────────────
    # Connection Management
    # ──────────────────────────────────────────────

    def connect(self) -> bool:
        try:
            self._connection = sqlite3.connect(
                self.config.db_path,
                check_same_thread=False,        # Allow multi-threaded access (FastAPI)
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            )
            self._connection.row_factory = sqlite3.Row  # Dict-like row access
            self._connected = True
            logger.info(f"[SQLite] Connected to '{self.config.db_path}'")
            return True
        except Exception as e:
            logger.error(f"[SQLite] Connection failed: {e}")
            self._connected = False
            return False

    def disconnect(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None
            self._connected = False
            logger.info(f"[SQLite] Disconnected from '{self.config.db_path}'")

    def test_connection(self) -> Dict[str, Any]:
        start = time.time()
        try:
            if not self._connected:
                self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            latency = round((time.time() - start) * 1000, 2)
            return {"success": True, "message": "SQLite connection healthy.", "latency_ms": latency}
        except Exception as e:
            return {"success": False, "message": str(e), "latency_ms": None}

    # ──────────────────────────────────────────────
    # Schema Exploration
    # ──────────────────────────────────────────────

    def list_tables(self) -> List[str]:
        try:
            result = self.execute_query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            if result.success and result.data is not None:
                return result.data["name"].tolist()
            return []
        except Exception as e:
            logger.error(f"[SQLite] list_tables failed: {e}")
            return []

    def get_table_profile(self, table_name: str) -> TableProfile:
        # Row count
        count_result = self.execute_query(f"SELECT COUNT(*) as cnt FROM \"{table_name}\"")
        row_count = int(count_result.data["cnt"].iloc[0]) if count_result.success else 0

        # Column info via PRAGMA
        pragma_result = self.execute_query(f"PRAGMA table_info(\"{table_name}\")")
        columns = []
        if pragma_result.success and pragma_result.data is not None:
            for _, row in pragma_result.data.iterrows():
                columns.append({
                    "name": row["name"],
                    "type": row["type"] if row["type"] else "UNKNOWN",
                    "nullable": str(row["notnull"] == 0),
                    "primary_key": str(row["pk"] > 0),
                })

        # Sample data
        sample_result = self.execute_query(f"SELECT * FROM \"{table_name}\" LIMIT 5")
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
            if not self._connected or self._connection is None:
                self.connect()

            df = pd.read_sql_query(sql, self._connection, params=params)
            elapsed = round((time.time() - start) * 1000, 2)

            return QueryResult(
                success=True,
                data=df,
                row_count=len(df),
                columns=list(df.columns),
                execution_time_ms=elapsed,
            )
        except Exception as e:
            logger.error(f"[SQLite] Query failed: {e}\nSQL: {sql}")
            return QueryResult(
                success=False,
                data=None,
                row_count=0,
                columns=[],
                error=str(e),
                execution_time_ms=round((time.time() - start) * 1000, 2),
            )

    # ──────────────────────────────────────────────
    # Column Statistics (used by Profiling Agent)
    # ──────────────────────────────────────────────

    def get_column_stats(self, table_name: str, column_name: str) -> Dict[str, Any]:
        col = f'"{column_name}"'
        tbl = f'"{table_name}"'

        # Null stats
        null_result = self.execute_query(
            f"SELECT COUNT(*) as total, SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as nulls FROM {tbl}"
        )
        total = int(null_result.data["total"].iloc[0]) if null_result.success else 0
        nulls = int(null_result.data["nulls"].iloc[0]) if null_result.success else 0
        null_pct = round((nulls / total * 100), 2) if total > 0 else 0.0

        # Distinct count
        distinct_result = self.execute_query(f"SELECT COUNT(DISTINCT {col}) as distinct_count FROM {tbl}")
        distinct_count = int(distinct_result.data["distinct_count"].iloc[0]) if distinct_result.success else 0

        # Numeric stats (min, max, avg) — gracefully skip for non-numeric
        numeric_stats = {}
        try:
            num_result = self.execute_query(
                f"SELECT MIN(CAST({col} AS REAL)) as min_val, "
                f"MAX(CAST({col} AS REAL)) as max_val, "
                f"AVG(CAST({col} AS REAL)) as mean_val "
                f"FROM {tbl} WHERE {col} IS NOT NULL"
            )
            if num_result.success and num_result.data is not None:
                row = num_result.data.iloc[0]
                numeric_stats = {
                    "min": round(float(row["min_val"]), 4) if row["min_val"] is not None else None,
                    "max": round(float(row["max_val"]), 4) if row["max_val"] is not None else None,
                    "mean": round(float(row["mean_val"]), 4) if row["mean_val"] is not None else None,
                }
        except Exception:
            pass  # Non-numeric column, skip silently

        return {
            "column": column_name,
            "total_rows": total,
            "null_count": nulls,
            "null_pct": null_pct,
            "distinct_count": distinct_count,
            "uniqueness_pct": round((distinct_count / total * 100), 2) if total > 0 else 0.0,
            **numeric_stats,
        }