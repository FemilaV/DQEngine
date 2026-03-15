"""
CSV Connector — File-based data source connector.
Loads CSV files into an in-memory SQLite database so you can run
real SQL against them — same interface as all other connectors.
"""

import os
import re
import time
import sqlite3
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .base_connector import BaseConnector, ConnectionConfig, QueryResult, TableProfile

logger = logging.getLogger(__name__)


class CSVConnector(BaseConnector):
    """
    Treats one or more CSV files as a queryable SQL database.
    
    How it works:
    - Loads each CSV into an in-memory SQLite database
    - Table name = sanitized filename (e.g. "customers.csv" → table "customers")
    - Supports full SQL queries against the loaded data
    
    Supports two modes:
    1. Single file: config.file_path = "./data/customers.csv"
    2. Directory:   config.file_path = "./data/"  (loads all .csv files)
    
    Usage:
        config = ConnectionConfig(
            source_type="csv",
            name="Customer Data",
            file_path="./sample_data/customers.csv"
        )
        with CSVConnector(config) as conn:
            tables = conn.list_tables()         # ["customers"]
            profile = conn.get_full_profile("customers")
    """

    def __init__(self, config: ConnectionConfig):
        super().__init__(config)
        self._db: Optional[sqlite3.Connection] = None
        self._loaded_tables: Dict[str, str] = {}    # {table_name: file_path}

        if not config.file_path:
            raise ValueError("CSVConnector requires 'file_path' in ConnectionConfig.")

    # ──────────────────────────────────────────────
    # Connection Management
    # ──────────────────────────────────────────────

    def connect(self) -> bool:
        try:
            # In-memory SQLite to hold all CSV data
            self._db = sqlite3.connect(":memory:", check_same_thread=False)

            path = Path(self.config.file_path)
            files_to_load = []

            if path.is_dir():
                files_to_load = list(path.glob("*.csv"))
                if not files_to_load:
                    logger.warning(f"[CSV] No .csv files found in directory '{path}'")
            elif path.is_file() and path.suffix.lower() == ".csv":
                files_to_load = [path]
            else:
                raise FileNotFoundError(f"Path '{path}' is neither a CSV file nor a directory.")

            for csv_file in files_to_load:
                table_name = self._sanitize_table_name(csv_file.stem)
                self._load_csv_to_table(csv_file, table_name)

            self._connected = True
            logger.info(f"[CSV] Loaded {len(self._loaded_tables)} table(s): {list(self._loaded_tables.keys())}")
            return True

        except Exception as e:
            logger.error(f"[CSV] Connection failed: {e}")
            self._connected = False
            return False

    def disconnect(self) -> None:
        if self._db:
            self._db.close()
            self._db = None
            self._loaded_tables.clear()
            self._connected = False
            logger.info("[CSV] Disconnected (in-memory DB released).")

    def test_connection(self) -> Dict[str, Any]:
        start = time.time()
        try:
            if not self._connected:
                self.connect()
            cursor = self._db.cursor()
            cursor.execute("SELECT 1")
            latency = round((time.time() - start) * 1000, 2)
            loaded = list(self._loaded_tables.keys())
            return {
                "success": True,
                "message": f"CSV source healthy. Tables loaded: {loaded}",
                "latency_ms": latency,
            }
        except Exception as e:
            return {"success": False, "message": str(e), "latency_ms": None}

    # ──────────────────────────────────────────────
    # Schema Exploration
    # ──────────────────────────────────────────────

    def list_tables(self) -> List[str]:
        return list(self._loaded_tables.keys())

    def get_table_profile(self, table_name: str) -> TableProfile:
        if table_name not in self._loaded_tables:
            raise ValueError(f"Table '{table_name}' not found. Available: {list(self._loaded_tables.keys())}")

        # Row count
        count_result = self.execute_query(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
        row_count = int(count_result.data["cnt"].iloc[0]) if count_result.success else 0

        # Column info via PRAGMA
        pragma_result = self.execute_query(f'PRAGMA table_info("{table_name}")')
        columns = []
        if pragma_result.success and pragma_result.data is not None:
            for _, row in pragma_result.data.iterrows():
                columns.append({
                    "name": row["name"],
                    "type": row["type"] if row["type"] else "TEXT",
                    "nullable": "true",
                    "primary_key": str(row["pk"] > 0),
                })

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
            if not self._connected or self._db is None:
                self.connect()

            df = pd.read_sql_query(sql, self._db, params=params)
            elapsed = round((time.time() - start) * 1000, 2)

            return QueryResult(
                success=True,
                data=df,
                row_count=len(df),
                columns=list(df.columns),
                execution_time_ms=elapsed,
            )
        except Exception as e:
            logger.error(f"[CSV] Query failed: {e}\nSQL: {sql}")
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

        null_result = self.execute_query(
            f"SELECT COUNT(*) as total, "
            f"SUM(CASE WHEN {col} IS NULL OR TRIM(CAST({col} AS TEXT)) = '' THEN 1 ELSE 0 END) as nulls "
            f"FROM {tbl}"
        )
        # Note: CSV nulls include empty strings — treated as missing values above
        total = int(null_result.data["total"].iloc[0]) if null_result.success else 0
        nulls = int(null_result.data["nulls"].iloc[0]) if null_result.success else 0
        null_pct = round((nulls / total * 100), 2) if total > 0 else 0.0

        distinct_result = self.execute_query(
            f"SELECT COUNT(DISTINCT {col}) as distinct_count FROM {tbl}"
        )
        distinct_count = int(distinct_result.data["distinct_count"].iloc[0]) if distinct_result.success else 0

        numeric_stats = {}
        try:
            num_result = self.execute_query(
                f"SELECT MIN(CAST({col} AS REAL)) as min_val, "
                f"MAX(CAST({col} AS REAL)) as max_val, "
                f"AVG(CAST({col} AS REAL)) as mean_val "
                f"FROM {tbl} WHERE {col} IS NOT NULL AND TRIM(CAST({col} AS TEXT)) != ''"
            )
            if num_result.success and num_result.data is not None:
                row = num_result.data.iloc[0]
                numeric_stats = {
                    "min": float(row["min_val"]) if row["min_val"] is not None else None,
                    "max": float(row["max_val"]) if row["max_val"] is not None else None,
                    "mean": round(float(row["mean_val"]), 4) if row["mean_val"] is not None else None,
                }
        except Exception:
            pass

        return {
            "column": column_name,
            "total_rows": total,
            "null_count": nulls,
            "null_pct": null_pct,
            "distinct_count": distinct_count,
            "uniqueness_pct": round((distinct_count / total * 100), 2) if total > 0 else 0.0,
            **numeric_stats,
        }

    # ──────────────────────────────────────────────
    # Internal Helpers
    # ──────────────────────────────────────────────

    def _load_csv_to_table(self, file_path: Path, table_name: str) -> None:
        """Load a single CSV file into the in-memory SQLite database."""
        # Try to detect encoding
        encodings = ["utf-8", "latin-1", "utf-8-sig"]
        df = None
        for enc in encodings:
            try:
                df = pd.read_csv(file_path, encoding=enc, low_memory=False)
                break
            except UnicodeDecodeError:
                continue

        if df is None:
            raise ValueError(f"Could not read '{file_path}' with any known encoding.")

        # Clean column names (remove spaces, special chars)
        df.columns = [self._sanitize_table_name(c) for c in df.columns]

        df.to_sql(table_name, self._db, if_exists="replace", index=False)
        self._loaded_tables[table_name] = str(file_path)
        logger.info(f"[CSV] Loaded '{file_path.name}' → table '{table_name}' ({len(df)} rows, {len(df.columns)} cols)")

    @staticmethod
    def _sanitize_table_name(name: str) -> str:
        """Convert any string to a valid SQL table/column name."""
        name = re.sub(r"[^\w]", "_", name.strip())
        if name and name[0].isdigit():
            name = f"col_{name}"
        return name.lower()