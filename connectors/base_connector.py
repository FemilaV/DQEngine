"""
Base Connector — Abstract interface all connectors must implement.
Every connector (SQLite, Postgres, CSV) inherits from this class.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import pandas as pd
import logging

logger = logging.getLogger(__name__)


@dataclass
class ConnectionConfig:
    """Unified config object passed to any connector."""
    source_type: str                        # "sqlite" | "postgres" | "csv"
    name: str                               # Human-readable label, e.g. "Sales DB"
    
    # SQLite
    db_path: Optional[str] = None          # e.g. "./sample_data/sales.db"
    
    # PostgreSQL
    host: Optional[str] = None
    port: Optional[int] = 5432
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    
    # CSV
    file_path: Optional[str] = None        # e.g. "./sample_data/customers.csv"
    
    # Shared options
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TableProfile:
    """Metadata snapshot of a single table/dataset."""
    table_name: str
    row_count: int
    column_count: int
    columns: List[Dict[str, str]]           # [{"name": "id", "type": "INTEGER"}, ...]
    sample_data: pd.DataFrame               # First 5 rows for preview
    source_name: str                        # Which connector produced this


@dataclass
class QueryResult:
    """Result of executing a SQL query."""
    success: bool
    data: Optional[pd.DataFrame]
    row_count: int
    columns: List[str]
    error: Optional[str] = None
    execution_time_ms: Optional[float] = None


class BaseConnector(ABC):
    """
    Abstract base class for all data source connectors.
    
    Any new connector (MySQL, BigQuery, Snowflake, etc.) must implement
    all @abstractmethod methods below. This guarantees every agent can
    work with any connector without knowing its internals.
    """

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.name = config.name
        self.source_type = config.source_type
        self._connected = False
        logger.info(f"[{self.source_type.upper()}] Connector '{self.name}' initialized.")

    # ─────────────────────────────────────────────
    # REQUIRED — Every subclass must implement these
    # ─────────────────────────────────────────────

    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the data source.
        Returns True on success, False on failure.
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close and clean up the connection."""
        pass

    @abstractmethod
    def test_connection(self) -> Dict[str, Any]:
        """
        Ping the data source.
        Returns: {"success": bool, "message": str, "latency_ms": float}
        """
        pass

    @abstractmethod
    def list_tables(self) -> List[str]:
        """Return all table/dataset names available in this source."""
        pass

    @abstractmethod
    def get_table_profile(self, table_name: str) -> TableProfile:
        """Return schema + basic stats for a given table."""
        pass

    @abstractmethod
    def execute_query(self, sql: str, params: Optional[Dict] = None) -> QueryResult:
        """
        Run a SQL query and return results as a DataFrame.
        Must handle errors gracefully — never raise raw exceptions.
        """
        pass

    @abstractmethod
    def get_column_stats(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """
        Return per-column statistics used by the Profiling Agent.
        Should include: null_count, null_pct, distinct_count, min, max, mean, std
        """
        pass

    # ─────────────────────────────────────────────
    # SHARED — Available to all connectors for free
    # ─────────────────────────────────────────────

    def is_connected(self) -> bool:
        return self._connected

    def get_full_profile(self, table_name: str) -> Dict[str, Any]:
        """
        Convenience method: full profile + column stats for all columns.
        Agents call this instead of assembling it themselves.
        """
        profile = self.get_table_profile(table_name)
        column_stats = {}
        
        for col in profile.columns:
            col_name = col["name"]
            try:
                column_stats[col_name] = self.get_column_stats(table_name, col_name)
            except Exception as e:
                logger.warning(f"Could not get stats for column '{col_name}': {e}")
                column_stats[col_name] = {"error": str(e)}

        return {
            "source": self.name,
            "source_type": self.source_type,
            "table": table_name,
            "row_count": profile.row_count,
            "column_count": profile.column_count,
            "columns": profile.columns,
            "sample_data": profile.sample_data.to_dict(orient="records"),
            "column_stats": column_stats,
        }

    def __repr__(self):
        status = "connected" if self._connected else "disconnected"
        return f"<{self.__class__.__name__} name='{self.name}' status={status}>"

    # Context manager support: `with connector as c:`
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()