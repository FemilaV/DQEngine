"""Connectors package — public API."""

from .base_connector import BaseConnector, ConnectionConfig, QueryResult, TableProfile
from .sqlite_connector import SQLiteConnector
from .postgres_connector import PostgreSQLConnector
from .csv_connector import CSVConnector
from .factory import get_connector, get_connector_from_dict, list_supported_sources, register_connector

__all__ = [
    "BaseConnector",
    "ConnectionConfig",
    "QueryResult",
    "TableProfile",
    "SQLiteConnector",
    "PostgreSQLConnector",
    "CSVConnector",
    "get_connector",
    "get_connector_from_dict",
    "list_supported_sources",
    "register_connector",
]