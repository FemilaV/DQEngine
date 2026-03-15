"""
Connector Factory — Single entry point to get any connector by type.

This is the ONLY import agents and API routes need.
They don't care whether the source is SQLite, Postgres, or CSV — 
the factory hands them the right connector and they use the same interface.
"""

import logging
from typing import Dict, List, Optional

from .base_connector import BaseConnector, ConnectionConfig
from .sqlite_connector import SQLiteConnector
from .postgres_connector import PostgreSQLConnector
from .csv_connector import CSVConnector

logger = logging.getLogger(__name__)

# Registry maps source_type strings → connector classes
CONNECTOR_REGISTRY: Dict[str, type] = {
    "sqlite": SQLiteConnector,
    "postgres": PostgreSQLConnector,
    "postgresql": PostgreSQLConnector,    # Alias
    "csv": CSVConnector,
}


def get_connector(config: ConnectionConfig) -> BaseConnector:
    """
    Factory function: given a ConnectionConfig, return the right connector.
    
    Example:
        cfg = ConnectionConfig(source_type="sqlite", name="Sales", db_path="./sales.db")
        connector = get_connector(cfg)
        connector.connect()
    
    Raises ValueError for unknown source types.
    """
    source_type = config.source_type.lower().strip()
    connector_class = CONNECTOR_REGISTRY.get(source_type)

    if not connector_class:
        supported = list(CONNECTOR_REGISTRY.keys())
        raise ValueError(
            f"Unknown source_type '{source_type}'. Supported: {supported}\n"
            f"To add a new connector, register it in CONNECTOR_REGISTRY in factory.py"
        )

    logger.info(f"[Factory] Creating {connector_class.__name__} for source '{config.name}'")
    return connector_class(config)


def get_connector_from_dict(config_dict: dict) -> BaseConnector:
    """
    Convenience: build a connector directly from a plain dictionary.
    Useful for loading from config.yaml or API request bodies.
    
    Example:
        config_dict = {
            "source_type": "postgres",
            "name": "Prod DB",
            "host": "localhost",
            "database": "mydb",
            "username": "admin",
            "password": "secret"
        }
        connector = get_connector_from_dict(config_dict)
    """
    config = ConnectionConfig(**config_dict)
    return get_connector(config)


def list_supported_sources() -> List[str]:
    """Return all supported source type strings."""
    return sorted(set(CONNECTOR_REGISTRY.keys()))


def register_connector(source_type: str, connector_class: type) -> None:
    """
    Extend the registry at runtime — add connectors without editing this file.
    
    Example (adding MySQL later):
        from connectors.factory import register_connector
        from connectors.mysql_connector import MySQLConnector
        register_connector("mysql", MySQLConnector)
    """
    if not issubclass(connector_class, BaseConnector):
        raise TypeError(f"{connector_class.__name__} must inherit from BaseConnector.")
    CONNECTOR_REGISTRY[source_type.lower()] = connector_class
    logger.info(f"[Factory] Registered new connector: '{source_type}' → {connector_class.__name__}")