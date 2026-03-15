"""
Test all three connectors end-to-end.
Run: python tests/test_connectors.py

Expected output: All tests PASS with profile data printed.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from connectors import (
    ConnectionConfig,
    get_connector,
    list_supported_sources,
)


def print_separator(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def test_factory():
    print_separator("TEST: Connector Factory")
    sources = list_supported_sources()
    print(f"✅ Supported sources: {sources}")
    assert "sqlite" in sources
    assert "postgres" in sources
    assert "csv" in sources
    print("✅ Factory test PASSED")


def test_sqlite_connector():
    print_separator("TEST: SQLite Connector")

    db_path = os.path.join(os.path.dirname(__file__), "../sample_data/retail.db")
    if not os.path.exists(db_path):
        print("⚠️  retail.db not found. Run: python sample_data/generate_samples.py")
        return

    config = ConnectionConfig(source_type="sqlite", name="Retail DB", db_path=db_path)

    with get_connector(config) as conn:
        # Test connection
        health = conn.test_connection()
        print(f"✅ Connection health: {health}")
        assert health["success"] is True

        # List tables
        tables = conn.list_tables()
        print(f"✅ Tables found: {tables}")
        assert len(tables) > 0

        # Profile a table
        profile = conn.get_table_profile("orders")
        print(f"✅ orders → {profile.row_count} rows, {profile.column_count} columns")
        print(f"   Columns: {[c['name'] for c in profile.columns]}")
        print(f"   Sample:\n{profile.sample_data[['order_id','customer_id','status']].head(3)}")

        # Column stats
        stats = conn.get_column_stats("orders", "customer_id")
        print(f"✅ customer_id stats: null_pct={stats['null_pct']}%, distinct={stats['distinct_count']}")

        # Full profile (used by agents)
        full = conn.get_full_profile("products")
        print(f"✅ Full profile keys: {list(full.keys())}")

        # Custom query
        result = conn.execute_query(
            "SELECT status, COUNT(*) as cnt FROM orders GROUP BY status ORDER BY cnt DESC"
        )
        print(f"✅ Custom query result:\n{result.data}")

    print("\n✅ SQLite connector PASSED")


def test_csv_connector():
    print_separator("TEST: CSV Connector")

    csv_path = os.path.join(os.path.dirname(__file__), "../sample_data/customers.csv")
    if not os.path.exists(csv_path):
        print("⚠️  customers.csv not found. Run: python sample_data/generate_samples.py")
        return

    config = ConnectionConfig(source_type="csv", name="Customer Data", file_path=csv_path)

    with get_connector(config) as conn:
        health = conn.test_connection()
        print(f"✅ Connection health: {health}")

        tables = conn.list_tables()
        print(f"✅ Tables: {tables}")

        profile = conn.get_table_profile("customers")
        print(f"✅ customers → {profile.row_count} rows, {profile.column_count} columns")

        # Check null detection works
        email_stats = conn.get_column_stats("customers", "email")
        print(f"✅ email stats: null_pct={email_stats['null_pct']}%, distinct={email_stats['distinct_count']}")
        assert email_stats["null_count"] > 0, "Expected some nulls in email column"

        # SQL against CSV
        result = conn.execute_query(
            "SELECT city, COUNT(*) as cnt FROM customers WHERE city IS NOT NULL GROUP BY city ORDER BY cnt DESC LIMIT 5"
        )
        print(f"✅ Top cities:\n{result.data}")

    # Test directory mode (load all CSVs from sample_data/)
    print("\n--- Directory mode test ---")
    dir_path = os.path.join(os.path.dirname(__file__), "../sample_data/")
    config_dir = ConnectionConfig(source_type="csv", name="All CSVs", file_path=dir_path)
    with get_connector(config_dir) as conn:
        tables = conn.list_tables()
        print(f"✅ Directory mode loaded tables: {tables}")

    print("\n✅ CSV connector PASSED")


def test_postgres_connector():
    """
    PostgreSQL test — only runs if PG_HOST env var is set.
    Set these env vars to test:
        PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS
    """
    print_separator("TEST: PostgreSQL Connector")

    host = os.environ.get("PG_HOST")
    if not host:
        print("⏭️  Skipping PostgreSQL test (set PG_HOST env var to run)")
        print("   Example: export PG_HOST=localhost PG_DB=testdb PG_USER=postgres PG_PASS=secret")
        return

    config = ConnectionConfig(
        source_type="postgres",
        name="Test Postgres",
        host=host,
        port=int(os.environ.get("PG_PORT", 5432)),
        database=os.environ.get("PG_DB", "postgres"),
        username=os.environ.get("PG_USER", "postgres"),
        password=os.environ.get("PG_PASS", ""),
    )

    with get_connector(config) as conn:
        health = conn.test_connection()
        print(f"✅ Connection health: {health}")
        assert health["success"] is True

        tables = conn.list_tables()
        print(f"✅ Tables: {tables}")

        if tables:
            profile = conn.get_table_profile(tables[0])
            print(f"✅ Profile for '{tables[0]}': {profile.row_count} rows")

    print("\n✅ PostgreSQL connector PASSED")


def test_context_manager_and_error_handling():
    print_separator("TEST: Error Handling")

    # Bad file path
    config = ConnectionConfig(source_type="csv", name="Bad CSV", file_path="./nonexistent.csv")
    connector = get_connector(config)
    result = connector.connect()
    print(f"✅ Bad file path gracefully returns False: {result}")
    assert result is False

    # Unknown query
    db_path = os.path.join(os.path.dirname(__file__), "../sample_data/retail.db")
    if os.path.exists(db_path):
        config = ConnectionConfig(source_type="sqlite", name="Test", db_path=db_path)
        with get_connector(config) as conn:
            result = conn.execute_query("SELECT * FROM nonexistent_table_xyz")
            print(f"✅ Bad query returns success=False: {result.success}")
            print(f"   Error: {result.error}")
            assert result.success is False

    print("\n✅ Error handling PASSED")


if __name__ == "__main__":
    print("🧪 Running Connector Tests")
    print("Make sure you've run: python sample_data/generate_samples.py first\n")

    test_factory()
    test_sqlite_connector()
    test_csv_connector()
    test_postgres_connector()
    test_context_manager_and_error_handling()

    print("\n" + "="*60)
    print("  🎉 ALL CONNECTOR TESTS COMPLETE")
    print("="*60)