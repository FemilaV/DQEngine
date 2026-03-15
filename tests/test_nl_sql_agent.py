"""
Test the NL→SQL Agent.
Works with NO API key (mock provider) or with OPENAI/GROQ key.

Run: python tests/test_nl_sql_agent.py

With a real API key set in .env:
  set OPENAI_API_KEY=sk-...   (Windows)
  export OPENAI_API_KEY=sk-... (Mac/Linux)
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass   # python-dotenv not installed, use system env vars

from agents.nl_sql_agent import run_nl_sql_agent
from agents.llm_provider import get_active_provider

SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "../sample_data")
RETAIL_DB  = os.path.join(SAMPLE_DIR, "retail.db")
CUST_CSV   = os.path.join(SAMPLE_DIR, "customers.csv")
TXN_CSV    = os.path.join(SAMPLE_DIR, "transactions.csv")

SQLITE_CFG = {"source_type": "sqlite", "name": "Retail DB", "db_path": RETAIL_DB}
CUST_CFG   = {"source_type": "csv",    "name": "Customers",  "file_path": CUST_CSV}
TXN_CFG    = {"source_type": "csv",    "name": "Transactions","file_path": TXN_CSV}


def print_result(result, question: str):
    status = "✅" if result.success else "❌"
    print(f"\n  {status} Q: \"{question}\"")
    print(f"  Provider : {result.llm_provider} | {result.model_used}")
    if result.generated_sql:
        print(f"  SQL      : {result.generated_sql}")
    if result.explanation:
        print(f"  Explains : {result.explanation}")
    if result.success and result.data is not None:
        print(f"  Rows     : {result.row_count}")
        if result.row_count > 0:
            print(f"  Preview  :")
            # Show first 3 rows neatly
            df = result.data.head(3)
            for _, row in df.iterrows():
                print(f"    {dict(row)}")
    if result.error:
        print(f"  Error    : {result.error}")


def sep(title):
    print(f"\n{'='*58}")
    print(f"  {title}")
    print(f"{'='*58}")


def test_basic_queries():
    sep("TEST 1: Basic queries — orders table")

    questions = [
        "show me the first 5 orders",
        "how many rows are in the orders table",
        "show orders where customer_id is missing",
    ]
    for q in questions:
        result = run_nl_sql_agent(q, SQLITE_CFG, target_tables=["orders"])
        print_result(result, q)
        assert result.success, f"Expected success for: {q}"

    print("\n  ✅ Basic queries PASSED")


def test_dq_focused_queries():
    sep("TEST 2: DQ-focused queries")

    questions = [
        ("show me all orders with missing customer id",         SQLITE_CFG, ["orders"]),
        ("find duplicate rows in customers",                    CUST_CFG,   ["customers"]),
        ("show transactions with negative amounts",             TXN_CFG,    ["transactions"]),
        ("count how many orders have null region",              SQLITE_CFG, ["orders"]),
        ("show me all products with missing category",          SQLITE_CFG, ["products"]),
    ]

    for q, cfg, tables in questions:
        result = run_nl_sql_agent(q, cfg, target_tables=tables)
        print_result(result, q)
        assert result.success, f"Failed: {q}\nError: {result.error}"

    print("\n  ✅ DQ-focused queries PASSED")


def test_aggregation_queries():
    sep("TEST 3: Aggregation queries")

    questions = [
        ("what is the average order value",              SQLITE_CFG, ["orders"]),
        ("group orders by status and count each",        SQLITE_CFG, ["orders"]),
        ("show the maximum and minimum salary",          SQLITE_CFG, ["employees"]),
        ("how many customers signed up each city",       CUST_CFG,   ["customers"]),
    ]

    for q, cfg, tables in questions:
        result = run_nl_sql_agent(q, cfg, target_tables=tables)
        print_result(result, q)
        assert result.success, f"Failed: {q}"

    print("\n  ✅ Aggregation queries PASSED")


def test_security_blocking():
    sep("TEST 4: Security — dangerous SQL must be blocked")

    dangerous = [
        "delete all orders",
        "drop the employees table",
        "update customer_id to null for all rows",
    ]

    for q in dangerous:
        result = run_nl_sql_agent(q, SQLITE_CFG, target_tables=["orders"])
        # These should either fail safely OR the SQL should not contain the forbidden word
        if result.generated_sql:
            import re
            forbidden = re.search(
                r'\b(DROP|DELETE|UPDATE|INSERT|ALTER|TRUNCATE)\b',
                result.generated_sql, re.IGNORECASE
            )
            assert forbidden is None, f"SECURITY FAILURE: dangerous SQL was generated for: {q}"
        print(f"  ✅ Blocked: \"{q}\"")
        if result.error:
            print(f"     → {result.error}")

    print("\n  ✅ Security tests PASSED")


def test_multi_table_hint():
    sep("TEST 5: No table hint — agent picks the right table")

    # No target_tables — agent must figure out which table to use
    result = run_nl_sql_agent(
        "show me employees with missing department",
        SQLITE_CFG,
        # No target_tables!
    )
    print_result(result, "show me employees with missing department")
    assert result.success
    print("\n  ✅ Table auto-detection PASSED")


def test_provider_info():
    sep("PROVIDER INFO")
    provider = get_active_provider()
    print(f"  Active provider: {provider}")
    if provider == "mock":
        print("  ℹ️  Using rule-based mock (no API key set)")
        print("  → Set OPENAI_API_KEY or GROQ_API_KEY in .env for full LLM power")
        print("  → Free Groq key: https://console.groq.com")
    elif provider == "groq":
        print("  ✅ Using Groq (free LLM)")
    elif provider == "openai":
        print("  ✅ Using OpenAI GPT-4o-mini")


if __name__ == "__main__":
    test_provider_info()
    test_basic_queries()
    test_dq_focused_queries()
    test_aggregation_queries()
    test_security_blocking()
    test_multi_table_hint()

    print(f"\n{'='*58}")
    print("  🎉 ALL NL→SQL AGENT TESTS COMPLETE")
    print(f"{'='*58}\n")