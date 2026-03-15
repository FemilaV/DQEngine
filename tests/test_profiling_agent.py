"""
Test the Profiling Agent against all 3 sample data sources.
Run from DQEngine root: python tests/test_profiling_agent.py
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agents.profiling_agent import run_profiling_agent

SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "../sample_data")


def print_report(report):
    """Pretty-print a TableDQReport."""
    score = report.overall_dq_score
    bar_filled = int(score / 5)
    bar = "█" * bar_filled + "░" * (20 - bar_filled)

    print(f"\n  {'─'*52}")
    print(f"  Table   : {report.table_name}")
    print(f"  Source  : {report.source_name} ({report.source_type})")
    print(f"  Rows    : {report.row_count:,}   Columns: {report.column_count}")
    print(f"  Score   : {score}/100  [{bar}]  {report.score_label()}")
    print(f"  Time    : {report.profiling_time_ms}ms")
    print(f"  {'─'*52}")

    print(f"\n  Dimension Scores:")
    for dim, sc in sorted(report.dimension_scores.items(), key=lambda x: x[1]):
        bar_d = "█" * int(sc / 5) + "░" * (20 - int(sc / 5))
        print(f"    {dim:<18} {sc:>5.1f}  [{bar_d}]")

    if report.critical_issues:
        print(f"\n  🔴 CRITICAL ({len(report.critical_issues)}):")
        for issue in report.critical_issues[:5]:
            print(f"    • {issue}")

    if report.warnings:
        print(f"\n  🟡 WARNINGS ({len(report.warnings)}):")
        for w in report.warnings[:5]:
            print(f"    • {w}")

    passed = len(report.passed_checks())
    failed = len(report.failed_checks())
    print(f"\n  Checks: {passed} passed, {failed} failed, {len(report.checks)} total")


def test_sqlite_orders():
    print("\n" + "="*58)
    print("  TEST 1: SQLite — orders table (has nulls, dupes)")
    print("="*58)

    report = run_profiling_agent(
        source_config={
            "source_type": "sqlite",
            "name": "Retail DB",
            "db_path": os.path.join(SAMPLE_DIR, "retail.db"),
        },
        table_name="orders",
    )
    print_report(report)
    assert report.overall_dq_score > 0
    assert report.row_count > 0
    # Should catch nulls in customer_id, region etc.
    completeness_checks = report.checks_by_type(
        __import__("models.profiling_models", fromlist=["CheckType"]).CheckType.COMPLETENESS
    )
    assert len(completeness_checks) > 0, "Expected completeness checks"
    print("\n  ✅ SQLite orders test PASSED")


def test_sqlite_employees():
    print("\n" + "="*58)
    print("  TEST 2: SQLite — employees table (timeliness test)")
    print("="*58)

    report = run_profiling_agent(
        source_config={
            "source_type": "sqlite",
            "name": "Retail DB",
            "db_path": os.path.join(SAMPLE_DIR, "retail.db"),
        },
        table_name="employees",
    )
    print_report(report)
    assert report.overall_dq_score > 0

    from models.profiling_models import CheckType
    timeliness_checks = report.checks_by_type(CheckType.TIMELINESS)
    print(f"\n  Timeliness checks found: {len(timeliness_checks)}")
    for t in timeliness_checks:
        print(f"    [{t.severity.value.upper()}] {t.column}: {t.message}")

    print("\n  ✅ SQLite employees test PASSED")


def test_csv_customers():
    print("\n" + "="*58)
    print("  TEST 3: CSV — customers (nulls + duplicates)")
    print("="*58)

    report = run_profiling_agent(
        source_config={
            "source_type": "csv",
            "name": "Customer Data",
            "file_path": os.path.join(SAMPLE_DIR, "customers.csv"),
        },
        table_name="customers",
    )
    print_report(report)

    from models.profiling_models import CheckType
    uniqueness_checks = report.checks_by_type(CheckType.UNIQUENESS)
    dup_check = next((c for c in uniqueness_checks if c.column is None), None)
    if dup_check:
        print(f"\n  Duplicate check: {dup_check.message}")
        assert not dup_check.passed, "Expected duplicates to be detected in customers.csv"

    print("\n  ✅ CSV customers test PASSED")


def test_csv_transactions():
    print("\n" + "="*58)
    print("  TEST 4: CSV — transactions (outliers + negatives)")
    print("="*58)

    report = run_profiling_agent(
        source_config={
            "source_type": "csv",
            "name": "Transaction Data",
            "file_path": os.path.join(SAMPLE_DIR, "transactions.csv"),
        },
        table_name="transactions",
    )
    print_report(report)

    from models.profiling_models import CheckType
    accuracy_checks = report.checks_by_type(CheckType.ACCURACY)
    validity_checks = report.checks_by_type(CheckType.VALIDITY)
    print(f"\n  Accuracy checks (outliers): {len(accuracy_checks)}")
    print(f"  Validity checks (negatives): {len(validity_checks)}")
    for c in accuracy_checks + validity_checks:
        print(f"    [{c.severity.value.upper()}] {c.column}: {c.message}")

    print("\n  ✅ CSV transactions test PASSED")


def test_error_handling():
    print("\n" + "="*58)
    print("  TEST 5: Error handling — bad table name")
    print("="*58)

    report = run_profiling_agent(
        source_config={
            "source_type": "sqlite",
            "name": "Retail DB",
            "db_path": os.path.join(SAMPLE_DIR, "retail.db"),
        },
        table_name="nonexistent_table",
    )
    assert report.overall_dq_score == 0.0
    assert len(report.critical_issues) > 0
    print(f"  ✅ Error caught gracefully: {report.critical_issues[0][:60]}...")


if __name__ == "__main__":
    print("\n🔍 Running Profiling Agent Tests")
    print("   Uses sequential fallback if LangGraph not installed\n")

    test_sqlite_orders()
    test_sqlite_employees()
    test_csv_customers()
    test_csv_transactions()
    test_error_handling()

    print("\n" + "="*58)
    print("  🎉 ALL PROFILING AGENT TESTS COMPLETE")
    print("="*58)