"""
Test the full DQ Engine orchestrator pipeline end-to-end.
Run: python tests/test_orchestrator.py

This is the most important test — it exercises ALL agents together.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from orchestrator import run_dq_pipeline

SAMPLE_DIR  = os.path.join(os.path.dirname(__file__), "../sample_data")
REPORTS_DIR = os.path.join(os.path.dirname(__file__), "../reports")

RETAIL_CFG = {
    "source_type": "sqlite",
    "name": "Retail DB",
    "db_path": os.path.join(SAMPLE_DIR, "retail.db"),
}
CUST_CFG = {
    "source_type": "csv",
    "name": "Customer Data",
    "file_path": os.path.join(SAMPLE_DIR, "customers.csv"),
}
TXN_CFG = {
    "source_type": "csv",
    "name": "Transaction Data",
    "file_path": os.path.join(SAMPLE_DIR, "transactions.csv"),
}


def print_result(result, title=""):
    icon = "✅" if result.success else "❌"
    print(f"\n  {icon} {title}")
    if not result.success:
        print(f"  Error: {result.error}")
        return

    print(f"  Score       : {result.overall_score}/100 — {result.score_label}")
    print(f"  Time        : {result.total_time_ms}ms")
    print(f"  Stages done : {', '.join(result.stages_completed)}")

    if result.dq_report:
        r = result.dq_report
        print(f"  Rows        : {r.row_count:,}   Columns: {r.column_count}")
        print(f"  Checks      : {len(r.checks)} total, "
              f"{len(r.passed_checks())} passed, {len(r.failed_checks())} failed")
        if r.critical_issues:
            print(f"  🔴 Critical :")
            for i in r.critical_issues[:3]:
                print(f"     • {i}")
        if r.warnings:
            print(f"  🟡 Warnings :")
            for w in r.warnings[:3]:
                print(f"     • {w}")

    if result.nl_sql_result and result.nl_sql_result.success:
        nl = result.nl_sql_result
        print(f"  SQL         : {nl.generated_sql[:90]}...")
        print(f"  SQL Rows    : {nl.row_count}")
        print(f"  Explains    : {nl.explanation}")

    if result.report_path:
        print(f"  Report      : {os.path.basename(result.report_path)}")


def sep(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ──────────────────────────────────────────────────────────────
# TEST 1: Profiling + Report (no NL question)
# ──────────────────────────────────────────────────────────────
def test_profiling_and_report():
    sep("TEST 1: Profiling + Report — orders table")

    result = run_dq_pipeline(
        source_config=RETAIL_CFG,
        table_name="orders",
        output_dir=REPORTS_DIR,
    )

    assert result.success, f"Pipeline failed: {result.error}"
    assert result.dq_report is not None
    assert result.overall_score > 0
    assert result.report_path is not None
    assert "profiling" in result.stages_completed
    assert "report"    in result.stages_completed
    assert result.nl_sql_result is None   # No question given

    print_result(result, "orders — profiling + report")
    print("\n  ✅ TEST 1 PASSED")


# ──────────────────────────────────────────────────────────────
# TEST 2: Full pipeline with NL question
# ──────────────────────────────────────────────────────────────
def test_full_pipeline_with_nl():
    sep("TEST 2: Full pipeline — profiling + NL→SQL + report")

    result = run_dq_pipeline(
        source_config=RETAIL_CFG,
        table_name="orders",
        nl_question="show me orders where customer_id is missing",
        output_dir=REPORTS_DIR,
    )

    assert result.success
    assert result.dq_report is not None
    assert result.nl_sql_result is not None
    assert "profiling" in result.stages_completed
    assert "nl_sql"    in result.stages_completed
    assert "report"    in result.stages_completed

    print_result(result, "orders — full pipeline with NL question")
    print("\n  ✅ TEST 2 PASSED")


# ──────────────────────────────────────────────────────────────
# TEST 3: CSV source
# ──────────────────────────────────────────────────────────────
def test_csv_pipeline():
    sep("TEST 3: CSV source — customers with NL question")

    result = run_dq_pipeline(
        source_config=CUST_CFG,
        table_name="customers",
        nl_question="how many customers have missing email?",
        output_dir=REPORTS_DIR,
    )

    assert result.success
    assert result.dq_report is not None
    print_result(result, "customers CSV — full pipeline")
    print("\n  ✅ TEST 3 PASSED")


# ──────────────────────────────────────────────────────────────
# TEST 4: Skip report (profiling + NL only — faster for API)
# ──────────────────────────────────────────────────────────────
def test_skip_report():
    sep("TEST 4: Skip report — profiling + NL only")

    result = run_dq_pipeline(
        source_config=RETAIL_CFG,
        table_name="employees",
        nl_question="show employees with missing department",
        skip_report=True,
    )

    assert result.success
    assert result.report_path is None    # Report was skipped
    assert result.dq_report is not None
    print_result(result, "employees — skip_report=True")
    print("\n  ✅ TEST 4 PASSED")


# ──────────────────────────────────────────────────────────────
# TEST 5: Multiple tables — pipeline called in a loop
# ──────────────────────────────────────────────────────────────
def test_multi_table_loop():
    sep("TEST 5: Multiple tables in a loop")

    tables = ["orders", "products", "employees"]
    scores = {}

    for table in tables:
        result = run_dq_pipeline(
            source_config=RETAIL_CFG,
            table_name=table,
            skip_report=True,    # Skip reports for speed
        )
        assert result.success, f"Failed for {table}: {result.error}"
        scores[table] = result.overall_score
        print(f"  ✓ {table:<12} → {result.overall_score}/100 — {result.score_label}")

    print(f"\n  Best table  : {max(scores, key=scores.get)} ({max(scores.values())}/100)")
    print(f"  Worst table : {min(scores, key=scores.get)} ({min(scores.values())}/100)")
    print("\n  ✅ TEST 5 PASSED")


# ──────────────────────────────────────────────────────────────
# TEST 6: Error handling — bad table name
# ──────────────────────────────────────────────────────────────
def test_error_handling():
    sep("TEST 6: Error handling — bad inputs")

    # Bad table name — profiling fails internally, pipeline still returns
    result = run_dq_pipeline(
        source_config=RETAIL_CFG,
        table_name="this_table_does_not_exist",
    )
    # Either pipeline fails OR score is 0 (profiling found nothing)
    assert not result.success or result.overall_score == 0.0
    msg = result.error or "score=0 (table not found)"
    print(f"  ✓ Bad table caught: {str(msg)[:60]}...")

    # Missing table_name
    result = run_dq_pipeline(
        source_config=RETAIL_CFG,
        table_name="",
    )
    assert not result.success
    print(f"  ✓ Empty table caught: {result.error}")

    # Bad source type
    result = run_dq_pipeline(
        source_config={"source_type": "mongodb", "name": "Test"},
        table_name="orders",
    )
    assert not result.success
    print(f"  ✓ Bad source_type caught: {result.error}")

    print("\n  ✅ TEST 6 PASSED")


if __name__ == "__main__":
    print("\n🚀 Running DQ Engine Orchestrator Tests")
    print("   Tests ALL agents together: Profiling + NL→SQL + Report\n")

    test_profiling_and_report()
    test_full_pipeline_with_nl()
    test_csv_pipeline()
    test_skip_report()
    test_multi_table_loop()
    test_error_handling()

    print(f"\n{'='*60}")
    print("  🎉 ALL ORCHESTRATOR TESTS COMPLETE")
    print(f"  Reports saved in: ./reports/")
    print(f"{'='*60}\n")