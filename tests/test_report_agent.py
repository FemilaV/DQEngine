"""
Test the Report Agent.
Run: python tests/test_report_agent.py

Opens the generated HTML report automatically in your browser.
"""

import sys, os, webbrowser
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agents.profiling_agent import run_profiling_agent
from agents.report_agent import run_report_agent, generate_multi_table_report

SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "../sample_data")
REPORTS_DIR = os.path.join(os.path.dirname(__file__), "../reports")

def sep(title):
    print(f"\n{'='*58}")
    print(f"  {title}")
    print(f"{'='*58}")


def test_orders_report():
    sep("TEST 1: Generate report for orders table")

    print("  Running profiling agent...")
    report = run_profiling_agent(
        source_config={
            "source_type": "sqlite",
            "name": "Retail DB",
            "db_path": os.path.join(SAMPLE_DIR, "retail.db"),
        },
        table_name="orders",
    )
    print(f"  ✓ Profiled: score={report.overall_dq_score}/100 ({report.score_label()})")

    print("  Generating HTML report...")
    result = run_report_agent(report, output_dir=REPORTS_DIR)

    assert result["success"], f"Report failed: {result.get('error')}"
    print(f"  ✓ Report saved: {result['file_path']}")
    print(f"  ✓ Score: {result['score']}/100 — {result['score_label']}")

    # Open in browser
    webbrowser.open(f"file:///{result['file_path'].replace(os.sep, '/')}")
    print("  ✓ Opened in browser")
    return result["file_path"]


def test_customers_report():
    sep("TEST 2: Generate report for customers CSV")

    report = run_profiling_agent(
        source_config={
            "source_type": "csv",
            "name": "Customer Data",
            "file_path": os.path.join(SAMPLE_DIR, "customers.csv"),
        },
        table_name="customers",
    )
    result = run_report_agent(report, output_dir=REPORTS_DIR)
    assert result["success"]
    print(f"  ✓ Report: {os.path.basename(result['file_path'])}")
    print(f"  ✓ Score: {result['score']}/100 — {result['score_label']}")
    return result["file_path"]


def test_multi_table_reports():
    sep("TEST 3: Generate reports for all 3 SQLite tables")

    tables = ["orders", "products", "employees"]
    reports = []
    for table in tables:
        r = run_profiling_agent(
            source_config={
                "source_type": "sqlite",
                "name": "Retail DB",
                "db_path": os.path.join(SAMPLE_DIR, "retail.db"),
            },
            table_name=table,
        )
        reports.append(r)
        print(f"  ✓ {table}: {r.overall_dq_score}/100 — {r.score_label()}")

    paths = generate_multi_table_report(reports, output_dir=REPORTS_DIR)
    assert len(paths) == 3
    print(f"\n  ✓ {len(paths)} reports generated in ./reports/")
    for p in paths:
        print(f"    • {os.path.basename(p)}")


if __name__ == "__main__":
    print("\n📊 Running Report Agent Tests\n")

    # Check dependencies
    try:
        import plotly
        print("  ✓ plotly available")
    except ImportError:
        print("  ✗ plotly missing — run: pip install plotly")

    try:
        import jinja2
        print("  ✓ jinja2 available")
    except ImportError:
        print("  ✗ jinja2 missing — run: pip install jinja2")
        sys.exit(1)

    test_orders_report()
    test_customers_report()
    test_multi_table_reports()

    print(f"\n{'='*58}")
    print("  🎉 ALL REPORT AGENT TESTS COMPLETE")
    print(f"  Reports saved in: ./reports/")
    print(f"{'='*58}\n")