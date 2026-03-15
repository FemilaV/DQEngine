"""
Test all FastAPI endpoints.
Starts the server, runs all tests, stops the server.

Run: python tests/test_api.py

Requires: pip install httpx fastapi uvicorn
"""

import sys, os, time, json, subprocess, signal
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import httpx
except ImportError:
    print("❌ httpx not installed. Run: pip install httpx")
    sys.exit(1)

BASE_URL    = "http://localhost:8000"
SAMPLE_DIR  = os.path.abspath(os.path.join(os.path.dirname(__file__), "../sample_data"))

SQLITE_SRC = {"source_type": "sqlite", "name": "Retail DB",      "db_path":   f"{SAMPLE_DIR}/retail.db"}
CSV_SRC    = {"source_type": "csv",    "name": "Customer Data",   "file_path": f"{SAMPLE_DIR}/customers.csv"}


def sep(title):
    print(f"\n{'='*58}")
    print(f"  {title}")
    print(f"{'='*58}")


def check(label, condition, detail=""):
    icon = "✅" if condition else "❌"
    msg = f"  {icon} {label}"
    if detail:
        msg += f"  →  {detail}"
    print(msg)
    if not condition:
        raise AssertionError(f"FAILED: {label}")


# ──────────────────────────────────────────────────────────────
def test_health(client):
    sep("TEST 1: Health Check")
    r = client.get("/health")
    data = r.json()
    check("Status 200",        r.status_code == 200)
    check("status=ok",         data["status"] == "ok")
    check("llm_provider set",  bool(data["llm_provider"]),  data["llm_provider"])
    check("supported_sources", len(data["supported_sources"]) >= 3)
    print(f"  LLM Provider: {data['llm_provider']}")
    print(f"  Sources: {data['supported_sources']}")


# ──────────────────────────────────────────────────────────────
def test_list_tables(client):
    sep("TEST 2: List Tables")
    r = client.post("/sources/tables", json={"source_config": SQLITE_SRC})
    data = r.json()
    check("Status 200",         r.status_code == 200)
    check("success=True",       data["success"])
    check("Tables returned",    len(data["tables"]) >= 3)
    check("orders present",     "orders" in data["tables"])
    print(f"  Tables: {data['tables']}")

    # Test CSV
    r2 = client.post("/sources/tables", json={"source_config": CSV_SRC})
    data2 = r2.json()
    check("CSV tables returned", data2["success"])
    print(f"  CSV Tables: {data2['tables']}")


# ──────────────────────────────────────────────────────────────
def test_profile(client):
    sep("TEST 3: Profile Endpoint")
    r = client.post("/profile", json={
        "source_config": SQLITE_SRC,
        "table_name": "orders",
        "skip_report": True,
    }, timeout=60)
    data = r.json()
    check("Status 200",          r.status_code == 200)
    check("success=True",        data["success"])
    check("score > 0",           data["overall_dq_score"] > 0,  f"{data['overall_dq_score']}/100")
    check("score_label set",     bool(data["score_label"]),      data["score_label"])
    check("checks returned",     data["total_checks"] > 0,       f"{data['total_checks']} checks")
    check("critical_issues",     isinstance(data["critical_issues"], list))
    check("dimension_scores",    len(data["dimension_scores"]) >= 7)
    print(f"  Score: {data['overall_dq_score']}/100 — {data['score_label']}")
    print(f"  Checks: {data['total_checks']} total, {data['failed_checks']} failed")
    print(f"  Critical: {len(data['critical_issues'])}, Warnings: {len(data['warnings'])}")


# ──────────────────────────────────────────────────────────────
def test_query(client):
    sep("TEST 4: NL→SQL Query Endpoint")
    r = client.post("/query", json={
        "source_config": SQLITE_SRC,
        "table_name": "orders",
        "natural_language": "show me the first 5 orders",
        "max_rows": 5,
    }, timeout=60)
    data = r.json()
    check("Status 200",         r.status_code == 200)
    check("success=True",       data["success"])
    check("SQL generated",      bool(data["generated_sql"]))
    check("explanation set",    bool(data["explanation"]))
    check("columns returned",   len(data["columns"]) > 0)
    check("llm_provider set",   bool(data["llm_provider"]))
    print(f"  SQL: {data['generated_sql'][:80]}...")
    print(f"  Rows: {data['row_count']} | Provider: {data['llm_provider']}")
    print(f"  Explains: {data['explanation']}")


# ──────────────────────────────────────────────────────────────
def test_report(client):
    sep("TEST 5: Report Endpoint")
    r = client.post("/report", json={
        "source_config": SQLITE_SRC,
        "table_name": "orders",
        "return_html": True,
    }, timeout=120)
    data = r.json()
    check("Status 200",       r.status_code == 200)
    check("success=True",     data["success"])
    check("score returned",   data["overall_dq_score"] > 0)
    check("report_path set",  bool(data.get("report_path")))
    check("HTML returned",    bool(data.get("html")), f"{len(data.get('html',''))} chars")
    print(f"  Score: {data['overall_dq_score']}/100 — {data['score_label']}")
    print(f"  Report: {os.path.basename(data['report_path'])}")


# ──────────────────────────────────────────────────────────────
def test_pipeline(client):
    sep("TEST 6: Full Pipeline Endpoint")
    r = client.post("/pipeline", json={
        "source_config": SQLITE_SRC,
        "table_name": "orders",
        "nl_question": "how many orders have missing customer id?",
        "max_rows": 10,
        "skip_report": False,
    }, timeout=120)
    data = r.json()
    check("Status 200",            r.status_code == 200)
    check("success=True",          data["success"],             data.get("error",""))
    check("overall_score > 0",     data["overall_score"] > 0,   f"{data['overall_score']}/100")
    check("stages completed",      len(data["stages_completed"]) >= 2)
    check("profile in response",   data.get("profile") is not None)
    check("query in response",     data.get("query") is not None)
    check("report_path set",       bool(data.get("report_path")))
    check("total_time_ms > 0",     data["total_time_ms"] > 0,   f"{data['total_time_ms']}ms")
    print(f"  Score     : {data['overall_score']}/100 — {data['score_label']}")
    print(f"  Stages    : {data['stages_completed']}")
    print(f"  Time      : {data['total_time_ms']}ms")
    print(f"  SQL       : {data['query']['generated_sql'][:80]}...")
    print(f"  Report    : {os.path.basename(data.get('report_path',''))}")


# ──────────────────────────────────────────────────────────────
def test_report_list(client):
    sep("TEST 7: List Reports")
    r = client.get("/report/list")
    data = r.json()
    check("Status 200",   r.status_code == 200)
    check("reports list", "reports" in data)
    print(f"  Reports on disk: {data['count']}")
    for rpt in data["reports"][:3]:
        print(f"    • {rpt['filename']}  ({rpt['size_kb']} KB)")


# ──────────────────────────────────────────────────────────────
def test_csv_pipeline(client):
    sep("TEST 8: CSV Source Pipeline")
    r = client.post("/pipeline", json={
        "source_config": CSV_SRC,
        "table_name": "customers",
        "nl_question": "show customers with missing email",
        "skip_report": True,
    }, timeout=120)
    data = r.json()
    check("Status 200",        r.status_code == 200)
    check("success=True",      data["success"])
    check("score > 0",         data["overall_score"] > 0)
    print(f"  CSV Score: {data['overall_score']}/100 — {data['score_label']}")


# ──────────────────────────────────────────────────────────────
def wait_for_server(client, max_wait=15):
    print("  Waiting for server to start", end="", flush=True)
    for _ in range(max_wait):
        try:
            client.get("/health", timeout=2)
            print(" ✓")
            return True
        except Exception:
            print(".", end="", flush=True)
            time.sleep(1)
    print(" ✗")
    return False


if __name__ == "__main__":
    print("\n🚀 DQ Engine API Tests")
    print("   Starting server on port 8000...\n")

    # Start uvicorn server as subprocess
    server = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "api.main:app", "--port", "8000", "--log-level", "error"],
        cwd=os.path.join(os.path.dirname(__file__), ".."),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        with httpx.Client(base_url=BASE_URL) as client:
            if not wait_for_server(client):
                print("❌ Server failed to start.")
                server.terminate()
                sys.exit(1)

            test_health(client)
            test_list_tables(client)
            test_profile(client)
            test_query(client)
            test_report(client)
            test_pipeline(client)
            test_report_list(client)
            test_csv_pipeline(client)

            print(f"\n{'='*58}")
            print("  🎉 ALL API TESTS COMPLETE")
            print(f"{'='*58}\n")

    finally:
        server.terminate()
        server.wait()
        print("  Server stopped.")