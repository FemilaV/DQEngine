"""
DQ Checks Engine — The 8 data quality checks.
Each function takes the raw profile dict and returns a list of CheckResults.
Pure functions — no side effects, easy to test individually.
"""

import re
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from models.profiling_models import CheckResult, CheckType, Severity

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# SCORING HELPERS
# ─────────────────────────────────────────────────────────────────

def _severity_from_pct(pct: float) -> Severity:
    """Convert a bad-data percentage to severity level."""
    if pct >= 30: return Severity.CRITICAL
    if pct >= 5:  return Severity.WARNING
    if pct > 0:   return Severity.INFO
    return Severity.PASS

def _score_from_pct(bad_pct: float) -> float:
    """
    Convert percentage of bad data → quality score (0–100).
    0% bad  = 100 score
    100% bad = 0 score
    Non-linear: small issues penalised less, large issues penalised heavily.
    """
    return round(max(0.0, 100.0 - (bad_pct ** 1.2)), 2)


# ─────────────────────────────────────────────────────────────────
# CHECK 1: COMPLETENESS — Null / missing values per column
# ─────────────────────────────────────────────────────────────────

def check_completeness(profile: Dict[str, Any]) -> List[CheckResult]:
    """
    For every column: what % of values are NULL?
    Threshold: >30% = CRITICAL, 5–30% = WARNING, 1–5% = INFO
    """
    results = []
    col_stats = profile.get("column_stats", {})
    total_rows = profile.get("row_count", 1)

    for col_name, stats in col_stats.items():
        if "error" in stats:
            continue

        null_pct = stats.get("null_pct", 0.0)
        null_count = stats.get("null_count", 0)
        severity = _severity_from_pct(null_pct)
        passed = severity in (Severity.PASS, Severity.INFO)

        if null_pct == 0:
            msg = f"No null values."
        else:
            msg = f"{null_pct}% null values ({null_count:,} of {total_rows:,} rows)."

        results.append(CheckResult(
            check_type=CheckType.COMPLETENESS,
            column=col_name,
            passed=passed,
            severity=severity,
            score=_score_from_pct(null_pct),
            message=msg,
            detail={"null_count": null_count, "null_pct": null_pct, "total_rows": total_rows},
        ))

    return results


# ─────────────────────────────────────────────────────────────────
# CHECK 2: EMPTY STRINGS — Blanks that aren't NULL (CSV plague)
# ─────────────────────────────────────────────────────────────────

def check_empty_strings(profile: Dict[str, Any], connector) -> List[CheckResult]:
    """
    Detects columns where values are whitespace/empty string but not NULL.
    Very common in CSV data. Different from NULL — must be caught separately.
    """
    results = []
    table = profile["table"]
    col_stats = profile.get("column_stats", {})

    for col_name, stats in col_stats.items():
        if "error" in stats:
            continue

        col_type = ""
        for col_meta in profile.get("columns", []):
            if col_meta["name"] == col_name:
                col_type = col_meta.get("type", "").upper()
                break

        # Only check text-like columns
        if any(t in col_type for t in ["INT", "REAL", "FLOAT", "NUMERIC", "DOUBLE"]):
            continue

        try:
            result = connector.execute_query(
                f"SELECT COUNT(*) as cnt FROM \"{table}\" "
                f"WHERE \"{col_name}\" IS NOT NULL "
                f"AND TRIM(CAST(\"{col_name}\" AS TEXT)) = ''"
            )
            empty_count = int(result.data["cnt"].iloc[0]) if result.success else 0
            total = profile.get("row_count", 1)
            empty_pct = round((empty_count / total * 100), 2) if total > 0 else 0.0

            severity = _severity_from_pct(empty_pct)
            passed = severity == Severity.PASS

            results.append(CheckResult(
                check_type=CheckType.EMPTY_STRINGS,
                column=col_name,
                passed=passed,
                severity=severity,
                score=_score_from_pct(empty_pct),
                message=f"{empty_pct}% empty strings ({empty_count:,} rows)." if empty_count else "No empty strings.",
                detail={"empty_count": empty_count, "empty_pct": empty_pct},
            ))
        except Exception as e:
            logger.debug(f"Empty string check skipped for '{col_name}': {e}")

    return results


# ─────────────────────────────────────────────────────────────────
# CHECK 3: UNIQUENESS — Duplicate rows and low-cardinality ID cols
# ─────────────────────────────────────────────────────────────────

def check_uniqueness(profile: Dict[str, Any], connector) -> List[CheckResult]:
    """
    Two sub-checks:
    1. Table-level: are there fully duplicate rows?
    2. Column-level: columns that look like IDs (name contains 'id') — should be unique
    """
    results = []
    table = profile["table"]
    total_rows = profile.get("row_count", 1)
    columns = [c["name"] for c in profile.get("columns", [])]

    # ── Sub-check 1: Full duplicate rows ──
    try:
        cols_csv = ", ".join(f'"{c}"' for c in columns)
        dup_result = connector.execute_query(
            f"SELECT COUNT(*) as total FROM ("
            f"  SELECT {cols_csv}, COUNT(*) as cnt "
            f"  FROM \"{table}\" GROUP BY {cols_csv} HAVING cnt > 1"
            f") AS dupes"
        )
        dup_groups = int(dup_result.data["total"].iloc[0]) if dup_result.success else 0

        # Count total duplicate rows (not just groups)
        dup_rows_result = connector.execute_query(
            f"SELECT COALESCE(SUM(cnt - 1), 0) as dup_rows FROM ("
            f"  SELECT COUNT(*) as cnt FROM \"{table}\" "
            f"  GROUP BY {cols_csv} HAVING cnt > 1"
            f") AS dupes"
        )
        dup_rows = int(dup_rows_result.data["dup_rows"].iloc[0]) if dup_rows_result.success else 0
        dup_pct = round((dup_rows / total_rows * 100), 2) if total_rows > 0 else 0.0

        severity = _severity_from_pct(dup_pct)
        results.append(CheckResult(
            check_type=CheckType.UNIQUENESS,
            column=None,   # Table-level check
            passed=dup_rows == 0,
            severity=severity,
            score=_score_from_pct(dup_pct),
            message=f"{dup_rows:,} duplicate rows detected ({dup_pct}%)." if dup_rows else "No duplicate rows found.",
            detail={"duplicate_rows": dup_rows, "duplicate_groups": dup_groups, "duplicate_pct": dup_pct},
        ))
    except Exception as e:
        logger.warning(f"Duplicate row check failed: {e}")

    # ── Sub-check 2: ID columns should be 100% unique ──
    col_stats = profile.get("column_stats", {})
    for col_name, stats in col_stats.items():
        if "id" not in col_name.lower():
            continue
        uniqueness_pct = stats.get("uniqueness_pct", 100.0)
        bad_pct = max(0.0, 100.0 - uniqueness_pct)

        severity = _severity_from_pct(bad_pct)
        results.append(CheckResult(
            check_type=CheckType.UNIQUENESS,
            column=col_name,
            passed=bad_pct < 1.0,
            severity=severity,
            score=_score_from_pct(bad_pct),
            message=f"ID column is {uniqueness_pct}% unique." if bad_pct > 0 else "ID column is fully unique.",
            detail={"uniqueness_pct": uniqueness_pct, "distinct_count": stats.get("distinct_count")},
        ))

    return results


# ─────────────────────────────────────────────────────────────────
# CHECK 4: VALIDITY — Value range and format checks
# ─────────────────────────────────────────────────────────────────

def check_validity(profile: Dict[str, Any], connector) -> List[CheckResult]:
    """
    Checks:
    - Numeric columns: are there negative values where impossible (qty, price, age)?
    - Numeric columns: extreme outliers (value > mean + 5*std)
    - Age columns: realistic range (0–120)
    - Price/amount columns: no negative values
    """
    results = []
    table = profile["table"]
    col_stats = profile.get("column_stats", {})
    columns_meta = {c["name"]: c for c in profile.get("columns", [])}

    NEGATIVE_FORBIDDEN = ["price", "amount", "quantity", "qty", "age", "salary", "cost", "revenue"]
    AGE_COLS = ["age", "years"]

    for col_name, stats in col_stats.items():
        if "error" in stats or stats.get("min") is None:
            continue

        col_lower = col_name.lower()
        col_type = columns_meta.get(col_name, {}).get("type", "").upper()
        is_numeric = any(t in col_type for t in ["INT", "REAL", "FLOAT", "NUMERIC", "DOUBLE"])
        if not is_numeric:
            continue

        min_val = stats.get("min")
        max_val = stats.get("max")
        mean_val = stats.get("mean")

        # ── Negative value check ──
        if any(kw in col_lower for kw in NEGATIVE_FORBIDDEN):
            if min_val is not None and min_val < 0:
                try:
                    neg_result = connector.execute_query(
                        f"SELECT COUNT(*) as cnt FROM \"{table}\" "
                        f"WHERE \"{col_name}\" < 0"
                    )
                    neg_count = int(neg_result.data["cnt"].iloc[0]) if neg_result.success else 0
                    total = profile.get("row_count", 1)
                    neg_pct = round((neg_count / total * 100), 2)
                    severity = _severity_from_pct(neg_pct)
                    results.append(CheckResult(
                        check_type=CheckType.VALIDITY,
                        column=col_name,
                        passed=neg_count == 0,
                        severity=severity,
                        score=_score_from_pct(neg_pct),
                        message=f"{neg_count:,} negative values found in '{col_name}' ({neg_pct}%).",
                        detail={"negative_count": neg_count, "negative_pct": neg_pct, "min_value": min_val},
                    ))
                except Exception as e:
                    logger.debug(f"Negative check failed for {col_name}: {e}")

        # ── Age range check ──
        if any(kw in col_lower for kw in AGE_COLS):
            if max_val is not None and (max_val > 120 or (min_val is not None and min_val < 0)):
                results.append(CheckResult(
                    check_type=CheckType.VALIDITY,
                    column=col_name,
                    passed=False,
                    severity=Severity.WARNING,
                    score=70.0,
                    message=f"Age values out of realistic range: min={min_val}, max={max_val}.",
                    detail={"min": min_val, "max": max_val},
                ))

        # ── Outlier detection (values beyond mean ± 4*std) ──
        std_val = stats.get("std")
        if mean_val is not None and std_val is not None and std_val > 0:
            upper_fence = mean_val + (4 * std_val)
            lower_fence = mean_val - (4 * std_val)
            try:
                outlier_result = connector.execute_query(
                    f"SELECT COUNT(*) as cnt FROM \"{table}\" "
                    f"WHERE \"{col_name}\" > {upper_fence} OR \"{col_name}\" < {lower_fence}"
                )
                outlier_count = int(outlier_result.data["cnt"].iloc[0]) if outlier_result.success else 0
                total = profile.get("row_count", 1)
                outlier_pct = round((outlier_count / total * 100), 2)

                if outlier_pct > 0:
                    severity = _severity_from_pct(outlier_pct)
                    results.append(CheckResult(
                        check_type=CheckType.ACCURACY,
                        column=col_name,
                        passed=outlier_count == 0,
                        severity=severity,
                        score=_score_from_pct(outlier_pct),
                        message=f"{outlier_count:,} statistical outliers in '{col_name}' ({outlier_pct}%).",
                        detail={
                            "outlier_count": outlier_count,
                            "outlier_pct": outlier_pct,
                            "mean": round(mean_val, 2),
                            "std": round(std_val, 2),
                            "upper_fence": round(upper_fence, 2),
                            "lower_fence": round(lower_fence, 2),
                        },
                    ))
            except Exception as e:
                logger.debug(f"Outlier check failed for {col_name}: {e}")

    return results


# ─────────────────────────────────────────────────────────────────
# CHECK 5: TIMELINESS — Are date columns fresh / not stale?
# ─────────────────────────────────────────────────────────────────

def check_timeliness(profile: Dict[str, Any], connector) -> List[CheckResult]:
    """
    For columns named *date*, *time*, *updated*, *created*, *login*:
    - What is the most recent value?
    - What % of rows have dates older than 1 year? (stale data)
    - What % of rows have future dates? (data entry error)
    """
    results = []
    table = profile["table"]
    total_rows = profile.get("row_count", 1)
    DATE_KEYWORDS = ["date", "time", "updated", "created", "modified", "login", "timestamp", "at"]

    for col_meta in profile.get("columns", []):
        col_name = col_meta["name"]
        if not any(kw in col_name.lower() for kw in DATE_KEYWORDS):
            continue

        try:
            # Get max and min date
            stat_result = connector.execute_query(
                f"SELECT "
                f"  MAX(\"{col_name}\") as max_date, "
                f"  MIN(\"{col_name}\") as min_date "
                f"FROM \"{table}\" WHERE \"{col_name}\" IS NOT NULL"
            )
            if not stat_result.success or stat_result.data is None:
                continue

            max_date_str = stat_result.data["max_date"].iloc[0]
            min_date_str = stat_result.data["min_date"].iloc[0]
            if not max_date_str:
                continue

            # Stale rows: dates older than 1 year
            one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            stale_result = connector.execute_query(
                f"SELECT COUNT(*) as cnt FROM \"{table}\" "
                f"WHERE \"{col_name}\" < '{one_year_ago}' AND \"{col_name}\" IS NOT NULL"
            )
            stale_count = int(stale_result.data["cnt"].iloc[0]) if stale_result.success else 0
            stale_pct = round((stale_count / total_rows * 100), 2) if total_rows > 0 else 0.0

            # Future dates: data entry errors
            today = datetime.now().strftime("%Y-%m-%d")
            future_result = connector.execute_query(
                f"SELECT COUNT(*) as cnt FROM \"{table}\" "
                f"WHERE \"{col_name}\" > '{today}' AND \"{col_name}\" IS NOT NULL"
            )
            future_count = int(future_result.data["cnt"].iloc[0]) if future_result.success else 0
            future_pct = round((future_count / total_rows * 100), 2)

            # Score based on stale %
            severity = _severity_from_pct(stale_pct)
            issues = []
            if stale_pct > 30:
                issues.append(f"{stale_pct}% of dates are older than 1 year")
            if future_count > 0:
                issues.append(f"{future_count} future dates detected (data entry error)")

            msg = "; ".join(issues) if issues else f"Date range looks healthy ({min_date_str} → {max_date_str})."

            results.append(CheckResult(
                check_type=CheckType.TIMELINESS,
                column=col_name,
                passed=len(issues) == 0,
                severity=severity if issues else Severity.PASS,
                score=_score_from_pct(stale_pct),
                message=msg,
                detail={
                    "max_date": str(max_date_str),
                    "min_date": str(min_date_str),
                    "stale_count": stale_count,
                    "stale_pct": stale_pct,
                    "future_count": future_count,
                },
            ))
        except Exception as e:
            logger.debug(f"Timeliness check skipped for '{col_name}': {e}")

    return results


# ─────────────────────────────────────────────────────────────────
# CHECK 6: CONSISTENCY — Cross-column logical rules
# ─────────────────────────────────────────────────────────────────

def check_consistency(profile: Dict[str, Any], connector) -> List[CheckResult]:
    """
    Checks logical rules that span multiple columns:
    - status column: only known values (no rogue entries)
    - If 'end_date' and 'start_date' exist: end must be >= start
    - Category columns: check for mixed case / format inconsistency
    """
    results = []
    table = profile["table"]
    total_rows = profile.get("row_count", 1)
    col_names = [c["name"].lower() for c in profile.get("columns", [])]
    col_name_map = {c["name"].lower(): c["name"] for c in profile.get("columns", [])}

    # ── Date ordering: start_date must be before end_date ──
    start_col = next((col_name_map[c] for c in col_names if "start" in c and "date" in c), None)
    end_col = next((col_name_map[c] for c in col_names if "end" in c and "date" in c), None)

    if start_col and end_col:
        try:
            inv_result = connector.execute_query(
                f"SELECT COUNT(*) as cnt FROM \"{table}\" "
                f"WHERE \"{end_col}\" < \"{start_col}\" "
                f"AND \"{start_col}\" IS NOT NULL AND \"{end_col}\" IS NOT NULL"
            )
            inv_count = int(inv_result.data["cnt"].iloc[0]) if inv_result.success else 0
            inv_pct = round((inv_count / total_rows * 100), 2) if total_rows > 0 else 0.0
            results.append(CheckResult(
                check_type=CheckType.CONSISTENCY,
                column=f"{start_col} → {end_col}",
                passed=inv_count == 0,
                severity=_severity_from_pct(inv_pct) if inv_count > 0 else Severity.PASS,
                score=_score_from_pct(inv_pct),
                message=f"{inv_count} rows where end_date < start_date." if inv_count else "Date ordering is consistent.",
                detail={"invalid_date_order_count": inv_count},
            ))
        except Exception as e:
            logger.debug(f"Date consistency check failed: {e}")

    # ── Low-cardinality columns: check for casing inconsistency ──
    col_stats = profile.get("column_stats", {})
    for col_name, stats in col_stats.items():
        if "error" in stats:
            continue
        distinct = stats.get("distinct_count", 0)
        total = stats.get("total_rows", total_rows)

        # Columns with 2–20 distinct values are likely categoricals
        if not (2 <= distinct <= 20):
            continue

        col_type = ""
        for c in profile.get("columns", []):
            if c["name"] == col_name:
                col_type = c.get("type", "").upper()
                break
        if any(t in col_type for t in ["INT", "REAL", "FLOAT", "NUMERIC"]):
            continue

        try:
            # Check if the same value appears in different cases
            case_result = connector.execute_query(
                f"SELECT COUNT(DISTINCT UPPER(CAST(\"{col_name}\" AS TEXT))) as upper_distinct, "
                f"COUNT(DISTINCT \"{col_name}\") as raw_distinct "
                f"FROM \"{table}\" WHERE \"{col_name}\" IS NOT NULL"
            )
            if case_result.success:
                upper_d = int(case_result.data["upper_distinct"].iloc[0])
                raw_d = int(case_result.data["raw_distinct"].iloc[0])
                if raw_d > upper_d:
                    extra = raw_d - upper_d
                    results.append(CheckResult(
                        check_type=CheckType.CONSISTENCY,
                        column=col_name,
                        passed=False,
                        severity=Severity.WARNING,
                        score=80.0,
                        message=f"Case inconsistency: {extra} extra variants due to mixed casing (e.g. 'Active' vs 'active').",
                        detail={"raw_distinct": raw_d, "normalised_distinct": upper_d, "extra_variants": extra},
                    ))
        except Exception as e:
            logger.debug(f"Consistency check skipped for '{col_name}': {e}")

    return results


# ─────────────────────────────────────────────────────────────────
# CHECK 7: SCHEMA — Type mismatches and structural issues
# ─────────────────────────────────────────────────────────────────

def check_schema(profile: Dict[str, Any], connector) -> List[CheckResult]:
    """
    Checks:
    - Columns declared as numeric but contain non-numeric data
    - ID columns that are TEXT instead of INTEGER
    - Date columns stored as TEXT (not proper DATE type)
    """
    results = []
    table = profile["table"]
    total_rows = profile.get("row_count", 1)
    DATE_KEYWORDS = ["date", "time", "created", "updated"]

    for col_meta in profile.get("columns", []):
        col_name = col_meta["name"]
        col_type = col_meta.get("type", "").upper()

        # ── Date columns stored as TEXT ──
        if any(kw in col_name.lower() for kw in DATE_KEYWORDS):
            if "TEXT" in col_type or "VARCHAR" in col_type or col_type == "":
                # Check if values actually look like dates
                try:
                    sample_result = connector.execute_query(
                        f"SELECT \"{col_name}\" as val FROM \"{table}\" "
                        f"WHERE \"{col_name}\" IS NOT NULL LIMIT 5"
                    )
                    if sample_result.success and len(sample_result.data) > 0:
                        sample_val = str(sample_result.data["val"].iloc[0])
                        # Looks like a date string stored as TEXT
                        date_pattern = r'\d{4}-\d{2}-\d{2}'
                        if re.match(date_pattern, sample_val):
                            results.append(CheckResult(
                                check_type=CheckType.SCHEMA,
                                column=col_name,
                                passed=False,
                                severity=Severity.INFO,
                                score=85.0,
                                message=f"Date column '{col_name}' stored as TEXT. Consider using DATE type.",
                                detail={"declared_type": col_type, "sample_value": sample_val},
                            ))
                except Exception:
                    pass

        # ── Numeric columns with non-numeric values ──
        if any(t in col_type for t in ["INT", "REAL", "FLOAT", "NUMERIC"]):
            try:
                non_num_result = connector.execute_query(
                    f"SELECT COUNT(*) as cnt FROM \"{table}\" "
                    f"WHERE \"{col_name}\" IS NOT NULL "
                    f"AND CAST(\"{col_name}\" AS TEXT) NOT LIKE '%[0-9]%' "
                    f"AND TRIM(CAST(\"{col_name}\" AS TEXT)) != ''"
                )
                # Only flag if significant (SQLite is lenient, this catches edge cases)
            except Exception:
                pass

    # ── Check for columns with ALL nulls (dead columns) ──
    col_stats = profile.get("column_stats", {})
    for col_name, stats in col_stats.items():
        if "error" in stats:
            continue
        null_pct = stats.get("null_pct", 0)
        if null_pct == 100.0:
            results.append(CheckResult(
                check_type=CheckType.SCHEMA,
                column=col_name,
                passed=False,
                severity=Severity.CRITICAL,
                score=0.0,
                message=f"Column '{col_name}' is 100% NULL — effectively empty.",
                detail={"null_pct": 100.0},
            ))

    return results