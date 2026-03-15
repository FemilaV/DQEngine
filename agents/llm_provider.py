"""
LLM Provider — Abstraction over OpenAI and Groq.

Priority order:
  1. OpenAI  (if OPENAI_API_KEY is set)
  2. Groq    (if GROQ_API_KEY is set — free tier available)
  3. Mock    (no API key — returns template SQL for testing)
"""

import os
import re
import json
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# Detect which provider is available
# ─────────────────────────────────────────────────────────────────

def _get_provider() -> str:
    if os.getenv("OPENAI_API_KEY"):
        return "openai"
    if os.getenv("GROQ_API_KEY"):
        return "groq"
    return "mock"


def get_active_provider() -> str:
    return _get_provider()


# ─────────────────────────────────────────────────────────────────
# OpenAI
# ─────────────────────────────────────────────────────────────────

def _call_openai(system_prompt: str, user_prompt: str) -> Tuple[str, str, int]:
    """Returns (response_text, model_name, tokens_used)"""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0,          # Deterministic SQL
            max_tokens=800,
        )
        text = response.choices[0].message.content
        tokens = response.usage.total_tokens if response.usage else 0
        return text, "gpt-4o-mini", tokens
    except ImportError:
        raise RuntimeError("openai package not installed. Run: pip install openai")


# ─────────────────────────────────────────────────────────────────
# Groq (free)
# ─────────────────────────────────────────────────────────────────

def _call_groq(system_prompt: str, user_prompt: str) -> Tuple[str, str, int]:
    """Returns (response_text, model_name, tokens_used)"""
    try:
        from groq import Groq
        client = Groq(api_key=os.getenv("GROQ_API_KEY"))
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",   # Fast, free, accurate for SQL
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0,
            max_tokens=800,
        )
        text = response.choices[0].message.content
        tokens = response.usage.total_tokens if response.usage else 0
        return text, "llama-3.1-8b-instant (Groq)", tokens
    except ImportError:
        raise RuntimeError("groq package not installed. Run: pip install groq")


# ─────────────────────────────────────────────────────────────────
# Mock provider — no API key needed, uses rule-based SQL templates
# ─────────────────────────────────────────────────────────────────

_MOCK_TEMPLATES = {
    "null":        ('SELECT * FROM "{table}" WHERE "{col}" IS NULL LIMIT {limit}',
                    "Returns rows where {col} is missing."),
    "duplicate":   ('SELECT *, COUNT(*) as dup_count FROM "{table}" GROUP BY {all_cols} HAVING COUNT(*) > 1 LIMIT {limit}',
                    "Returns duplicate rows in {table}."),
    "count":       ('SELECT COUNT(*) as total_rows FROM "{table}"',
                    "Counts all rows in {table}."),
    "show":        ('SELECT * FROM "{table}" LIMIT {limit}',
                    "Shows first {limit} rows from {table}."),
    "average":     ('SELECT AVG("{col}") as average_{col} FROM "{table}" WHERE "{col}" IS NOT NULL',
                    "Calculates average of {col} in {table}."),
    "max":         ('SELECT MAX("{col}") as max_{col}, MIN("{col}") as min_{col} FROM "{table}"',
                    "Gets max and min of {col} in {table}."),
    "group":       ('SELECT "{col}", COUNT(*) as count FROM "{table}" GROUP BY "{col}" ORDER BY count DESC LIMIT {limit}',
                    "Groups {table} by {col} and counts each group."),
}

def _call_mock(user_prompt: str, schema_context: str) -> Tuple[str, str, int]:
    """
    Rule-based SQL generation — no LLM needed.
    Covers the most common DQ query patterns.
    """
    prompt_lower = user_prompt.lower()
    
    # Extract table name from schema context
    table = "unknown_table"
    for line in schema_context.split("\n"):
        if line.strip().startswith("TABLE:"):
            table = line.split("TABLE:")[1].strip().split()[0]
            break

    # Extract column name hints from prompt
    col = "id"
    for line in schema_context.split("\n"):
        if "column" in line.lower() or "|" in line:
            parts = line.split("|") if "|" in line else []
            if parts:
                candidate = parts[0].strip().lower()
                if any(kw in prompt_lower for kw in [candidate]):
                    col = candidate
                    break
    
    # Pick best matching template
    limit = 100
    if "null" in prompt_lower or "missing" in prompt_lower or "empty" in prompt_lower:
        # Find which column is mentioned
        for line in schema_context.split("\n"):
            if "|" in line:
                parts = [p.strip() for p in line.split("|")]
                if parts and any(word in prompt_lower for word in parts[0].lower().split("_")):
                    col = parts[0]
                    break
        sql = f'SELECT * FROM "{table}" WHERE "{col}" IS NULL LIMIT {limit}'
        explanation = f"Returns rows from '{table}' where '{col}' has missing values."

    elif "duplicate" in prompt_lower or "duplicat" in prompt_lower:
        cols_query = f'SELECT * FROM "{table}" LIMIT 1'
        sql = (f'SELECT *, COUNT(*) as duplicate_count FROM "{table}" '
               f'GROUP BY * HAVING COUNT(*) > 1 LIMIT {limit}')
        explanation = f"Finds duplicate rows in the '{table}' table."

    elif "count" in prompt_lower or "how many" in prompt_lower or "total" in prompt_lower:
        sql = f'SELECT COUNT(*) as total_rows FROM "{table}"'
        explanation = f"Counts total rows in '{table}'."

    elif "average" in prompt_lower or "avg" in prompt_lower or "mean" in prompt_lower:
        for line in schema_context.split("\n"):
            if "|" in line and any(t in line.upper() for t in ["REAL", "INT", "FLOAT", "NUMERIC"]):
                col = line.split("|")[0].strip()
                break
        sql = f'SELECT AVG("{col}") as average FROM "{table}" WHERE "{col}" IS NOT NULL'
        explanation = f"Calculates the average of '{col}' in '{table}'."

    elif "group" in prompt_lower or "by " in prompt_lower or "each" in prompt_lower:
        for line in schema_context.split("\n"):
            if "|" in line and any(t in line.upper() for t in ["TEXT", "VARCHAR"]):
                col = line.split("|")[0].strip()
                break
        sql = f'SELECT "{col}", COUNT(*) as count FROM "{table}" GROUP BY "{col}" ORDER BY count DESC LIMIT {limit}'
        explanation = f"Groups '{table}' by '{col}' and counts each group."

    elif "max" in prompt_lower or "highest" in prompt_lower or "largest" in prompt_lower:
        for line in schema_context.split("\n"):
            if "|" in line and any(t in line.upper() for t in ["REAL", "INT", "FLOAT"]):
                col = line.split("|")[0].strip()
                break
        sql = f'SELECT MAX("{col}") as max_val, MIN("{col}") as min_val FROM "{table}"'
        explanation = f"Gets the maximum and minimum of '{col}' in '{table}'."

    elif "negative" in prompt_lower or "invalid" in prompt_lower:
        for line in schema_context.split("\n"):
            if "|" in line and any(t in line.upper() for t in ["REAL", "INT", "FLOAT"]):
                col = line.split("|")[0].strip()
                break
        sql = f'SELECT * FROM "{table}" WHERE "{col}" < 0 LIMIT {limit}'
        explanation = f"Finds rows with negative values in '{col}'."

    else:
        sql = f'SELECT * FROM "{table}" LIMIT {limit}'
        explanation = f"Shows the first {limit} rows from '{table}'."

    # Wrap in the expected JSON format
    response = json.dumps({
        "sql": sql,
        "explanation": explanation,
        "tables_used": [table],
        "confidence": 0.7,
        "warnings": ["Using mock provider — set OPENAI_API_KEY or GROQ_API_KEY for full NL→SQL capability"]
    })
    return response, "mock (no API key)", 0


# ─────────────────────────────────────────────────────────────────
# Unified call — automatic fallback chain: OpenAI → Groq → Mock
# If OpenAI fails (quota/billing/rate limit), falls back to Groq.
# If Groq also fails, falls back to Mock. Never crashes the agent.
# ─────────────────────────────────────────────────────────────────

def call_llm(system_prompt: str, user_prompt: str, schema_context: str = "") -> Tuple[str, str, int]:
    """
    Call LLM with automatic fallback chain: OpenAI → Groq → Mock.
    Returns: (raw_response_text, model_name, tokens_used)
    """
    errors = []

    # Try OpenAI first
    if os.getenv("OPENAI_API_KEY"):
        try:
            logger.info("[LLM] Trying OpenAI...")
            result = _call_openai(system_prompt, user_prompt)
            logger.info("[LLM] OpenAI succeeded.")
            return result
        except Exception as e:
            error_msg = str(e)
            errors.append(f"OpenAI: {error_msg}")
            if any(k in error_msg.lower() for k in ["quota", "billing", "insufficient", "rate limit", "429"]):
                logger.warning("[LLM] OpenAI quota/billing issue — falling back to Groq.")
            else:
                logger.warning(f"[LLM] OpenAI error — falling back to Groq. ({error_msg})")

    # Try Groq second
    if os.getenv("GROQ_API_KEY"):
        try:
            logger.info("[LLM] Trying Groq...")
            result = _call_groq(system_prompt, user_prompt)
            logger.info("[LLM] Groq succeeded.")
            return result
        except Exception as e:
            errors.append(f"Groq: {str(e)}")
            logger.warning(f"[LLM] Groq error — falling back to mock. ({e})")

    # Final fallback: Mock
    logger.warning(f"[LLM] All providers failed or no keys set. Using mock. Errors: {errors}")
    return _call_mock(user_prompt, schema_context)


def parse_llm_json_response(raw: str) -> dict:
    """
    Safely parse JSON from LLM response.
    LLMs often wrap JSON in markdown code blocks — this strips them.
    """
    # Strip markdown code fences if present
    cleaned = re.sub(r"```(?:json)?", "", raw).strip().strip("`").strip()
    
    # Find the JSON object (in case there's surrounding text)
    match = re.search(r'\{.*\}', cleaned, re.DOTALL)
    if match:
        cleaned = match.group(0)

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # Last resort — extract SQL manually
        sql_match = re.search(r'SELECT.*?;', cleaned, re.IGNORECASE | re.DOTALL)
        sql = sql_match.group(0) if sql_match else "SELECT 1"
        return {
            "sql": sql,
            "explanation": "Could not parse full LLM response.",
            "tables_used": [],
            "confidence": 0.3,
            "warnings": ["JSON parse failed — SQL extracted via regex"],
        }