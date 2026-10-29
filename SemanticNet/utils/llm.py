"""LLM module: structured batch optimize and free-form parse+match+optimize.

All calls use SNOWFLAKE.CORTEX.COMPLETE(). Auto-chunks at CHUNK_SIZE items.
"""

from __future__ import annotations

import json
import re

CHUNK_SIZE = 25
MODEL = "claude-sonnet-4-6"

# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

_OPTIMIZE_SYSTEM = """You are a data catalog description writer for Snowflake data objects.
Your job is to take raw descriptions written by data stewards and rewrite them
into clear, precise descriptions optimized for both human data consumers and
AI systems (semantic models, Cortex Analyst, Cortex Agents).

Rules:
- 1-2 sentences per description
- Preserve domain-specific terminology exactly as provided
- State the business meaning: what the object represents, not just its data type
- For columns: include data type semantics where they add meaning
- Do not invent information not present in the raw input
- Do not repeat the column name in the description
- Present tense, third person
- For each column, suggest 2-4 synonyms: alternate names a business user might use
  when asking questions (e.g., AUM_WITH_SAM -> "AUM", "assets under management", "total AUM")
- Return ONLY a JSON array, no commentary

Each item in the array: {"column_name": "...", "optimized": "...", "synonyms": ["alt1", "alt2"]}"""

_PARSE_SYSTEM = """You are a data catalog assistant. A data steward has provided a block of
natural language descriptions for Snowflake data objects. Your job is to:

1. Segment the text into individual descriptions
2. Match each segment to the correct column from the provided column lists
3. Identify any table-level descriptions (about the table itself, not a column)
4. Flag segments that cannot be confidently matched to any column

Matching rules:
- Match by column name similarity: "client id" matches CLIENT_ID,
  "onboarded date" matches ONBOARDED_DATE, "market val" matches MARKET_VALUE
- If the user specifies a table name, scope the match to that table's columns
- If no table is specified, match against all provided tables
- Set confidence: "high" if the column name is clearly referenced,
  "medium" if inferred from context, "low" if ambiguous
- Segments about "this table" or "the table" with no column reference are table-level

Return ONLY this JSON structure, no commentary:
{
  "matched": [
    {"database": "...", "schema": "...", "table": "...", "column": "...",
     "raw_segment": "...", "confidence": "high"}
  ],
  "table_level": [
    {"database": "...", "schema": "...", "table": "...", "raw_segment": "..."}
  ],
  "unmatched": ["segment text"]
}"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _call_complete(session, system_prompt: str, user_prompt: str) -> str:
    """Call CORTEX.COMPLETE and return the text response."""
    sys_escaped = system_prompt.replace("'", "''")
    user_escaped = user_prompt.replace("'", "''")
    sql = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        '{MODEL}',
        [
            {{'role': 'system', 'content': '{sys_escaped}'}},
            {{'role': 'user',   'content': '{user_escaped}'}}
        ],
        {{}}
    ):choices[0]:messages::STRING AS RESPONSE
    """
    rows = session.sql(sql).collect()
    return rows[0]["RESPONSE"] if rows else ""


def _extract_json_from_response(text: str):
    """Extract the first JSON array or object from an LLM response string."""
    if not isinstance(text, str):
        text = str(text)
    # Try to find JSON array or object
    for pattern in [r'\[[\s\S]*\]', r'\{[\s\S]*\}']:
        match = re.search(pattern, text)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                continue
    return None


def _chunk_list(items: list, size: int) -> list[list]:
    return [items[i:i + size] for i in range(0, len(items), size)]


# ---------------------------------------------------------------------------
# Path A: Structured batch optimize (column-by-column mode)
# ---------------------------------------------------------------------------

def batch_optimize(session, table_fqn: str, items: list[dict]) -> list[dict]:
    """Optimize a batch of structured descriptions for a single table.

    items: [{"column_name": str | None, "data_type": str | None, "raw_input": str}]
    Returns: [{"column_name": str | None, "raw_input": str, "optimized": str}]
    """
    if not items:
        return []

    results = []
    for chunk in _chunk_list(items, CHUNK_SIZE):
        payload = []
        for item in chunk:
            entry = {
                "column_name": item.get("column_name"),
                "data_type": item.get("data_type"),
                "raw": item["raw_input"],
            }
            payload.append(entry)

        user_prompt = f"""Table: {table_fqn}

Optimize these descriptions:
{json.dumps(payload, indent=2)}

Return a JSON array:
[{{"column_name": "...", "optimized": "...", "synonyms": ["alt1", "alt2"]}}, ...]"""

        response = _call_complete(session, _OPTIMIZE_SYSTEM, user_prompt)
        parsed = _extract_json_from_response(response)

        if isinstance(parsed, list):
            opt_map = {}
            syn_map = {}
            for r in parsed:
                key = (r.get("column_name") or "").upper()
                opt_map[key] = r.get("optimized", "")
                syn_map[key] = r.get("synonyms", [])
            for item in chunk:
                key = (item.get("column_name") or "").upper()
                syns = syn_map.get(key, [])
                results.append({
                    "column_name": item.get("column_name"),
                    "raw_input": item["raw_input"],
                    "optimized": opt_map.get(key, item["raw_input"]),
                    "synonyms": ", ".join(syns) if isinstance(syns, list) else str(syns or ""),
                })
        else:
            for item in chunk:
                results.append({
                    "column_name": item.get("column_name"),
                    "raw_input": item["raw_input"],
                    "optimized": item["raw_input"],
                    "synonyms": "",
                })

    return results


# ---------------------------------------------------------------------------
# Path B: Free-form parse + match + optimize (free-form mode)
# ---------------------------------------------------------------------------

def freeform_parse(
    session,
    free_text: str,
    scope_database: str,
    scope_schema: str,
    column_lists: dict[str, list[dict]],
) -> dict:
    """Parse free-form text against known column lists, then optimize.

    column_lists: {table_fqn: [{"column_name": ..., "data_type": ...}]}

    Returns:
    {
        "matched": [{"database", "schema", "table", "column", "data_type",
                      "raw_segment", "optimized", "confidence"}],
        "table_level": [{"database", "schema", "table", "raw_segment", "optimized"}],
        "unmatched": [str]
    }
    """
    # Step 1: Parse and match
    col_context_lines = []
    for fqn, cols in column_lists.items():
        col_context_lines.append(f"TABLE: {fqn}")
        for c in cols:
            col_context_lines.append(f"  - {c['column_name']} ({c['data_type']})")
    col_context = "\n".join(col_context_lines)

    parse_prompt = f"""Scope database: {scope_database}, schema: {scope_schema}

Available columns:
{col_context}

Free-form text from the data steward:
\"\"\"
{free_text}
\"\"\""""

    parse_response = _call_complete(session, _PARSE_SYSTEM, parse_prompt)
    parsed = _extract_json_from_response(parse_response)

    if not isinstance(parsed, dict):
        return {"matched": [], "table_level": [], "unmatched": [free_text]}

    matched_raw = parsed.get("matched", [])
    table_level_raw = parsed.get("table_level", [])
    unmatched = parsed.get("unmatched", [])

    # Enrich matched items with data_type from column_lists
    for item in matched_raw:
        fqn = f"{item.get('database', '')}.{item.get('schema', '')}.{item.get('table', '')}".upper()
        col_name = (item.get("column") or "").upper()
        item["data_type"] = None
        for c in column_lists.get(fqn, []):
            if c["column_name"].upper() == col_name:
                item["data_type"] = c["data_type"]
                break

    # Step 2: Optimize matched descriptions
    all_to_optimize = []
    for item in matched_raw:
        all_to_optimize.append({
            "table": f"{item.get('database')}.{item.get('schema')}.{item.get('table')}",
            "column": item.get("column"),
            "data_type": item.get("data_type"),
            "raw": item.get("raw_segment", ""),
        })
    for item in table_level_raw:
        all_to_optimize.append({
            "table": f"{item.get('database')}.{item.get('schema')}.{item.get('table')}",
            "column": None,
            "data_type": None,
            "raw": item.get("raw_segment", ""),
        })

    optimized_map = {}
    synonyms_map = {}
    if all_to_optimize:
        for chunk in _chunk_list(all_to_optimize, CHUNK_SIZE):
            opt_prompt = f"""Optimize these parsed descriptions:
{json.dumps(chunk, indent=2)}

Return a JSON array:
[{{"table": "...", "column": "...", "optimized": "...", "synonyms": ["alt1", "alt2"]}}, ...]"""

            opt_response = _call_complete(session, _OPTIMIZE_SYSTEM, opt_prompt)
            opt_parsed = _extract_json_from_response(opt_response)
            if isinstance(opt_parsed, list):
                for r in opt_parsed:
                    key = (r.get("table", "").upper(), (r.get("column") or "").upper())
                    optimized_map[key] = r.get("optimized", "")
                    syns = r.get("synonyms", [])
                    synonyms_map[key] = ", ".join(syns) if isinstance(syns, list) else str(syns or "")

    # Build final results
    final_matched = []
    for item in matched_raw:
        fqn = f"{item.get('database')}.{item.get('schema')}.{item.get('table')}".upper()
        col = (item.get("column") or "").upper()
        final_matched.append({
            "database": item.get("database", scope_database),
            "schema": item.get("schema", scope_schema),
            "table": item.get("table", ""),
            "column": item.get("column"),
            "data_type": item.get("data_type"),
            "raw_segment": item.get("raw_segment", ""),
            "optimized": optimized_map.get((fqn, col), item.get("raw_segment", "")),
            "synonyms": synonyms_map.get((fqn, col), ""),
            "confidence": item.get("confidence", "medium"),
        })

    final_table_level = []
    for item in table_level_raw:
        fqn = f"{item.get('database')}.{item.get('schema')}.{item.get('table')}".upper()
        final_table_level.append({
            "database": item.get("database", scope_database),
            "schema": item.get("schema", scope_schema),
            "table": item.get("table", ""),
            "raw_segment": item.get("raw_segment", ""),
            "optimized": optimized_map.get((fqn, ""), item.get("raw_segment", "")),
            "synonyms": synonyms_map.get((fqn, ""), ""),
        })

    return {
        "matched": final_matched,
        "table_level": final_table_level,
        "unmatched": unmatched,
    }
