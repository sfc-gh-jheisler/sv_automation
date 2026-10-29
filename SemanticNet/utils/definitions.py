"""Definition writer: batch save with versioning, history retrieval, revert."""

from __future__ import annotations

SEMANTICLOG_FQN = "SEMANTIC_NET.SEMANTICS.SEMANTICLOG"


def batch_save(session, items: list[dict], write_mode: str) -> int:
    """Save a batch of definitions with versioning.

    Each item: {
        "database": str,
        "schema": str,
        "table": str,
        "column": str | None,   -- None for table-level
        "object_type": str,     -- TABLE, VIEW
        "raw_input": str,
        "definition_text": str,
    }

    Returns the number of definitions saved.
    """
    saved = 0
    for item in items:
        db = item["database"].replace("'", "''")
        sch = item["schema"].replace("'", "''")
        tbl = item["table"].replace("'", "''")
        col = item.get("column")
        obj_type = item.get("object_type", "TABLE")
        raw = item["raw_input"].replace("'", "''")
        defn = item["definition_text"].replace("'", "''")

        col_match = f"AND UPPER(COLUMN_NAME) = UPPER('{col.replace(chr(39), chr(39)+chr(39))}')" if col else "AND COLUMN_NAME IS NULL"
        col_val = f"'{col.replace(chr(39), chr(39)+chr(39))}'" if col else "NULL"

        # Get current max version
        version_sql = f"""
        SELECT COALESCE(MAX(VERSION), 0) AS MAX_VER
        FROM {SEMANTICLOG_FQN}
        WHERE UPPER(OBJECT_DATABASE) = UPPER('{db}')
          AND UPPER(OBJECT_SCHEMA)   = UPPER('{sch}')
          AND UPPER(OBJECT_NAME)     = UPPER('{tbl}')
          {col_match}
        """
        rows = session.sql(version_sql).collect()
        max_ver = rows[0]["MAX_VER"] if rows else 0
        new_ver = max_ver + 1

        # Flip previous current to not-current
        update_sql = f"""
        UPDATE {SEMANTICLOG_FQN}
        SET IS_CURRENT = FALSE
        WHERE UPPER(OBJECT_DATABASE) = UPPER('{db}')
          AND UPPER(OBJECT_SCHEMA)   = UPPER('{sch}')
          AND UPPER(OBJECT_NAME)     = UPPER('{tbl}')
          {col_match}
          AND IS_CURRENT = TRUE
        """
        session.sql(update_sql).collect()

        synonyms = (item.get("synonyms") or "").replace("'", "''")

        # Insert new version
        insert_sql = f"""
        INSERT INTO {SEMANTICLOG_FQN}
            (OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME, COLUMN_NAME,
             OBJECT_TYPE, RAW_INPUT, DEFINITION_TEXT, VERSION, IS_CURRENT,
             WRITE_MODE, METADATA_SYNCED, SYNONYMS)
        VALUES
            ('{db}', '{sch}', '{tbl}', {col_val},
             '{obj_type}', '{raw}', '{defn}', {new_ver}, TRUE,
             '{write_mode}', {_synced_value(write_mode)}, '{synonyms}')
        """
        session.sql(insert_sql).collect()

        # Write metadata comment if requested
        if write_mode in ("METADATA_ONLY", "BOTH"):
            _write_comment(session, db, sch, tbl, col, defn)

        saved += 1

    return saved


def get_definition_history(
    session, database: str, schema: str, table: str, column: str | None
) -> list[dict]:
    """All versions for an object, newest first."""
    col_match = (
        f"AND UPPER(COLUMN_NAME) = UPPER('{column}')" if column
        else "AND COLUMN_NAME IS NULL"
    )
    sql = f"""
    SELECT DEFINITION_ID, VERSION, RAW_INPUT, DEFINITION_TEXT,
           WRITE_MODE, IS_CURRENT, CREATED_BY, CREATED_AT
    FROM {SEMANTICLOG_FQN}
    WHERE UPPER(OBJECT_DATABASE) = UPPER('{database}')
      AND UPPER(OBJECT_SCHEMA)   = UPPER('{schema}')
      AND UPPER(OBJECT_NAME)     = UPPER('{table}')
      {col_match}
    ORDER BY VERSION DESC
    """
    return session.sql(sql).collect()


def get_current_definition(
    session, database: str, schema: str, table: str, column: str | None
) -> dict | None:
    """Get the current definition for an object, or None."""
    col_match = (
        f"AND UPPER(COLUMN_NAME) = UPPER('{column}')" if column
        else "AND COLUMN_NAME IS NULL"
    )
    sql = f"""
    SELECT DEFINITION_ID, VERSION, RAW_INPUT, DEFINITION_TEXT,
           WRITE_MODE, CREATED_BY, CREATED_AT
    FROM {SEMANTICLOG_FQN}
    WHERE UPPER(OBJECT_DATABASE) = UPPER('{database}')
      AND UPPER(OBJECT_SCHEMA)   = UPPER('{schema}')
      AND UPPER(OBJECT_NAME)     = UPPER('{table}')
      {col_match}
      AND IS_CURRENT = TRUE
    """
    rows = session.sql(sql).collect()
    return rows[0] if rows else None


def revert_to_version(session, definition_id: int) -> bool:
    """Create a new current version with the text from an older version."""
    sql = f"""
    SELECT OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME, COLUMN_NAME,
           OBJECT_TYPE, RAW_INPUT, DEFINITION_TEXT, WRITE_MODE
    FROM {SEMANTICLOG_FQN}
    WHERE DEFINITION_ID = {definition_id}
    """
    rows = session.sql(sql).collect()
    if not rows:
        return False

    old = rows[0]
    item = {
        "database": old["OBJECT_DATABASE"],
        "schema": old["OBJECT_SCHEMA"],
        "table": old["OBJECT_NAME"],
        "column": old["COLUMN_NAME"],
        "object_type": old["OBJECT_TYPE"],
        "raw_input": old["RAW_INPUT"],
        "definition_text": old["DEFINITION_TEXT"],
    }
    batch_save(session, [item], old["WRITE_MODE"])
    return True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _synced_value(write_mode: str) -> str:
    if write_mode == "TABLE_ONLY":
        return "NULL"
    return "TRUE"


def _write_comment(session, db: str, schema: str, table: str, column: str | None, text: str):
    """Execute COMMENT ON TABLE or COMMENT ON COLUMN."""
    escaped = text.replace("'", "''")
    if column:
        sql = f"COMMENT ON COLUMN {db}.{schema}.{table}.{column} IS '{escaped}'"
    else:
        sql = f"COMMENT ON TABLE {db}.{schema}.{table} IS '{escaped}'"
    try:
        session.sql(sql).collect()
    except Exception:
        pass  # Log but don't fail the batch
