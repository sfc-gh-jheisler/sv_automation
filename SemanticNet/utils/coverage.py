"""Coverage scanner: checks both INFORMATION_SCHEMA and SEMANTICLOG for descriptions."""

from __future__ import annotations

SEMANTICLOG_FQN = "SEMANTIC_NET.SEMANTICS.SEMANTICLOG"


def get_table_coverage(session, database: str, schema: str) -> list[dict]:
    """Per-table coverage stats.

    A column counts as 'described' if it has a metadata comment
    OR a current SEMANTICLOG entry (or both).
    """
    sql = f"""
    WITH cols AS (
        SELECT
            c.TABLE_CATALOG,
            c.TABLE_SCHEMA,
            c.TABLE_NAME,
            t.TABLE_TYPE,
            c.COLUMN_NAME,
            c.COMMENT AS METADATA_COMMENT
        FROM {database}.INFORMATION_SCHEMA.COLUMNS c
        JOIN {database}.INFORMATION_SCHEMA.TABLES t
            ON c.TABLE_CATALOG = t.TABLE_CATALOG
           AND c.TABLE_SCHEMA  = t.TABLE_SCHEMA
           AND c.TABLE_NAME    = t.TABLE_NAME
        WHERE t.TABLE_SCHEMA != 'INFORMATION_SCHEMA'
          AND t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
          AND c.TABLE_SCHEMA = '{schema}'
    ),
    defs AS (
        SELECT
            OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME, COLUMN_NAME,
            DEFINITION_TEXT, WRITE_MODE
        FROM {SEMANTICLOG_FQN}
        WHERE IS_CURRENT = TRUE AND COLUMN_NAME IS NOT NULL
    )
    SELECT
        c.TABLE_NAME,
        c.TABLE_TYPE,
        COUNT(*)                                                         AS TOTAL_COLUMNS,
        COUNT(CASE WHEN (c.METADATA_COMMENT IS NOT NULL AND c.METADATA_COMMENT != '')
                     OR d.DEFINITION_TEXT IS NOT NULL THEN 1 END)        AS DESCRIBED_COUNT,
        ROUND(COUNT(CASE WHEN (c.METADATA_COMMENT IS NOT NULL AND c.METADATA_COMMENT != '')
                          OR d.DEFINITION_TEXT IS NOT NULL THEN 1 END)
              * 100.0 / NULLIF(COUNT(*), 0), 1)                          AS COVERAGE_PCT,
        COUNT(CASE WHEN d.WRITE_MODE IN ('METADATA_ONLY', 'BOTH')
                    AND d.DEFINITION_TEXT != COALESCE(c.METADATA_COMMENT, '')
                   THEN 1 END)                                           AS DRIFT_COUNT
    FROM cols c
    LEFT JOIN defs d
        ON UPPER(d.OBJECT_DATABASE) = UPPER(c.TABLE_CATALOG)
       AND UPPER(d.OBJECT_SCHEMA)   = UPPER(c.TABLE_SCHEMA)
       AND UPPER(d.OBJECT_NAME)     = UPPER(c.TABLE_NAME)
       AND UPPER(d.COLUMN_NAME)     = UPPER(c.COLUMN_NAME)
    GROUP BY c.TABLE_NAME, c.TABLE_TYPE
    ORDER BY COVERAGE_PCT ASC, c.TABLE_NAME
    """
    return session.sql(sql).collect()


def get_column_detail(session, database: str, schema: str, table_name: str) -> list[dict]:
    """Per-column detail for a single table, including table-level description."""
    sql = f"""
    WITH col_info AS (
        -- Column-level rows
        SELECT
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            c.COMMENT                   AS METADATA_COMMENT,
            c.ORDINAL_POSITION          AS SORT_KEY
        FROM {database}.INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = '{schema}'
          AND c.TABLE_NAME   = '{table_name}'

        UNION ALL

        -- Table-level row (COLUMN_NAME = NULL)
        SELECT
            NULL                        AS COLUMN_NAME,
            NULL                        AS DATA_TYPE,
            NULL                        AS IS_NULLABLE,
            t.COMMENT                   AS METADATA_COMMENT,
            0                           AS SORT_KEY
        FROM {database}.INFORMATION_SCHEMA.TABLES t
        WHERE t.TABLE_SCHEMA = '{schema}'
          AND t.TABLE_NAME   = '{table_name}'
    ),
    defs AS (
        SELECT
            COLUMN_NAME, DEFINITION_TEXT, RAW_INPUT, VERSION, WRITE_MODE
        FROM {SEMANTICLOG_FQN}
        WHERE IS_CURRENT = TRUE
          AND UPPER(OBJECT_DATABASE) = UPPER('{database}')
          AND UPPER(OBJECT_SCHEMA)   = UPPER('{schema}')
          AND UPPER(OBJECT_NAME)     = UPPER('{table_name}')
    )
    SELECT
        ci.COLUMN_NAME,
        ci.DATA_TYPE,
        ci.IS_NULLABLE,
        ci.METADATA_COMMENT,
        d.DEFINITION_TEXT,
        d.RAW_INPUT,
        d.VERSION,
        CASE
            WHEN d.DEFINITION_TEXT IS NOT NULL AND ci.METADATA_COMMENT IS NOT NULL AND ci.METADATA_COMMENT != '' THEN 'BOTH'
            WHEN d.DEFINITION_TEXT IS NOT NULL THEN 'TABLE_ONLY'
            WHEN ci.METADATA_COMMENT IS NOT NULL AND ci.METADATA_COMMENT != '' THEN 'METADATA_ONLY'
            ELSE 'NONE'
        END AS COVERAGE_STATUS,
        CASE
            WHEN d.WRITE_MODE IN ('METADATA_ONLY', 'BOTH')
                 AND d.DEFINITION_TEXT != COALESCE(ci.METADATA_COMMENT, '')
            THEN TRUE ELSE FALSE
        END AS IS_OUT_OF_SYNC
    FROM col_info ci
    LEFT JOIN defs d
        ON COALESCE(UPPER(d.COLUMN_NAME), '___TABLE___')
         = COALESCE(UPPER(ci.COLUMN_NAME), '___TABLE___')
    ORDER BY ci.SORT_KEY
    """
    return session.sql(sql).collect()


def get_columns_for_tables(session, table_fqns: list[str]) -> dict[str, list[dict]]:
    """Fetch column name + data type for a list of fully-qualified tables.

    Used by the free-form parser to build context for the COMPLETE prompt.
    Returns {table_fqn: [{"column_name": ..., "data_type": ...}]}
    """
    if not table_fqns:
        return {}

    result: dict[str, list[dict]] = {}
    for fqn in table_fqns:
        parts = fqn.split(".")
        if len(parts) != 3:
            continue
        db, sch, tbl = parts
        sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM {db}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{sch}' AND TABLE_NAME = '{tbl}'
        ORDER BY ORDINAL_POSITION
        """
        rows = session.sql(sql).collect()
        result[fqn.upper()] = [
            {"column_name": r["COLUMN_NAME"], "data_type": r["DATA_TYPE"]}
            for r in rows
        ]
    return result


def get_all_tables_in_scope(session, database: str, schema: str) -> list[str]:
    """Return all table FQNs in a database.schema (for free-form reference matching)."""
    sql = f"""
    SELECT TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS TABLE_FQN
    FROM {database}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
      AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
      AND TABLE_SCHEMA = '{schema}'
    ORDER BY TABLE_NAME
    """
    rows = session.sql(sql).collect()
    return [r["TABLE_FQN"] for r in rows]
