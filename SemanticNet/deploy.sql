-- =============================================================================
-- SemanticNet Deploy Script
-- Creates all infrastructure for the SemanticNet metadata capture application.
-- Run this script once (deploy.sh does this for you), then upload app files.
-- =============================================================================

-- 1. Database and Schema
CREATE DATABASE IF NOT EXISTS SEMANTIC_NET;
CREATE SCHEMA IF NOT EXISTS SEMANTIC_NET.SEMANTICS;
USE SCHEMA SEMANTIC_NET.SEMANTICS;

-- 2. Versioned definitions log
CREATE TABLE IF NOT EXISTS SEMANTIC_NET.SEMANTICS.SEMANTICLOG (
    DEFINITION_ID       NUMBER AUTOINCREMENT PRIMARY KEY,
    OBJECT_DATABASE     VARCHAR NOT NULL,
    OBJECT_SCHEMA       VARCHAR NOT NULL,
    OBJECT_NAME         VARCHAR NOT NULL,
    COLUMN_NAME         VARCHAR,              -- NULL = table-level description
    OBJECT_TYPE         VARCHAR NOT NULL,      -- TABLE, VIEW
    RAW_INPUT           TEXT NOT NULL,          -- Verbatim user input (typed or dictated)
    DEFINITION_TEXT     TEXT NOT NULL,          -- LLM-optimized or user-edited description
    VERSION             NUMBER NOT NULL DEFAULT 1,
    IS_CURRENT          BOOLEAN NOT NULL DEFAULT TRUE,
    WRITE_MODE          VARCHAR NOT NULL,      -- TABLE_ONLY, METADATA_ONLY, BOTH
    METADATA_SYNCED     BOOLEAN DEFAULT NULL,  -- NULL if TABLE_ONLY; TRUE/FALSE otherwise
    CREATED_BY          VARCHAR NOT NULL DEFAULT CURRENT_USER(),
    CREATED_AT          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    SYNONYMS            VARCHAR DEFAULT NULL     -- Comma-separated alternate names for SV generation
);

-- 3. Internal stage for SiS app files
CREATE STAGE IF NOT EXISTS SEMANTIC_NET.SEMANTICS.SEMANTICNET_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- 4. The Streamlit app object is created by deploy.sh after the app files
--    are uploaded to the stage.
