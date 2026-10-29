#!/bin/bash
# =============================================================================
# SemanticNet Deploy Script
# Creates the infrastructure, uploads app files to the stage, and creates the
# Streamlit-in-Snowflake app.
#
# Usage (from the SemanticNet/ directory):
#   ./deploy.sh <snow_connection_name> <warehouse>
# =============================================================================
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 <snow_connection_name> <warehouse>" >&2
  exit 1
fi

CONN="$1"
WAREHOUSE="$2"
STAGE="@SEMANTIC_NET.SEMANTICS.SEMANTICNET_STAGE"

echo "=== Running DDL ==="
snow sql -c "$CONN" -f deploy.sql

echo "=== Uploading app files ==="
snow stage copy app.py "$STAGE" -c "$CONN" --overwrite
snow stage copy utils "$STAGE/utils" -c "$CONN" --overwrite

echo "=== Creating Streamlit app ==="
snow sql -c "$CONN" -q "CREATE OR REPLACE STREAMLIT SEMANTIC_NET.SEMANTICS.SEMANTICNET_APP ROOT_LOCATION = '$STAGE' MAIN_FILE = 'app.py' QUERY_WAREHOUSE = $WAREHOUSE"

echo "=== Done. Open the app in Snowsight: ==="
echo "    SEMANTIC_NET.SEMANTICS.SEMANTICNET_APP"
