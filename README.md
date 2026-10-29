# SV Automation

Automates creation and refresh of Snowflake Semantic Views (SVs) by mining query history,
clustering related tables, generating SV YAML via `SYSTEM$CORTEX_ANALYST_FAST_GENERATION`,
evaluating accuracy with Cortex Analyst, and integrating Cortex Search Services.

## Bring your own data

This repository does not include a sample dataset. The synthetic financial estate used for internal demos is kept local and excluded by `.gitignore`. To run the pipeline, point `config.yaml` at an existing database and schema in your account.

For useful results, the target scope should have:
- Tables with real query activity in `ACCOUNT_USAGE.QUERY_HISTORY` and `ACCESS_HISTORY`. Mining and clustering depend on it, so tables with no queries produce few or no semantic views.
- Recent activity. `ACCOUNT_USAGE` views lag by up to a few hours, so queries you just ran may not show up yet.

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure `config.yaml`

Update the placeholder values to match your environment:

```yaml
scope:
  database: YOUR_DATABASE    # database containing the source tables
  schema: YOUR_SCHEMA        # or null for full-database scan

snowflake:
  connection: YOUR_CONNECTION_NAME  # entry in ~/.snowflake/config.toml
  role: YOUR_ROLE
  warehouse: YOUR_WAREHOUSE
  target_database: YOUR_DATABASE    # where semantic views get created
  target_schema: PUBLIC             # schema for the created SVs
```

The `connection` value must match an entry under `[connections.<name>]` in `~/.snowflake/config.toml`. The code falls back to `~/.snowflake/connections.toml` for legacy configurations. Credentials are read from that local file.

### 3. Verify Snowflake connection

The role you specify needs access to:
- `SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY` and `QUERY_HISTORY` (Phase 1)
- `INFORMATION_SCHEMA.TABLES` and `COLUMNS` in your target database
- `SNOWFLAKE.CORE.GET_LINEAGE()` (Phase 3)
- `SYSTEM$CORTEX_ANALYST_FAST_GENERATION()` and `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML()` (Phase 4)
- Permissions to create Cortex Search Services (Phase 6)

## Prerequisites

- Python and the packages in `requirements.txt`
- The Cortex Code CLI (`cortex`) on `PATH`
- A Snowflake connection configured locally in `~/.snowflake/config.toml`
- The Snowflake CLI (`snow`) on `PATH` if deploying the optional SemanticNet app
- GitHub CLI authentication may be required when the SDK starts subprocesses that need Git access

## Security

The agent tools can execute SQL using the configured Snowflake connection. Use a dedicated least-privilege role and a non-production database for evaluation. Do not use `SYSADMIN` unless it is required for a controlled test, and do not place credentials in this repository.

## How to invoke

```bash
cd sv_automation
python orchestrator.py --config config.yaml
```

### CLI overrides

| Flag | Description |
|------|-------------|
| `--scope-database DB` | Run against a specific database |
| `--scope-schema DB.SCHEMA` | Run against a single schema |
| `--mode automated\|hitl` | Override execution mode from config |

## Execution modes

**`automated`** — Runs all phases without stopping. Best for scheduled/batch use.

**`hitl`** (human-in-the-loop) — Pauses at two checkpoints:
1. **post_plan** — After clustering + lineage. Review `assessment_report.html`, then:
   - Approve: `touch runs/<run>/checkpoints/post_plan.approved`
   - Reject:  `touch runs/<run>/checkpoints/post_plan.rejected`
2. **post_evaluation** — After SV accuracy scoring. Review `evaluation_report.html`, then:
   - Approve: `touch runs/<run>/checkpoints/post_evaluation.approved`
   - Reject:  `touch runs/<run>/checkpoints/post_evaluation.rejected`

## Phases (creation workflow)

| Phase | Description |
|-------|-------------|
| 1 — Context Mining | Mines `ACCOUNT_USAGE.ACCESS_HISTORY` and `QUERY_HISTORY` for tag, warehouse, join, and user/role affinity per table. Tables are batched (default 100/shard) and processed in parallel. |
| 2 — Clustering | Groups tables into semantic clusters using join co-occurrence and tag/warehouse affinity. Enforces `max_tables_per_sv` hard cap by splitting at weakest join edges. |
| 3 — Lineage + Plan | Fetches DDL and upstream lineage for each cluster. Identifies join keys and recommends SV names. Generates `assessment_report.html`. |
| 4 — SV Creation | Calls `SYSTEM$CORTEX_ANALYST_FAST_GENERATION` to produce SV YAML, validates with `cortex reflect`, and self-heals up to `max_self_heal_retries` times per SV. |
| 5 — Evaluation | Samples historical SQL queries and tests Cortex Analyst accuracy against the generated SVs. Produces `evaluation_report.html`. |
| 6 — Cortex Search | Identifies high-cardinality VARCHAR columns and creates Cortex Search Services for point-lookup enrichment. Updates SV YAML with CSS references. |
| 7 — Search Testing | Validates each CSS with synthetic point-lookup questions via Cortex Analyst. |
| 8 — Summary Report | Writes `creation_summary_report.json` with totals, quarantined SVs, self-heal stats, and accuracy scores. |

## Refresh workflow

```bash
python refresh/orchestrator.py --config config.yaml --sv-registry runs/<run>/creation_summary_report.json
```

| Phase | Description |
|-------|-------------|
| R1 — Change Detection | Scans for schema/column/data changes affecting tables in each SV. |
| R2 — Impact Analysis | Classifies changes as breaking / additive / cosmetic. Breaking changes are always quarantined for manual review. |
| R3 — Incremental Update | Re-runs SV creator agent (with self-healing) for additive/cosmetic changes. |
| R4 — Re-evaluation | Re-runs accuracy evaluation on updated SVs. |
| R5 — Refresh Report | Summary of what changed, what was updated, and what needs manual attention. |

## Output structure

```
runs/run_YYYYMMDD_HHMMSS/
  phase1_context.json          # Merged query history context
  cluster_plan.json            # Cluster assignments + confidence
  lineage_<cluster>.json       # Per-cluster DDL + lineage
  assessment_report.html       # HITL Checkpoint 1 review doc
  semantic_views/
    <sv_name>.yaml             # Generated + validated SV YAML
  sv_scores.json               # Per-SV accuracy scores
  evaluation_report.html       # HITL Checkpoint 2 review doc
  cortex_search_manual_action.json  # Columns needing manual change-tracking
  search_test_failures.json    # Failed point-lookup tests (if any)
  creation_summary_report.json # Final summary
  run_log.json                 # Errors, quarantined items, self-heal stats
```

## SemanticNet Deployment

The optional SemanticNet app captures table and column descriptions in Snowflake. Deploy it after configuring a Snowflake CLI connection:

```bash
cd SemanticNet
./deploy.sh YOUR_CONNECTION_NAME YOUR_WAREHOUSE
```

## Configuration

Key settings in `config.yaml`:

```yaml
scope:
  level: database          # account | database | schema
  database: MY_DB
  schema: null             # set for schema-level scope

mode: hitl                 # automated | hitl

thresholds:
  max_tables_per_sv: 15
  min_accuracy_score: 70
  cortex_search_min_distinct: 100

self_healing:
  max_retries: 3

batching:
  table_batch_size: 100
  max_concurrent_agents: 10

snowflake:
  connection: YOUR_CONNECTION_NAME
  role: YOUR_ROLE
  warehouse: YOUR_WAREHOUSE
  target_database: YOUR_DATABASE
  target_schema: PUBLIC
```
