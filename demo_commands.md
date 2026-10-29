# Demo Commands

## Prerequisites

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Authenticate with GitHub (required -- the SDK spawns subprocesses that need Git access):
```bash
gh auth login
gh auth setup-git
```

3. Fill in `config.yaml` with your environment values (database, schema, connection name, target database).

4. Your connection name must match an entry under `[connections.<name>]` in `~/.snowflake/config.toml`. The code falls back to `~/.snowflake/connections.toml` for legacy configurations.

## Run Commands

### Full automated run (no checkpoints)
```bash
cd <REPO_PATH>
python orchestrator.py --config config.yaml
```

### Human-in-the-loop run (pauses for review)
```bash
cd <REPO_PATH>
python orchestrator.py --config config.yaml --mode hitl
```

### Override scope at runtime
```bash
# Single schema
python orchestrator.py --config config.yaml --scope-schema YOUR_DATABASE.YOUR_SCHEMA

# Entire database
python orchestrator.py --config config.yaml --scope-database YOUR_DATABASE
```

## HITL Checkpoints

When running in `hitl` mode, the orchestrator pauses at two points. Open a separate terminal to approve/reject.

### Checkpoint 1: Post-Plan (after Phase 3)
Review `runs/<run>/assessment_report.html`, then:
```bash
# Approve
touch runs/<run>/checkpoints/post_plan.approved

# Reject
touch runs/<run>/checkpoints/post_plan.rejected
```

### Checkpoint 2: Post-Evaluation (after Phase 5)
Review `runs/<run>/evaluation_report.html`, then:
```bash
# Approve
touch runs/<run>/checkpoints/post_evaluation.approved

# Reject
touch runs/<run>/checkpoints/post_evaluation.rejected
```

## Refresh Workflow

Re-evaluate existing SVs after schema changes:
```bash
python refresh/orchestrator.py --config config.yaml --sv-registry runs/<run>/creation_summary_report.json
```

## Current Config

Use the placeholders in `config.yaml` with your own database, schema, connection, role, and warehouse.

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `'Server' object has no attribute 'list_tools'` | `pip install "mcp<2"` |
| `CortexCodeAgentOptions got unexpected keyword argument` | Clear `__pycache__`: `find . -name __pycache__ -exec rm -rf {} +` |
| `Connection not found` | Add the connection under `[connections.<name>]` in `~/.snowflake/config.toml`; legacy `connections.toml` is also supported |
| GitHub username prompt hangs the run | Run `gh auth login && gh auth setup-git`. Fallback: `export GIT_TERMINAL_PROMPT=0` |
| Tables excluded due to low query count | Lower `min_table_queries` in config.yaml (default 5, set to 0 to include all) |
