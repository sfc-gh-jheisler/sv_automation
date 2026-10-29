"""SemanticNet v0: Single-page metadata description capture tool."""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from utils.coverage import get_table_coverage, get_column_detail, get_columns_for_tables, get_all_tables_in_scope
from utils.llm import batch_optimize, freeform_parse
from utils.definitions import batch_save

session = get_active_session()
st.set_page_config(page_title="SemanticNet", layout="wide")


# =====================================================================
# Helpers
# =====================================================================

@st.cache_data(ttl=300)
def _get_databases():
    _session = get_active_session()
    rows = _session.sql("SHOW DATABASES").collect()
    return sorted(r["name"] for r in rows)


@st.cache_data(ttl=300)
def _get_schemas(database):
    _session = get_active_session()
    rows = _session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()
    return sorted(r["name"] for r in rows if r["name"] != "INFORMATION_SCHEMA")


@st.cache_data(ttl=120)
def _cached_table_coverage(database, schema):
    _session = get_active_session()
    from utils.coverage import get_table_coverage
    return get_table_coverage(_session, database, schema)


@st.cache_data(ttl=120)
def _cached_column_detail(database, schema, table_name):
    _session = get_active_session()
    from utils.coverage import get_column_detail
    return get_column_detail(_session, database, schema, table_name)


@st.cache_data(ttl=120)
def _cached_all_tables(database, schema):
    _session = get_active_session()
    from utils.coverage import get_all_tables_in_scope
    return get_all_tables_in_scope(_session, database, schema)


# =====================================================================
# Scope selector
# =====================================================================

st.title("SemanticNet")

databases = _get_databases()
db_upper = [d.upper() for d in databases]
db_idx = 0

col1, col2 = st.columns(2)

with col1:
    selected_db = st.selectbox("Database", databases, index=db_idx)

with col2:
    schemas = _get_schemas(selected_db) if selected_db else []
    sch_upper = [s.upper() for s in schemas]
    sch_idx = 0
    selected_schema = st.selectbox("Schema", schemas, index=sch_idx if schemas else 0)

db = selected_db
schema = selected_schema

if not db or not schema:
    st.info("Select a database and schema to get started.")
    st.stop()


# =====================================================================
# Coverage (collapsible)
# =====================================================================

tables = _cached_table_coverage(db, schema)

if tables:
    total = len(tables)
    total_cols = sum(int(t["TOTAL_COLUMNS"]) for t in tables)
    described_cols = sum(int(t["DESCRIBED_COUNT"]) for t in tables)
    overall_pct = (described_cols / total_cols * 100) if total_cols else 0

    with st.expander(f"Coverage: {described_cols}/{total_cols} columns described ({overall_pct:.0f}%)"):
        c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
        c1.metric("Tables", total)
        c2.metric("Described Columns", described_cols)
        c3.metric("Coverage", f"{overall_pct:.0f}%")
        with c4:
            if st.button("Refresh Coverage"):
                _cached_table_coverage.clear()
                _cached_column_detail.clear()
                st.experimental_rerun()

        df = pd.DataFrame([{
            "Table": t["TABLE_NAME"],
            "Type": t["TABLE_TYPE"],
            "Columns": int(t["TOTAL_COLUMNS"]),
            "Described": int(t["DESCRIBED_COUNT"]),
            "Coverage %": float(t["COVERAGE_PCT"]),
        } for t in tables])
        st.dataframe(df, use_container_width=True)

        # Drill into a table to see column-level detail
        inspect_table = st.selectbox(
            "Inspect table columns",
            [""] + [t["TABLE_NAME"] for t in tables],
            key="inspect_table",
        )
        if inspect_table:
            col_rows = _cached_column_detail(db, schema, inspect_table)
            if col_rows:
                col_df = pd.DataFrame([{
                    "Column": r["COLUMN_NAME"] or "(TABLE-LEVEL)",
                    "Type": r["DATA_TYPE"] or "--",
                    "Status": r["COVERAGE_STATUS"],
                    "Description": (r["DEFINITION_TEXT"] or r["METADATA_COMMENT"] or ""),
                    "Version": r["VERSION"] if r["VERSION"] else "--",
                } for r in col_rows])
                st.dataframe(col_df, use_container_width=True)
else:
    st.info("No tables found in this schema.")
    st.stop()

st.divider()


# =====================================================================
# State management
# =====================================================================

if "describe_state" not in st.session_state:
    st.session_state["describe_state"] = "COLLECTING"
if "pending_reviews" not in st.session_state:
    st.session_state["pending_reviews"] = []


# =====================================================================
# COLLECTING
# =====================================================================

if st.session_state["describe_state"] == "COLLECTING":

    mode = st.radio("Input mode", ["Column-by-Column", "Free-Form"], horizontal=True)

    if mode == "Column-by-Column":
        # --- Table selector ---
        all_tables = _cached_all_tables(db, schema)
        table_names = [""] + [t.split(".")[-1] for t in all_tables]
        selected_table = st.selectbox("Table", table_names)

        if selected_table:
            columns = _cached_column_detail(db, schema, selected_table)
            if columns:
                st.caption("Check columns to describe, type a description for each, then click Optimize All.")

                select_all = st.checkbox("Select all columns", key="select_all_cols")

                descriptions = {}
                for row in columns:
                    col_name = row["COLUMN_NAME"]
                    if col_name is None:
                        label = f"(TABLE-LEVEL) -- {row['COVERAGE_STATUS']}"
                        key = f"tbl__{selected_table}"
                    else:
                        label = f"{col_name} ({row['DATA_TYPE']}) -- {row['COVERAGE_STATUS']}"
                        key = f"col__{selected_table}__{col_name}"

                    checked = st.checkbox(label, key=f"chk_{key}", value=select_all)
                    if checked:
                        existing = row["DEFINITION_TEXT"] or row["METADATA_COMMENT"] or ""
                        desc = st.text_input(
                            f"Description for {col_name or 'table'}",
                            value=existing, key=f"inp_{key}",
                            label_visibility="collapsed",
                        )
                        if desc.strip():
                            descriptions[key] = {
                                "column_name": col_name,
                                "data_type": row["DATA_TYPE"],
                                "raw_input": desc.strip(),
                            }

                if descriptions:
                    st.caption(f"{len(descriptions)} description(s) pending")
                    if st.button("Optimize All with AI", type="primary"):
                        with st.spinner("Optimizing..."):
                            items = list(descriptions.values())
                            table_fqn = f"{db}.{schema}.{selected_table}"
                            optimized = batch_optimize(session, table_fqn, items)
                            reviews = []
                            for opt in optimized:
                                col_key = f"col__{selected_table}__{opt['column_name']}"
                                tbl_key = f"tbl__{selected_table}"
                                dt = descriptions.get(col_key, descriptions.get(tbl_key, {})).get("data_type")
                                reviews.append({
                                    "database": db, "schema": schema,
                                    "table": selected_table, "column": opt["column_name"],
                                    "data_type": dt,
                                    "raw_input": opt["raw_input"],
                                    "optimized": opt["optimized"],
                                    "synonyms": opt.get("synonyms", ""),
                                    "confidence": "high", "accepted": True,
                                })
                            st.session_state["pending_reviews"] = reviews
                            st.session_state["describe_state"] = "REVIEWING"
                            st.experimental_rerun()

    else:
        # --- Free-Form ---
        st.caption("Call out the column name (and table if spanning multiple), then describe it.")
        all_tables = _cached_all_tables(db, schema)
        with st.expander("Available tables"):
            st.text("\n".join(all_tables))

        free_text = st.text_area("Descriptions", height=200)

        if free_text and st.button("Parse and Optimize", type="primary"):
            with st.spinner("Parsing and optimizing..."):
                col_lists = get_columns_for_tables(session, all_tables)
                result = freeform_parse(session, free_text, db, schema, col_lists)
                reviews = []
                for m in result.get("matched", []):
                    reviews.append({
                        "database": m.get("database", db),
                        "schema": m.get("schema", schema),
                        "table": m.get("table", ""),
                        "column": m.get("column"),
                        "data_type": m.get("data_type"),
                        "raw_input": m.get("raw_segment", ""),
                        "optimized": m.get("optimized", m.get("raw_segment", "")),
                        "synonyms": m.get("synonyms", ""),
                        "confidence": m.get("confidence", "medium"),
                        "accepted": True,
                    })
                for t in result.get("table_level", []):
                    reviews.append({
                        "database": t.get("database", db),
                        "schema": t.get("schema", schema),
                        "table": t.get("table", ""),
                        "column": None, "data_type": None,
                        "raw_input": t.get("raw_segment", ""),
                        "optimized": t.get("optimized", t.get("raw_segment", "")),
                        "synonyms": t.get("synonyms", ""),
                        "confidence": "high", "accepted": True,
                    })
                for seg in result.get("unmatched", []):
                    reviews.append({
                        "database": db, "schema": schema,
                        "table": "", "column": "__UNMATCHED__",
                        "data_type": None, "raw_input": seg,
                        "optimized": seg, "confidence": "none", "accepted": False,
                    })
                st.session_state["pending_reviews"] = reviews
                st.session_state["describe_state"] = "REVIEWING"
                st.experimental_rerun()


# =====================================================================
# REVIEWING
# =====================================================================

elif st.session_state["describe_state"] == "REVIEWING":
    reviews = st.session_state.get("pending_reviews", [])
    if not reviews:
        st.session_state["describe_state"] = "COLLECTING"
        st.experimental_rerun()

    st.subheader("Review Optimized Descriptions")

    for i, item in enumerate(reviews):
        if item.get("column") == "__UNMATCHED__":
            st.warning(f"Unmatched: \"{item['raw_input']}\"")
            tbl = st.text_input("Assign to table", key=f"assign_tbl_{i}")
            col = st.text_input("Assign to column (empty = table-level)", key=f"assign_col_{i}")
            if tbl:
                item["table"] = tbl
                item["column"] = col if col else None
                item["accepted"] = True
            continue

        col_label = (
            f"{item['table']}.{item['column']}" if item.get("column")
            else f"{item['table']} (table-level)"
        )
        conf = f" [{item['confidence']}]" if item.get("confidence") != "high" else ""
        st.markdown(f"**{col_label}**{conf}")

        c1, c2 = st.columns(2)
        with c1:
            st.text_area("Raw", value=item["raw_input"], disabled=True, key=f"raw_{i}", height=68)
        with c2:
            edited = st.text_area("Optimized (editable)", value=item["optimized"], key=f"opt_{i}", height=68)
            item["optimized"] = edited

        syn_edited = st.text_input("Synonyms (comma-separated)", value=item.get("synonyms", ""), key=f"syn_{i}")
        item["synonyms"] = syn_edited

        item["accepted"] = st.checkbox("Accept", value=item.get("accepted", True), key=f"acc_{i}")
        st.divider()

    write_mode = st.radio(
        "Write to", ["TABLE_ONLY", "METADATA_ONLY", "BOTH"], horizontal=True,
    )

    bc, bs = st.columns(2)
    with bc:
        if st.button("Back"):
            st.session_state["describe_state"] = "COLLECTING"
            st.experimental_rerun()
    with bs:
        accepted = [r for r in reviews if r.get("accepted") and r.get("column") != "__UNMATCHED__"]
        if st.button(f"Save {len(accepted)} Approved", type="primary", disabled=not accepted):
            with st.spinner("Saving..."):
                save_items = [{
                    "database": r["database"], "schema": r["schema"],
                    "table": r["table"], "column": r.get("column"),
                    "object_type": "TABLE" if not r.get("column") else "COLUMN",
                    "raw_input": r["raw_input"],
                    "definition_text": r["optimized"],
                    "synonyms": r.get("synonyms", ""),
                } for r in accepted]
                count = batch_save(session, save_items, write_mode)
                st.session_state["describe_state"] = "SAVED"
                st.session_state["saved_count"] = count
                st.experimental_rerun()


# =====================================================================
# SAVED
# =====================================================================

elif st.session_state["describe_state"] == "SAVED":
    st.success(f"Saved {st.session_state.get('saved_count', 0)} description(s).")
    if st.button("Describe more"):
        st.session_state["describe_state"] = "COLLECTING"
        st.session_state["pending_reviews"] = []
        st.experimental_rerun()
