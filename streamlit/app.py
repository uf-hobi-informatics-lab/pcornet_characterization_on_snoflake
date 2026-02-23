"""Streamlit app to run Characterization stored procedures.

Primary target: Snowflake Streamlit (Snowpark session via get_active_session).
Fallback: local development using snowflake-connector-python + .env.

Includes dedicated runners for:
- CHARACTERIZATION.DCQ.SP_RUN_DCQ (orchestrator)
- CHARACTERIZATION.DCQ_CHECKS.POTENTIAL_CODE_ERRORS
"""

import re
from typing import Any
import streamlit as st


# ---------------------------------------------------------------------------
# Backend selection
# ---------------------------------------------------------------------------
try:
    from snowflake.snowpark.context import get_active_session  # type: ignore
except Exception:
    get_active_session = None


def _get_backend():
    if get_active_session is not None:
        try:
            sess = get_active_session()
            return "snowpark", sess
        except Exception:
            pass

    # Local dev fallback
    import os
    from dotenv import load_dotenv
    import snowflake.connector
    from snowflake.connector import DictCursor

    load_dotenv()

    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    role = os.getenv("SNOWFLAKE_ROLE")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")

    missing_vars = []
    for var_name, var_value in [
        ("SNOWFLAKE_ACCOUNT", account),
        ("SNOWFLAKE_USER", user),
        ("SNOWFLAKE_PRIVATE_KEY_PATH", private_key_path),
        ("SNOWFLAKE_WAREHOUSE", warehouse),
        ("SNOWFLAKE_DATABASE", database),
        ("SNOWFLAKE_SCHEMA", schema),
    ]:
        if not var_value:
            missing_vars.append(var_name)

    if missing_vars:
        raise RuntimeError(
            "Missing environment variables for local mode: " + ", ".join(missing_vars)
        )

    assert private_key_path is not None
    with open(private_key_path, "rb") as f:
        private_key = f.read()

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        private_key=private_key,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema,
        autocommit=True,
    )

    return "connector", (conn, DictCursor, database, schema)

try:
    backend, backend_obj = _get_backend()
    backend_obj = backend_obj  # type: Any
except Exception as e:
    st.error(f"Unable to initialize Snowflake session: {e}")
    st.stop()


def normalize_optional_param(value: str | None) -> str | None:
    v = (value or "").strip()
    if not v:
        return None
    if v.upper() in {"NONE", "(NONE)", "NULL"}:
        return None
    return v


def parse_run_id(result_text: str) -> str | None:
    m = re.search(r"\bRUN_ID=([0-9a-fA-F-]{36})\b", result_text or "")
    return m.group(1) if m else None


def _first_cell(rows) -> str:
    if not rows:
        return "(no result)"
    r0 = rows[0]
    if isinstance(r0, dict):
        return str(next(iter(r0.values())))
    try:
        return str(r0[0])
    except Exception:
        return str(r0)


def _row_to_dict(row: Any) -> dict:
    if isinstance(row, dict):
        return row
    if hasattr(row, "as_dict"):
        try:
            return row.as_dict()
        except Exception:
            return {}
    return {}


def _row_get(row: Any, keys: list[str], idx: int | None = None) -> Any:
    d = _row_to_dict(row)
    if d:
        for k in keys:
            if k in d:
                return d[k]
            ku = k.upper()
            if ku in d:
                return d[ku]
            kl = k.lower()
            if kl in d:
                return d[kl]
    if idx is not None:
        try:
            return row[idx]
        except Exception:
            return None
    return None


def run_query(sql: str, params=None):
    """Execute a query using the active backend.

    Snowpark: uses '?' placeholders and params as list.
    Connector: uses '%s' placeholders and params as tuple.
    """
    if backend == "snowpark":
        sess = backend_obj  # type: ignore[assignment]
        p = params if params is not None else []
        return sess.sql(sql, params=p).collect()  # type: ignore[attr-defined]

    conn, DictCursor, _database, _schema = backend_obj
    cs = conn.cursor(DictCursor)
    try:
        cs.execute(sql, params or ())
        try:
            return cs.fetchall()
        except Exception:
            return []
    finally:
        cs.close()


@st.cache_data(ttl=60)
def fetch_databases(prefixes: tuple[str, ...]) -> list[str]:
    rows = run_query("SHOW DATABASES")
    out: list[str] = []
    for r in rows:
        if isinstance(r, dict):
            name = (r.get("name") or r.get("NAME") or "")
        else:
            try:
                name = r[1]
            except Exception:
                name = getattr(r, "name", "")
        name_u = str(name).upper()
        if any(name_u.startswith(p) for p in prefixes):
            out.append(name_u)
    return sorted(set(out))


@st.cache_data(ttl=60)
def fetch_schemas(database: str) -> list[str]:
    if not database:
        return []
    rows = run_query(f"SHOW SCHEMAS IN DATABASE {database}")
    out: list[str] = []
    for r in rows:
        if isinstance(r, dict):
            name = (r.get("name") or r.get("NAME") or "")
        else:
            try:
                name = r[1]
            except Exception:
                name = getattr(r, "name", "")
        if name:
            out.append(str(name).upper())
    return sorted(set(out))


@st.cache_data(ttl=60)
def fetch_warehouses(prefix: str = "CHARACTERIZATION_") -> list[str]:
    rows = run_query("SHOW WAREHOUSES")
    out: list[str] = []
    for r in rows:
        if isinstance(r, dict):
            name = (r.get("name") or r.get("NAME") or "")
        else:
            try:
                name = r[0]
            except Exception:
                name = getattr(r, "name", "")
        name_u = str(name).upper()
        if name_u and name_u.startswith(prefix.upper()):
            out.append(name_u)
    return sorted(set(out))


def use_warehouse(warehouse: str | None) -> None:
    wh = (warehouse or "").strip()
    if not wh:
        return
    try:
        run_query(f"USE WAREHOUSE {wh}")
    except Exception:
        # Some environments restrict USE statements; procedure execution may still work.
        pass

st.title("Characterization procedure runner")

st.caption(f"Backend: `{backend}`")

try:
    current_role = run_query("SELECT CURRENT_ROLE()")
    current_wh = run_query("SELECT CURRENT_WAREHOUSE()")
    st.caption(f"Role: `{_first_cell(current_role)}` | Warehouse: `{_first_cell(current_wh)}`")
except Exception:
    pass


db_options = fetch_databases(("CHAR_", "STAGE_", "PROD_"))
if not db_options:
    st.error("No target databases found (expected CHAR_/STAGE_/PROD_ prefixes).")
    st.stop()

db_param = st.selectbox("DB_PARAM (target database)", options=db_options, index=0)
schema_options = fetch_schemas(db_param)
schema_name = st.selectbox("SCHEMA_NAME", options=schema_options, index=0 if schema_options else None)

wh_options = fetch_warehouses("CHARACTERIZATION_")
default_wh = "CHARACTERIZATION_XS"
wh_default_idx = 0
if default_wh in wh_options:
    wh_default_idx = wh_options.index(default_wh) + 1
warehouse = st.selectbox(
    "Warehouse",
    options=[""] + wh_options,
    index=wh_default_idx,
    format_func=lambda x: x if x else "(Use session default)",
)


tab_dcq, tab_pce, tab_other = st.tabs(["SP_RUN_DCQ", "POTENTIAL_CODE_ERRORS", "Other procedures"])


with tab_dcq:
    st.subheader("CHARACTERIZATION.DCQ.SP_RUN_DCQ")

    mode = st.selectbox("MODE", options=["ALL", "CHECK_NAME", "CHECK_NUM", "SOURCE_TABLE"], index=0)
    selector = st.text_input("SELECTOR", "", help="Comma-separated values used when MODE is not ALL.")
    part = st.text_input("PART", value="all")
    target_table = st.text_input("TARGET_TABLE (optional)", "", help="Blank defaults to ALL.")
    prev_db_param = st.text_input("PREV_DB_PARAM (optional)", "")
    prev_schema_name = st.text_input("PREV_SCHEMA_NAME (optional)", "")

    if st.button("Run SP_RUN_DCQ"):
        with st.spinner("Executing SP_RUN_DCQ..."):
            try:
                use_warehouse(warehouse)

                if backend == "snowpark":
                    rows = run_query(
                        "CALL CHARACTERIZATION.DCQ.SP_RUN_DCQ(?, ?, ?, ?, ?, ?, ?, ?)",
                        [
                            db_param,
                            schema_name,
                            mode,
                            selector,
                            part,
                            normalize_optional_param(target_table),
                            normalize_optional_param(prev_db_param),
                            normalize_optional_param(prev_schema_name),
                        ],
                    )
                else:
                    rows = run_query(
                        "CALL CHARACTERIZATION.DCQ.SP_RUN_DCQ(%s,%s,%s,%s,%s,%s,%s,%s)",
                        (
                            db_param,
                            schema_name,
                            mode,
                            selector,
                            part,
                            normalize_optional_param(target_table),
                            normalize_optional_param(prev_db_param),
                            normalize_optional_param(prev_schema_name),
                        ),
                    )

                result_text = _first_cell(rows)
                st.success("Executed")
                st.code(result_text)

                run_id = parse_run_id(result_text)
                if run_id:
                    st.session_state["last_run_id"] = run_id
                    st.session_state["last_run_db"] = db_param
            except Exception as e:
                st.error(f"Error: {e}")

    run_id = st.session_state.get("last_run_id")
    run_db = st.session_state.get("last_run_db")
    if run_id and run_db:
        with st.expander("Last run details"):
            st.write(f"RUN_ID: `{run_id}`")
            try:
                runs_tbl = f"{run_db}.CHARACTERIZATION_DCQ.DCQ_RUNS"
                log_tbl = f"{run_db}.CHARACTERIZATION_DCQ.DCQ_CHECK_LOG"
                res_tbl = f"{run_db}.CHARACTERIZATION_DCQ.DCQ_RESULTS"

                st.write(f"Tables: `{runs_tbl}` | `{log_tbl}` | `{res_tbl}`")

                run_rows = run_query(f"SELECT * FROM {runs_tbl} WHERE RUN_ID=%s", (run_id,))
                if run_rows:
                    st.dataframe(run_rows)

                counts = run_query(
                    f"SELECT STATUS, COUNT(*) AS CNT FROM {log_tbl} WHERE RUN_ID=%s GROUP BY STATUS ORDER BY STATUS",
                    (run_id,),
                )
                if counts:
                    st.dataframe(counts)

                failures = run_query(
                    f"""
                    SELECT CHECK_ID, CHECK_NAME, ROW_NUM, PROC_NAME, STATUS, ERROR_MESSAGE
                    FROM {log_tbl}
                    WHERE RUN_ID=%s AND STATUS <> 'SUCCEEDED'
                    ORDER BY ROW_NUM, CHECK_NAME
                    """,
                    (run_id,),
                )
                if failures:
                    st.subheader("Non-succeeded checks")
                    st.dataframe(failures)

                if st.checkbox("Show results rows", value=False, key="show_results_rows"):
                    limit = st.number_input("Max rows", min_value=10, max_value=5000, value=500, step=50)
                    results = run_query(
                        f"SELECT * FROM {res_tbl} WHERE RUN_ID=%s ORDER BY CREATED_AT DESC LIMIT {int(limit)}",
                        (run_id,),
                    )
                    st.dataframe(results)
            except Exception as e:
                st.error(f"Failed to fetch run details: {e}")


with tab_pce:
    st.subheader("CHARACTERIZATION.DCQ_CHECKS.POTENTIAL_CODE_ERRORS")
    st.caption("Validates medical codes across selected CDM tables. Blank defaults to ALL.")

    all_tables = st.checkbox("All tables", value=True)
    table_choices = [
        "DIAGNOSIS",
        "PROCEDURES",
        "PRESCRIBING",
        "DISPENSING",
        "MED_ADMIN",
        "LAB_RESULT_CM",
        "CONDITION",
        "IMMUNIZATION",
    ]
    selected_tables: list[str] = []
    if not all_tables:
        selected_tables = st.multiselect("TABLE_LIST", options=table_choices, default=[])

    if st.button("Run POTENTIAL_CODE_ERRORS"):
        with st.spinner("Executing POTENTIAL_CODE_ERRORS..."):
            try:
                use_warehouse(warehouse)
                table_list = "ALL" if all_tables else ",".join(selected_tables) or "ALL"

                if backend == "snowpark":
                    rows = run_query(
                        "CALL CHARACTERIZATION.DCQ_CHECKS.POTENTIAL_CODE_ERRORS(?, ?, ?)",
                        [db_param, schema_name, table_list],
                    )
                else:
                    rows = run_query(
                        "CALL CHARACTERIZATION.DCQ_CHECKS.POTENTIAL_CODE_ERRORS(%s,%s,%s)",
                        (db_param, schema_name, table_list),
                    )

                result_text = _first_cell(rows)
                st.success("Executed")
                st.code(result_text)

                try:
                    summary = run_query(
                        f"SELECT * FROM {db_param}.CHARACTERIZATION.CODE_SUMMARY ORDER BY TABLE_NAME, CODE_TYPE"
                    )
                    if summary:
                        st.subheader("CODE_SUMMARY")
                        st.dataframe(summary)
                except Exception as e:
                    st.info(f"CODE_SUMMARY not available (yet): {e}")
            except Exception as e:
                st.error(f"Error: {e}")


with tab_other:
    st.subheader("Browse and run other procedures")

    if backend == "connector":
        _conn, _DictCursor, database, schema = backend_obj
    else:
        database = "CHARACTERIZATION"
        schema = "DCQ"

    procedures = run_query(f"SHOW PROCEDURES IN SCHEMA {database}.{schema}")
    if not procedures:
        st.info("No stored procedures found.")
        st.stop()

    proc_options: list[str] = []
    proc_meta: dict[str, dict] = {}
    for proc in procedures:
        name = _row_get(proc, ["name"], idx=1)
        args = _row_get(proc, ["arguments"], idx=None) or ""
        identifier = f"{name}({args})" if args else str(name)
        proc_options.append(identifier)
        proc_meta[identifier] = _row_to_dict(proc)

    selected = st.selectbox("Select a stored procedure", proc_options)
    metadata = proc_meta[selected]
    args_signature = metadata.get("arguments") or metadata.get("ARGUMENTS") or ""
    st.write(f"Signature: `{selected}`")

    if args_signature:
        arg_input = st.text_input("Arguments (comma-separated, as SQL literals)", "")
    else:
        arg_input = ""

    if st.button("Run selected procedure"):
        with st.spinner("Executing..."):
            try:
                use_warehouse(warehouse)
                call_sql = f"CALL {selected}"
                if args_signature:
                    call_sql += f"({arg_input})"
                else:
                    call_sql += "()"
                rows = run_query(call_sql)
                st.success("Procedure executed successfully.")
                if rows:
                    st.write("Result:")
                    st.dataframe(rows)
            except Exception as e:
                st.error(f"Error: {e}")
