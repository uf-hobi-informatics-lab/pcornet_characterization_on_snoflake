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


def _get_new_connection():
    """Create a new Snowflake connection for thread-safe parallel execution."""
    import os
    from dotenv import load_dotenv
    import snowflake.connector

    load_dotenv()
    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    assert private_key_path is not None
    with open(private_key_path, "rb") as f:
        pk = f.read()

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pk,
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        autocommit=True,
    )


def run_query_threadsafe(sql: str, params=None):
    """Execute a query in a thread-safe manner.

    For Snowpark: uses the shared session (Snowpark is thread-safe).
    For connector: creates a dedicated cursor per call (connection is shared but
    each cursor is independent).  For truly parallel workloads, callers should
    use run_query_new_connection() instead.
    """
    return run_query(sql, params)


def run_async_call(call_sql: str, params: list, db_param: str, show_debug: bool = False):
    """Submit a CALL statement as a Snowflake Task so it survives session suspension.

    Creates a one-shot task in <db_param>.CHARACTERIZATION_DCQ, executes it,
    and returns immediately.  The caller should poll DCQ_RUNS for completion.
    """
    import time, uuid

    task_id = f"DCQ_RUN_{uuid.uuid4().hex[:12].upper()}"
    task_schema = f"{db_param}.CHARACTERIZATION_DCQ"
    task_fqn = f"{task_schema}.{task_id}"

    # Build the SQL CALL with literal values (tasks cannot use bind params)
    def _lit(v):
        if v is None:
            return "NULL"
        return "'" + str(v).replace("'", "''") + "'"

    literals = ", ".join(_lit(p) for p in params)
    inner_sql = f"CALL CHARACTERIZATION.DCQ.SP_RUN_DCQ({literals})"

    # Resolve the current warehouse name (CREATE TASK needs an identifier, not a function)
    current_wh = _first_cell(run_query("SELECT CURRENT_WAREHOUSE()"))

    # Ensure the schema exists
    run_query(f"CREATE SCHEMA IF NOT EXISTS {task_schema}")

    # Create and execute a one-shot task
    create_sql = f"""
        CREATE OR REPLACE TASK {task_fqn}
        WAREHOUSE = {current_wh}
        SCHEDULE = '1 MINUTE'
        AS {inner_sql}
    """

    if show_debug:
        st.code(create_sql, language="sql")

    run_query(create_sql)
    run_query(f"ALTER TASK {task_fqn} RESUME")
    run_query(f"EXECUTE TASK {task_fqn}")

    # Immediately suspend so it doesn't re-run on schedule
    try:
        run_query(f"ALTER TASK {task_fqn} SUSPEND")
    except Exception:
        pass

    return task_fqn


@st.cache_data(ttl=60)
def fetch_databases(prefixes: tuple[str, ...] = ()) -> list[str]:
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
        if not prefixes or any(name_u.startswith(p) for p in prefixes):
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
def fetch_warehouses(prefix: str = "") -> list[str]:
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
        if name_u and (not prefix or name_u.startswith(prefix.upper())):
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


st.write(
    "Provide the required parameters and click Run to execute the stored "
    "procedure CHARACTERIZATION.DCQ.SP_RUN_DCQ. The result (or any error) will be shown below."
)

show_debug = st.checkbox("Show debug output", value=False)

with st.expander("How to run this app"):
    st.markdown(
        """
        What this app does
        - Calls `CHARACTERIZATION.DCQ.SP_RUN_DCQ` to execute one or more DCQ check procedures defined in
          `CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY`.
        - Creates/uses an output schema in the target database: `<DB_PARAM>.CHARACTERIZATION_DCQ`.
        - Writes run metadata and per-check status to:
          - `<DB_PARAM>.CHARACTERIZATION_DCQ.DCQ_RUNS`
          - `<DB_PARAM>.CHARACTERIZATION_DCQ.DCQ_CHECK_LOG`
          - `<DB_PARAM>.CHARACTERIZATION_DCQ.DCQ_RESULTS` (check procedures typically write results here)
        - Returns a string like: `OK RUN_ID=<uuid> STATUS=SUCCEEDED|PARTIAL ...`

        Steps
        1. Select the target
           - `DB_PARAM`: target database
           - `SCHEMA_NAME`: schema within `DB_PARAM` that the checks should evaluate

        2. Choose which checks to run
           - `PART`: filter checks by `PART` in the registry; `all` runs all enabled checks
           - `MODE` + `SELECTOR`:
             - `ALL`: ignore SELECTOR and run all enabled checks (subject to PART)
             - `CHECK_NAME`: SELECTOR is a comma-separated list of check names
               Example: `DCQ_CHECK_001, DCQ_CHECK_010`
             - `CHECK_NUM`: SELECTOR is a comma-separated list of check numbers (decimals allowed)
               Example: `1, 2, 10.5`
             - `SOURCE_TABLE`: SELECTOR is a comma-separated list of source tables; matched against the
               `SOURCE_TABLES` array in the registry
               Example: `MY_TABLE, OTHER_TABLE`

        3. Optional compare/override parameters
           - `TARGET_TABLE`: if provided, passed through to check procedures that support it (otherwise `ALL`)
           - `PREV_DB_PARAM` + `PREV_SCHEMA_NAME`: optional "previous" location. These are only passed to check
             procedures that accept the additional arguments (the orchestrator detects procedure signatures).

        4. Optional date filter
            - `START_DATE` + `END_DATE`: if provided, check procedures will restrict their queries to records
              whose primary date column falls within the given range (inclusive). Leave blank to run on all data.

        5. Parallelism
            - `Max parallel checks`: controls how many checks execute concurrently (default 8). Checks are
              submitted as Snowflake Tasks and run in batches on the selected warehouse. Higher values
              use more warehouse resources but finish faster. A multi-cluster warehouse is recommended
              for best throughput.

        6. Click Run and review output
           - The app prints the returned `RUN_ID` string.
           - Expand "DCQ Check Registry" to see the available checks.
        """
    )


# ---------------------------------------------------------------------------
# Expander - show the full DCQ_CHECK_REGISTRY table
# ---------------------------------------------------------------------------
with st.expander("DCQ Check Registry"):
    try:
        reg_cols = "ROW_NUM_STR AS ROW_NUM, PART, EDC_TABLE, DESCRIPTION, PROC_NAME, SOURCE_TABLES, ENABLED"
        if backend == "snowpark":
            sess = backend_obj
            try:
                df = sess.sql(
                    f"SELECT CHECK_NUM AS ROW_NUM, PART, EDC_TABLE, DESCRIPTION, PROC_NAME, SOURCE_TABLES, ENABLED FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY ORDER BY CHECK_NUM"
                ).to_pandas()
            except Exception:
                df = sess.sql(
                    f"SELECT {reg_cols} FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY ORDER BY ROW_NUM"
                ).to_pandas()

            st.dataframe(df)
        else:
            try:
                rows = run_query(
                    f"SELECT CHECK_NUM AS ROW_NUM, PART, EDC_TABLE, DESCRIPTION, PROC_NAME, SOURCE_TABLES, ENABLED FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY ORDER BY CHECK_NUM"
                )
            except Exception:
                rows = run_query(
                    f"SELECT {reg_cols} FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY ORDER BY ROW_NUM"
                )
            st.dataframe(rows)
    except Exception as e:
        st.error(f"Failed to fetch DCQ_CHECK_REGISTRY: {e}")


db_options = fetch_databases()
if not db_options:
    st.error("No databases found. Check your connection and permissions.")
    st.stop()

db_param = st.selectbox("DB_PARAM (target database)", options=db_options, index=0)
schema_options = fetch_schemas(db_param)
if not schema_options:
    st.error(f"No schemas found in database {db_param}.")
    st.stop()
schema_name = st.selectbox(
    "SCHEMA_NAME",
    options=schema_options,
    index=schema_options.index("PUBLIC") if "PUBLIC" in schema_options else 0,
)

wh_options = fetch_warehouses()
default_wh = ""
wh_default_idx = 0
if default_wh in wh_options:
    wh_default_idx = wh_options.index(default_wh) + 1
warehouse = st.selectbox(
    "Warehouse",
    options=[""] + wh_options,
    index=wh_default_idx,
    format_func=lambda x: x if x else "(Use session default)",
)

if show_debug:
    st.write("DEBUG db_options:", db_options)
    st.write("DEBUG schema_options:", schema_options)
    st.write("DEBUG warehouse options:", wh_options)


tab_dcq, tab_pce, tab_other = st.tabs(["SP_RUN_DCQ", "POTENTIAL_CODE_ERRORS", "Other procedures"])


with tab_dcq:
    st.subheader("CHARACTERIZATION.DCQ.SP_RUN_DCQ")

    mode = st.selectbox("MODE", options=["ALL", "CHECK_NUM", "SOURCE_TABLE"], index=0)
    selector = st.text_input("SELECTOR", "", help="Comma-separated values used when MODE is not ALL.")
    part = st.text_input("PART", value="all")
    target_table = st.text_input("TARGET_TABLE (optional)", "", help="Blank defaults to ALL.")

    prod_db_options = fetch_databases()

    def _on_prev_db_change() -> None:
        st.session_state.pop("prev_schema", None)

    prev_db_param = st.selectbox(
        "PREV_DB_PARAM (optional)",
        options=[""] + prod_db_options,
        format_func=lambda x: x if x else "(None)",
        help="Previous DB parameter for comparison checks.",
        key="prev_db",
        on_change=_on_prev_db_change,
    )

    prev_schema_options = fetch_schemas(prev_db_param) if prev_db_param else []
    prev_schema_default_idx = 0
    if "PUBLIC" in prev_schema_options:
        prev_schema_default_idx = prev_schema_options.index("PUBLIC") + 1  # +1 for the "" entry
    prev_schema_name = st.selectbox(
        "PREV_SCHEMA_NAME (optional)",
        options=[""] + prev_schema_options,
        index=prev_schema_default_idx,
        format_func=lambda x: x if x else "(None)",
        help="Previous schema name (populated from the DB chosen above).",
        key="prev_schema",
    )

    st.markdown("---")
    max_parallel = st.slider(
        "Max parallel checks",
        min_value=1,
        max_value=20,
        value=10,
        step=1,
        help="Number of checks to run concurrently (max 20 due to Snowflake table lock limits). "
             "Higher values use more warehouse resources but finish faster.",
    )
    st.markdown("---")
    st.markdown("**Date filter (optional)**")
    use_date_filter = st.checkbox("Enable date filter", value=False)
    use_10yr_lookback = st.checkbox("Use 10 year lookback", value=False)
    start_date_str: str | None = None
    end_date_str: str | None = None
    if use_10yr_lookback:
        from datetime import date, timedelta
        today = date.today()
        try:
            ten_years_ago = today.replace(year=today.year - 10)
        except ValueError:
            # handles leap day edge case (Feb 29)
            ten_years_ago = today.replace(year=today.year - 10, day=28)
        start_date_str = ten_years_ago.isoformat()
        st.info(f"Start date set to {start_date_str}")
    elif use_date_filter:
        col_start, col_end = st.columns(2)
        with col_start:
            start_date = st.date_input(
                "START_DATE",
                help="Only include records on or after this date.",
                key="start_date",
            )
        with col_end:
            end_date = st.date_input(
                "END_DATE",
                help="Only include records on or before this date.",
                key="end_date",
            )
        start_date_str = start_date.isoformat() if start_date else None
        end_date_str = end_date.isoformat() if end_date else None

    if st.button("Run SP_RUN_DCQ"):
        try:
            use_warehouse(warehouse)

            params = [
                db_param,
                schema_name,
                mode,
                selector,
                part,
                normalize_optional_param(target_table),
                normalize_optional_param(prev_db_param),
                normalize_optional_param(prev_schema_name),
                start_date_str,
                end_date_str,
                str(max_parallel),
            ]
            # Snowpark converts Python None to the string "None" rather than
            # SQL NULL.  Coerce any None values to empty strings so that the
            # stored procedures see '' and treat it the same as NULL.
            params = [p if p is not None else "" for p in params]

            if show_debug:
                st.write("DEBUG params:", params)

            # Step 1: Call SP_RUN_DCQ for setup — returns JSON with run info and check list
            call_sql = "CALL CHARACTERIZATION.DCQ.SP_RUN_DCQ(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            if backend == "snowpark":
                setup_result = run_query(call_sql, params)
            else:
                setup_result = run_query(call_sql.replace("?", "%s"), tuple(params))

            import json as _json
            setup_json = _json.loads(_first_cell(setup_result))
            run_id = setup_json["runId"]
            check_list = setup_json["checks"]
            log_tbl = setup_json["logTable"]
            runs_tbl = setup_json["runsTable"]
            out_schema = setup_json["outSchema"]

            st.session_state["last_run_id"] = run_id
            st.session_state["last_run_db"] = db_param

            st.info(f"RUN_ID: `{run_id}` | {len(check_list)} checks to run (max {max_parallel} parallel)")

            st.info("To monitor progress while running, execute in a worksheet:")
            st.code(
                f"-- Current run\n"
                f"SELECT RUN_ID, STATUS, STARTED_AT FROM {runs_tbl}\n"
                f"WHERE STATUS = 'RUNNING' ORDER BY STARTED_AT DESC LIMIT 1;\n\n"
                f"-- Check progress (replace <RUN_ID>)\n"
                f"SELECT CHECK_ID, CHECK_NAME, STATUS, STARTED_AT, ENDED_AT, ERROR_MESSAGE\n"
                f"FROM {log_tbl} WHERE RUN_ID = '<RUN_ID>' ORDER BY ROW_NUM;",
                language="sql",
            )

            if not check_list:
                st.warning("No checks to run.")
                run_query(f"UPDATE {runs_tbl} SET STATUS='SUCCEEDED', ENDED_AT=CURRENT_TIMESTAMP() WHERE RUN_ID='{run_id}'")
            else:
                # Step 2: Build CALL SQL for each check
                def _build_call_sql(check):
                    proc = check["procName"]
                    max_args = check["maxArgs"]
                    args = [db_param, schema_name, run_id]
                    if max_args >= 4:
                        args.append(setup_json.get("targetTable") or "ALL")
                    if max_args >= 6:
                        args.append(setup_json.get("prevDb") or "")
                        args.append(setup_json.get("prevSchema") or "")
                    if max_args >= 8:
                        args.append(setup_json.get("startDate") or "")
                        args.append(setup_json.get("endDate") or "")
                    return proc, args

                # Step 3: Execute checks in parallel using ThreadPoolExecutor
                import concurrent.futures
                import threading

                progress_bar = st.progress(0, text="Starting checks...")
                status_text = st.empty()
                counters = {"completed": 0, "failed": 0}
                count_lock = threading.Lock()

                def _run_check(check):
                    check_id = check["checkId"]
                    check_name = check["checkName"]
                    proc_name, call_args = _build_call_sql(check)

                    # Each thread gets its own connection for true parallelism
                    thread_conn = None
                    if backend == "snowpark":
                        thread_query = run_query
                    else:
                        thread_conn = _get_new_connection()
                        # Set the same warehouse as the main session
                        if warehouse:
                            try:
                                thread_conn.cursor().execute(f"USE WAREHOUSE {warehouse}")
                            except Exception:
                                pass
                        def thread_query(sql, params=None):
                            cs = thread_conn.cursor()  # type: ignore[union-attr]
                            try:
                                cs.execute(sql, params or ())
                                try:
                                    return cs.fetchall()
                                except Exception:
                                    return []
                            finally:
                                cs.close()

                    try:
                        # Update log to RUNNING
                        thread_query(
                            f"UPDATE {log_tbl} SET STATUS='RUNNING', STARTED_AT=CURRENT_TIMESTAMP() "
                            f"WHERE RUN_ID='{run_id}' AND CHECK_ID='{check_id}' AND STATUS='PENDING'"
                        )

                        # Execute the check procedure
                        ph = ", ".join(["?"] * len(call_args)) if backend == "snowpark" else ", ".join(["%s"] * len(call_args))
                        thread_query(f"CALL {proc_name}({ph})", call_args if backend == "snowpark" else tuple(call_args))

                        # Update log to SUCCEEDED
                        thread_query(
                            f"UPDATE {log_tbl} SET STATUS='SUCCEEDED', ENDED_AT=CURRENT_TIMESTAMP() "
                            f"WHERE RUN_ID='{run_id}' AND CHECK_ID='{check_id}' AND STATUS='RUNNING'"
                        )
                        with count_lock:
                            counters["completed"] += 1
                        return None
                    except Exception as e:
                        err_msg = str(e)[:2000].replace("'", "''")
                        try:
                            thread_query(
                                f"UPDATE {log_tbl} SET STATUS='FAILED', ENDED_AT=CURRENT_TIMESTAMP(), "
                                f"ERROR_MESSAGE='{err_msg}' "
                                f"WHERE RUN_ID='{run_id}' AND CHECK_ID='{check_id}' AND STATUS='RUNNING'"
                            )
                        except Exception:
                            pass
                        with count_lock:
                            counters["completed"] += 1
                            counters["failed"] += 1
                        return f"{check_name}: {str(e)[:200]}"
                    finally:
                        if thread_conn is not None:
                            try:
                                thread_conn.close()
                            except Exception:
                                pass

                total = len(check_list)
                errors = []

                with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as executor:
                    futures = {executor.submit(_run_check, c): c for c in check_list}
                    for future in concurrent.futures.as_completed(futures):
                        err = future.result()
                        if err:
                            errors.append(err)
                        with count_lock:
                            done = counters["completed"]
                            fails = counters["failed"]
                        pct = done / total
                        progress_bar.progress(pct, text=f"{done}/{total} checks completed ({fails} failed)")

                progress_bar.progress(1.0, text=f"Done: {total}/{total} checks completed ({counters['failed']} failed)")

                # Step 4: Finalize the run
                final_status = "PARTIAL" if counters["failed"] > 0 else "SUCCEEDED"
                run_query(
                    f"UPDATE {runs_tbl} SET STATUS='{final_status}', ENDED_AT=CURRENT_TIMESTAMP() "
                    f"WHERE RUN_ID='{run_id}'"
                )

                if final_status == "SUCCEEDED":
                    st.success(f"Completed: RUN_ID=`{run_id}` STATUS={final_status}")
                else:
                    st.warning(f"Completed with errors: RUN_ID=`{run_id}` STATUS={final_status}")
                    with st.expander(f"{counters['failed']} failed checks"):
                        for err in errors:
                            st.text(err)

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

                if backend == "snowpark":
                    run_rows = run_query(f"SELECT * FROM {runs_tbl} WHERE RUN_ID = ?", [run_id])
                else:
                    run_rows = run_query(f"SELECT * FROM {runs_tbl} WHERE RUN_ID = %s", (run_id,))
                if run_rows:
                    st.dataframe(run_rows)

                if backend == "snowpark":
                    counts = run_query(
                        f"SELECT STATUS, COUNT(*) AS CNT FROM {log_tbl} WHERE RUN_ID = ? GROUP BY STATUS ORDER BY STATUS",
                        [run_id],
                    )
                else:
                    counts = run_query(
                        f"SELECT STATUS, COUNT(*) AS CNT FROM {log_tbl} WHERE RUN_ID = %s GROUP BY STATUS ORDER BY STATUS",
                        (run_id,),
                    )
                if counts:
                    st.dataframe(counts)

                if backend == "snowpark":
                    failures = run_query(
                        f"""
                        SELECT CHECK_ID, CHECK_NAME, ROW_NUM, PROC_NAME, STATUS, ERROR_MESSAGE
                        FROM {log_tbl}
                        WHERE RUN_ID = ? AND STATUS <> 'SUCCEEDED'
                        ORDER BY ROW_NUM, CHECK_NAME
                        """,
                        [run_id],
                    )
                else:
                    failures = run_query(
                        f"""
                        SELECT CHECK_ID, CHECK_NAME, ROW_NUM, PROC_NAME, STATUS, ERROR_MESSAGE
                        FROM {log_tbl}
                        WHERE RUN_ID = %s AND STATUS <> 'SUCCEEDED'
                        ORDER BY ROW_NUM, CHECK_NAME
                        """,
                        (run_id,),
                    )
                if failures:
                    st.subheader("Non-succeeded checks")
                    st.dataframe(failures)

                if st.checkbox("Show results rows", value=False, key="show_results_rows"):
                    limit = st.number_input("Max rows", min_value=10, max_value=5000, value=500, step=50)

                    if backend == "snowpark":
                        results = run_query(
                            f"SELECT RUN_ID, CHECK_ID, CHECK_NAME, ROW_NUM, EDC_TABLE, SOURCE_TABLE, CODE_TYPE, METRIC, VALUE_NUM::DOUBLE AS VALUE_NUM, VALUE_STR, THRESHOLD_NUM::DOUBLE AS THRESHOLD_NUM, EXCEPTION_FLAG, DETAILS, CREATED_AT FROM {res_tbl} WHERE RUN_ID = ? ORDER BY CREATED_AT DESC LIMIT {int(limit)}",
                            [run_id],
                        )
                    else:
                        results = run_query(
                            f"SELECT RUN_ID, CHECK_ID, CHECK_NAME, ROW_NUM, EDC_TABLE, SOURCE_TABLE, CODE_TYPE, METRIC, VALUE_NUM::DOUBLE AS VALUE_NUM, VALUE_STR, THRESHOLD_NUM::DOUBLE AS THRESHOLD_NUM, EXCEPTION_FLAG, DETAILS, CREATED_AT FROM {res_tbl} WHERE RUN_ID = %s ORDER BY CREATED_AT DESC LIMIT {int(limit)}",
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
