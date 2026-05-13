# AGENTS.md — AI Coding Conventions

## General Rules

- Always read `.opencode/codebase-summary.md` first for project context.
- Prefer editing existing files over creating new ones.
- Do not commit `.env`, credentials, or private keys.
- Test changes locally before deploying to Snowflake when possible.

## Snowflake Stored Procedures (JavaScript)

- All procedures use `EXECUTE AS CALLER` and `LANGUAGE JAVASCRIPT`.
- The procedure body is delimited by `AS '...'`. Inside this body:
  - Every literal single quote must be doubled: `''` (e.g., `''RUNNING''` for the string `'RUNNING'`).
  - Comments must not contain unescaped single quotes (this will break parsing).
  - Regex literals (`/pattern/`) are fine — `'` inside regex does not need escaping.
- Use `q(sqlText, binds)` for parameterized queries. Never concatenate user input into SQL strings.
- Use `isSafeIdentPart()` or `isSafeProcName()` before interpolating identifiers.
- Check procedures share a common 8-param signature: `(DB_PARAM, SCHEMA_NAME, RUN_ID, TARGET_TABLE, PREV_DB_PARAM, PREV_SCHEMA_NAME, START_DATE, END_DATE)`. Some accept fewer args — the orchestrator auto-detects via `SHOW PROCEDURES`.
- Each check must be idempotent: DELETE prior results for the same RUN_ID before inserting.

## Streamlit App (`streamlit/app.py`)

- Must work on both Snowpark (native Snowflake) and connector (local dev) backends.
- Snowflake's bundled Streamlit is older than the latest version. Avoid APIs added after ~v1.24:
  - `st.rerun()` — use `try/except` fallback to `st.experimental_rerun()`
  - `st.date_input(value=None)` — does not render as blank; use checkbox toggle instead
- Parallel execution uses `ThreadPoolExecutor` with `_get_new_connection()` per thread.
- Keep max parallelism <= 20 (Snowflake's DML lock waiter limit on shared `DCQ_RESULTS` table).

## Deploying Changes

- **Full deploy**: `python push_snowflake_assets.py` from repo root. Deploys all SQL, data, and the Streamlit app.
- **Procedure-only deploy**: The push script reads SQL files via `execute_sql_file()` which strips double-quotes from the procedure header (before `AS`) but leaves the JS body intact.
- **Streamlit-only deploy**: PUT `streamlit/app.py` to `@STREAMLIT.PUBLIC.streamlit_deploy_stage`, then `CREATE OR REPLACE STREAMLIT`.
- **Data updates**: Update the `.json.gz` file in `snowflake/edc_ref/`, then run `push_snowflake_assets.py` or load manually via PUT + COPY INTO.

## Adding a New Check

1. Create `sql/dcq_checks/procedures/SP_DC_X_YY.sql` with the standard signature.
2. Add a row to `snowflake/DCQ_CHECK_REGISTRY_data_full.json` with CHECK_ID, CHECK_NAME, ROW_NUM, PART, EDC_TABLE, DESCRIPTION, PROC_NAME, SOURCE_TABLES, ENABLED, and documentation fields.
3. Run `python push_snowflake_assets.py` to deploy both.
4. No changes needed in `sp_run_dcq.sql` or `app.py` — the registry drives everything.

## Potential Code Errors Procedure (`potential_code_errors.sql`)

- Located at `sql/dcq_checks/procedures/potential_code_errors.sql`.
- Standalone procedure (not part of the DCQ registry/orchestrator). Called directly or from the Streamlit app's second tab.
- Signature: `(DB_PARAM, SCHEMA_NAME, TABLE_LIST DEFAULT 'ALL')`. Uses `EXECUTE AS OWNER`.
- Validates medical codes (ICD-9, ICD-10, CPT/HCPCS, NDC, RXNORM, LOINC, SNOMED) against structural heuristics.
- Tables validated: DIAGNOSIS, PROCEDURES, PRESCRIBING, DISPENSING, MED_ADMIN, LAB_RESULT_CM, CONDITION, IMMUNIZATION, OBS_GEN, OBS_CLIN.
- For each table creates `<TABLE>_VALIDATION` (all codes) and `BAD_<TABLE>` (codes failing rules), plus a final `CODE_SUMMARY` rollup.
- Adding a new table requires changes in 4 places: (1) the ALL array, (2) drop table block, (3) validation+bad table section, (4) base_counts and bad_counts summary queries.

## Git & Remotes

- **Branch**: `main`
- **Bitbucket** (origin): `git@bitbucket.org:bmi-ufl/characterization_on_snowflake.git`
- **GitHub**: `git@github.com:uf-hobi-informatics-lab/pcornet_characterization_on_snoflake.git`
- Push to both remotes: `git push origin main && git push github main`
- GitHub may have divergent commits from other contributors — pull before pushing if needed.

## Updating the Codebase Summary

Update `.opencode/codebase-summary.md` when:
- A new check procedure is added or an existing one is significantly changed
- The architecture changes (new execution model, new tables, new parameters)
- A known issue is resolved or a new one is discovered
- Reference data is updated (CDM version, LOINC, RxNorm, etc.)

Add entries to the **Change History** section (newest first) with date, what changed, and why.
