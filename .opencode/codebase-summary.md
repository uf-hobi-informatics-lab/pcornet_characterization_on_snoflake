# Codebase Summary

> Context file for AI coding assistants. Read this first.

## Project Identity

- **Name**: PCORnet CDM Data Quality Characterization on Snowflake
- **Purpose**: Implements the PCORnet Data Curation Query (DCQ) package to validate clinical data in the PCORnet Common Data Model (CDM) across the OneFlorida+ Clinical Research Network
- **Team**: UF Health Outcomes & Biomedical Informatics (HOBI) Lab
- **Standards**: PCORnet CDM v7.0 (as of 2025-05-01)

## Tech Stack

- **Languages**: Python 3.13, JavaScript (Snowflake stored procedures), SQL
- **Frameworks**: Streamlit (runs natively in Snowflake via Snowpark)
- **Database**: Snowflake (all data, procedures, and the Streamlit app live in Snowflake)
- **Auth**: RSA key pair authentication (private key path in `.env`)
- **Dependencies**: `snowflake-connector-python`, `snowflake-snowpark-python`, `streamlit`, `openpyxl`, `python-dotenv`, `cryptography`

## Architecture

### Execution Model

1. **Registry-driven**: `CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY` (48 rows) is the source of truth for all checks
2. **Setup procedure**: `CHARACTERIZATION.DCQ.SP_RUN_DCQ` (11 params) creates output tables, inserts run/log rows, queries the registry, and **returns a JSON payload** with the run_id and check list
3. **Parallel execution**: The Streamlit app parses the JSON and uses Python `concurrent.futures.ThreadPoolExecutor` with one dedicated Snowflake connection per thread to run checks concurrently
4. **Each check procedure** (`SP_DC_X_YY`) validates a specific aspect of CDM data and writes metric rows to `<DB_PARAM>.CHARACTERIZATION_DCQ.DCQ_RESULTS`
5. **Progress tracking**: Each thread updates `DCQ_CHECK_LOG` (PENDING -> RUNNING -> SUCCEEDED/FAILED). The Streamlit UI shows a real-time progress bar.

### 4 Check Parts (48 procedures total)

- **Part 1** (DC 1.01-1.21): Structural integrity (tables exist, columns match spec, PKs unique, value sets, orphans)
- **Part 2** (DC 2.01-2.09): Temporal & plausibility (future dates, extreme values, illogical date relationships)
- **Part 3** (DC 3.01-3.17): Completeness (missing/unknown values across fields)
- **Part 4** (DC 4.01-4.03): Cross-refresh comparison (record count changes, distribution shifts)

### Dual Backend

- **Snowpark** (native Snowflake Streamlit): Uses `get_active_session()`, `?` bind placeholders
- **Connector** (local dev): Uses `snowflake-connector-python` from `.env`, `%s` bind placeholders

### Output Schema

All results go to `<DB_PARAM>.CHARACTERIZATION_DCQ` with three tables:
- `DCQ_RUNS` — one row per execution run (RUN_ID, STATUS, timestamps)
- `DCQ_CHECK_LOG` — one row per check per run (status tracking)
- `DCQ_RESULTS` — metric rows written by each check procedure

### Lock Contention Constraint

All 48 checks write to the shared `DCQ_RESULTS` table. Snowflake has a 20-waiter lock limit on DML, so max parallelism is capped at 20 in the UI (default 10).

## Directory Map

```
characterization_on_snowflake/
├── .env                          # Snowflake credentials (not committed)
├── .gitignore
├── .opencode/
│   └── codebase-summary.md       # This file
├── opencode.json                 # OpenCode config
├── AGENTS.md                     # AI coding conventions
├── requirements.txt              # Python dependencies
├── push_snowflake_assets.py      # Deploy SQL + data + Streamlit to Snowflake
├── pull_snowflake_assets.py      # Reverse-sync Snowflake objects to local repo
├── rebuild_characterization.py   # Full database rebuild from scratch
│
├── streamlit/
│   └── app.py                    # Main Streamlit UI (~700 lines)
│                                 #   - Parallel check executor (ThreadPoolExecutor)
│                                 #   - _get_new_connection() for per-thread connections
│                                 #   - run_query() / run_async_call() helpers
│                                 #   - Two tabs: SP_RUN_DCQ runner, POTENTIAL_CODE_ERRORS
│
├── sql/
│   ├── dcq/
│   │   └── procedures/
│   │       └── sp_run_dcq.sql    # Setup-only orchestrator (returns JSON)
│   │                             #   Signature: 11 params (DB_PARAM through MAX_PARALLEL)
│   ├── dcq_checks/
│   │   └── procedures/
│   │       ├── SP_DC_1_01.sql    # 48 check procedures (JavaScript, EXECUTE AS CALLER)
│   │       ├── SP_DC_1_02.sql    #   All share same 8-param signature:
│   │       ├── ...               #   (DB_PARAM, SCHEMA_NAME, RUN_ID, TARGET_TABLE,
│   │       ├── SP_DC_4_03.sql    #    PREV_DB_PARAM, PREV_SCHEMA_NAME, START_DATE, END_DATE)
│   │       └── potential_code_errors.sql  # Medical code validation (ICD, CPT, NDC, LOINC)
│   └── edc_ref/
│       ├── tables/               # DDL for reference tables
│       │   ├── CDM_PARSEABLE_RAW.sql
│       │   ├── LOINC_REF_RAW.sql
│       │   ├── REQUIRED_STRUCTURE_RAW.sql
│       │   ├── RXNORM_CUI_REF_RAW.sql
│       │   └── TBL_IVI_REF_RAW.sql
│       └── views/
│           ├── CDM_CONSTRAINTS.sql
│           └── CDM_VALUESETS.sql
│
└── snowflake/
    ├── DCQ_CHECK_REGISTRY.sql          # Registry table DDL
    ├── DCQ_CHECK_REGISTRY_data_full.json  # 48 check definitions (JSON)
    └── edc_ref/
        ├── CDM_PARSEABLE_RAW_data.json.gz   # PCORnet CDM v7.0 spec (15,908 rows)
        ├── LOINC_REF_RAW_data.json.gz       # LOINC reference codes
        ├── REQUIRED_STRUCTURE_RAW_data.json.gz
        ├── RXNORM_CUI_REF_RAW_data.json.gz  # RxNorm CUI reference
        └── TBL_IVI_REF_RAW_data.json.gz
```

## Entry Points

| Command | What it does |
|---------|-------------|
| `python push_snowflake_assets.py` | Full deploy: procedures + data + Streamlit app to Snowflake |
| `python pull_snowflake_assets.py` | Reverse-sync: download Snowflake objects to local repo |
| `python rebuild_characterization.py` | Full database rebuild from scratch (destructive) |
| Snowsight > Streamlit > `characterization_runner` | Run the app in Snowflake (`STREAMLIT.PUBLIC.characterization_runner`) |
| `streamlit run streamlit/app.py` | Local dev mode (requires `.env` with credentials) |

## Snowflake Environment

- **Role**: `1FL-AZURE-DB-USERS`
- **Databases**: `CHARACTERIZATION` (procedures/registry/ref), `STREAMLIT` (Streamlit app), `CHAR_*`/`STAGE_*`/`PROD_*` (target data)
- **Key schemas**: `CHARACTERIZATION.DCQ` (orchestrator + registry), `CHARACTERIZATION.DCQ_CHECKS` (check procedures), `CHARACTERIZATION.EDC_REF` (reference tables)
- **Warehouses**: `CHARACTERIZATION_*` prefixes (XS, SCALED_SMALL, etc.), `STREAMLIT_XS` for the app
- **Streamlit**: `STREAMLIT.PUBLIC.characterization_runner` on `STREAMLIT_XS`
- **EXECUTE TASK privilege**: Required at account level for the role

## Key Files Quick Reference

| File | Lines | Purpose |
|------|-------|---------|
| `streamlit/app.py` | ~700 | Main UI, parallel executor, progress tracking |
| `sql/dcq/procedures/sp_run_dcq.sql` | ~200 | Setup-only orchestrator, returns JSON check list |
| `sql/dcq_checks/procedures/SP_DC_*.sql` | ~100-300 each | Individual check procedures (48 files) |
| `push_snowflake_assets.py` | ~170 | Deploy script (strips header quotes, loads JSON data) |
| `snowflake/DCQ_CHECK_REGISTRY_data_full.json` | ~2000 | Check definitions with metadata, thresholds, docs |

## Notable Design Decisions

1. **Setup-only orchestrator**: `SP_RUN_DCQ` returns JSON instead of executing checks. This avoids Snowflake's nested task limitation and task scheduler throttling (~3 concurrent). The Streamlit app handles parallel execution via Python threads.

2. **Per-thread connections**: Each parallel check gets its own `snowflake.connector.connect()` for true parallelism. Sharing a single connection bottlenecks to serial execution.

3. **Lock contention cap**: Max parallelism capped at 20 (default 10) because all checks INSERT into the shared `DCQ_RESULTS` table and Snowflake's 20-waiter lock limit causes failures above that.

4. **Adaptive procedure signatures**: The orchestrator uses `SHOW PROCEDURES` to detect each check's arity and passes only the relevant args (3, 4, 6, or 8 params).

5. **Idempotent checks**: Each check DELETEs prior results for the same RUN_ID before inserting, so reruns are safe.

6. **Bidirectional sync**: `push_snowflake_assets.py` and `pull_snowflake_assets.py` keep local repo and Snowflake in sync. The push script strips double-quotes from procedure headers but leaves JS bodies intact.

7. **Registry-driven**: Adding a new check = add a procedure SQL file + add a row to `DCQ_CHECK_REGISTRY_data_full.json`. No code changes needed in the orchestrator or UI.

## Known Issues

1. **SP_DC_2_07**: SQL bug — `ambiguous column name 'ADMIT_DATE'`. Fails every run. Needs fix in `sql/dcq_checks/procedures/SP_DC_2_07.sql`.

2. **Snowpark thread safety**: `_get_new_connection()` in `app.py` uses `.env` which doesn't exist in native Snowflake Streamlit. For Snowpark mode, threads share the session. May limit parallelism in native mode.

3. **Unused code**: `run_async_call()` and `run_query_threadsafe()` in `app.py` are no longer used and can be cleaned up.

4. **Streamlit version compatibility**: Snowflake's bundled Streamlit runtime is older. `st.rerun()` requires fallback to `st.experimental_rerun()`. `st.date_input(value=None)` doesn't work (date pickers default to today).

## Change History

> Newest entries first.

- **2025-03-09**: Updated CDM_PARSEABLE_RAW to PCORnet CDM v7.0 from `2025_05_01_PCORnet_Common_Data_Model_v7dot0_parseable.xlsx` (15,908 rows)
- **2025-03-09**: DC 1.09 threshold updated — orphan ENCOUNTERID flag now requires >= 5% (was > 0). Updated procedure, registry THRESHOLDS and DOC_INTERPRETATION.
- **2025-03-09**: Moved parallel execution from Snowflake Tasks to Python ThreadPoolExecutor. SP_RUN_DCQ refactored to setup-only (returns JSON). Each check runs on its own Snowflake connection.
- **2025-03-09**: Added MAX_PARALLEL parameter (11th) to SP_RUN_DCQ. Streamlit UI gets parallel slider (1-20, default 10), 10-year lookback checkbox, date filter toggle, schema defaults to PUBLIC.
- **2025-03-09**: Initial parallelization attempts via Snowflake Tasks (abandoned due to nested task limitation and scheduler throttling).
