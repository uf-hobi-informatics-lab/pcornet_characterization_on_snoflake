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

1. **Registry-driven**: `CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY` (49 rows) is the source of truth for all checks
2. **Setup procedure**: `CHARACTERIZATION.DCQ.SP_RUN_DCQ` (11 params) creates output tables, inserts run/log rows, queries the registry, and **returns a JSON payload** with the run_id and check list
3. **Parallel execution**: The Streamlit app parses the JSON and uses Python `concurrent.futures.ThreadPoolExecutor` with one dedicated Snowflake connection per thread to run checks concurrently
4. **Each check procedure** (`SP_DC_X_YY`) validates a specific aspect of CDM data and writes metric rows to `<DB_PARAM>.CHARACTERIZATION_DCQ.DCQ_RESULTS`
5. **Progress tracking**: Each thread updates `DCQ_CHECK_LOG` (PENDING -> RUNNING -> SUCCEEDED/FAILED). The Streamlit UI shows a real-time progress bar.

### 4 Check Parts (49 procedures total)

- **Part 1** (DC 1.01-1.21): Structural integrity (tables exist, columns match spec, PKs unique, value sets, orphans)
- **Part 2** (DC 2.01-2.09): Temporal & plausibility (future dates, extreme values, illogical date relationships)
- **Part 3** (DC 3.01-3.18): Completeness + cross-table (missing/unknown values across fields; DC 3.18 = XTBL_L3_CFIPSZIP_5Y)
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

All 49 checks write to the shared `DCQ_RESULTS` table. Snowflake has a 20-waiter lock limit on DML, so max parallelism is capped at 20 in the UI (default 10).

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
│   │       ├── SP_DC_1_01.sql    # 49 check procedures (JavaScript, EXECUTE AS CALLER)
│   │       ├── SP_DC_1_02.sql    #   All share same 8-param signature:
│   │       ├── ...               #   (DB_PARAM, SCHEMA_NAME, RUN_ID, TARGET_TABLE,
│   │       ├── SP_DC_3_18.sql    #    PREV_DB_PARAM, PREV_SCHEMA_NAME, START_DATE, END_DATE)
│   │       ├── SP_DC_4_03.sql
│   │       └── potential_code_errors.sql  # Medical code validation (ICD, CPT, NDC, LOINC, SNOMED)
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
    ├── DCQ_CHECK_REGISTRY_data_full.json  # 49 check definitions (JSON)
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
| `sql/dcq_checks/procedures/SP_DC_*.sql` | ~100-300 each | Individual check procedures (49 files) |
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

## Resume State

> Update this section at the start/end of every session. Contains active-work context that would be lost when a session drops.

**As of 2026-07-15 (session paused — pre-deploy backups complete, ready to deploy):**

- **Branch**: `add-dc-3-18-and-dc-1-13-snomed`, commit `45eea6d` — pushed to `origin` (GitHub, the only remote — AGENTS.md's Bitbucket reference is stale/wrong)
- **Working tree**: 69 files show as modified but are file-mode flips only (100644 -> 100755). Zero content changes. Safe to ignore, or run `git config core.fileMode false` locally to hide them.
- **Untracked**: `.opencode/package.json`, `.opencode/package-lock.json` (not investigated, probably OpenCode-managed).

**What this deploy changes:** Commit `45eea6d` touched exactly 4 Snowflake artifacts:
1. `SP_DC_3_18` — NEW procedure (XTBL_L3_CFIPSZIP_5Y)
2. `SP_DC_3_03` — added `PRO_CODE` to PRO_CM required-not-null list
3. `POTENTIAL_CODE_ERRORS` — SNOMED expansion (DIAGNOSIS/CONDITION/LAB_RESULT_CM/OBS_CLIN)
4. `DCQ_CHECK_REGISTRY` — new DC_3_18 row + updated DC_1_13 SOURCE_TABLES (48 -> 49 rows)

**Deploy tooling (all committed to repo in `45eea6d`):**
- `deploy_selective.py` — focused deploy of ONLY the 4 artifacts above (does NOT touch other 46 procs, Streamlit app, or ref tables). Uses same `.env` / RSA key auth as `push_snowflake_assets.py`.
- `rollback/01_capture_current_state.sql` — pre-deploy backup script (already run).
- `rollback/02_restore_if_broken.sql` — recovery script (only needed if deploy breaks).

**Pre-deploy backups — COMPLETE:**
- [x] Ran `GET_DDL` on `SP_DC_3_03(VARCHAR x8)` in Snowsight — user should save output to `rollback/captured_20260715_dc_3_03.sql` (needs confirmation).
- [x] Ran `GET_DDL` on `POTENTIAL_CODE_ERRORS(VARCHAR,VARCHAR,VARCHAR)` in Snowsight — user should save output to `rollback/captured_20260715_pce.sql` (needs confirmation).
- [x] Ran `CREATE OR REPLACE TABLE CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY_BACKUP_20260715 CLONE CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY;`
- [x] Verified backup row count = **48** (matches expected pre-deploy count).

**NEXT STEPS — resume here:**

1. **(User task)** Confirm the two `GET_DDL` outputs were saved to files in `rollback/` — check terminal scrollback / Snowsight worksheet history.

2. **Dry-run the deploy:**
   ```
   python deploy_selective.py --dry-run
   ```
   Expected: no errors, prints what would happen, does not touch Snowflake.

3. **Real deploy:**
   ```
   python deploy_selective.py
   ```
   Runs 4 operations in order: CREATE OR REPLACE SP_DC_3_18, SP_DC_3_03, POTENTIAL_CODE_ERRORS, then TRUNCATE + COPY INTO on the registry from `snowflake/DCQ_CHECK_REGISTRY_data_full.json`. Verifies final row count = 49.

4. **Post-deploy smoke SQL in Snowsight:**
   ```sql
   SELECT COUNT(*) FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY;                    -- expect 49
   SELECT CHECK_NAME, PROC_NAME, ENABLED FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY
     WHERE CHECK_NAME IN ('DC_3_18','DC_1_13','DC_3_03');
   SHOW PROCEDURES LIKE 'SP_DC_3_18' IN SCHEMA CHARACTERIZATION.DCQ_CHECKS;
   ```

5. **Direct-call each of the 3 procs against a real target DB** (user has NOT yet picked which — likely a `CHAR_*` or `STAGE_*` database and its CDM schema, probably `PUBLIC`). Use `TEST_3_18`, `TEST_3_03`, `TEST_PCE` as RUN_IDs. Templates in the previous session's message:
   - `CALL CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_18('<DB>','<SCHEMA>','TEST_3_18','ALL',NULL,NULL,NULL,NULL);`
   - `CALL CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_03('<DB>','<SCHEMA>','TEST_3_03','PRO_CM',NULL,NULL,NULL,NULL);`
   - `CALL CHARACTERIZATION.DCQ_CHECKS.POTENTIAL_CODE_ERRORS('<DB>','<SCHEMA>','ALL');`

6. **Query the result tables** to confirm output shape:
   - `SELECT * FROM <DB>.CHARACTERIZATION_DCQ.DCQ_RESULTS WHERE RUN_ID LIKE 'TEST_%';`
   - `SELECT * FROM <DB>.CHARACTERIZATION.CODE_SUMMARY WHERE SOURCE_TABLE IN ('LAB_RESULT_CM','OBS_CLIN') ORDER BY SOURCE_TABLE, CODE_TYPE;` — should show new SNOMED (`SM`) code_type rows.

7. **Streamlit end-to-end test:** Snowsight -> Streamlit -> `characterization_runner` (or `streamlit run streamlit/app.py` locally). Tab 1 (SP_RUN_DCQ), Mode=`ROW_NUM`, run each of `3.18`, `1.13`, `3.03` individually with `Max parallel=1`. Confirm the progress bar completes and results tab shows rows. No `streamlit/app.py` redeploy needed — app was not touched by this commit and reads registry at runtime.

8. **(Optional) Full-registry parallel run** with Mode=`ALL`, Max parallel=10, to confirm no regressions on the other 46 checks. Expect 49 SUCCEEDED (or 48 + 1 known-broken SP_DC_2_07 failure — see Known Issues).

9. **Merge + housekeeping:**
   - `git checkout main && git merge --no-ff add-dc-3-18-and-dc-1-13-snomed && git push origin main`
   - Delete branch on GitHub.
   - Once satisfied: `DROP TABLE CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY_BACKUP_20260715;`

**Rollback recipes if deploy breaks something:**
- **Proc SQL bad:** Nothing to do — Snowflake's CREATE OR REPLACE is atomic; the OLD version stays deployed if the new SQL is malformed.
- **Proc has a logic bug that survives compilation:** paste the captured `GET_DDL` output back and run it in Snowsight.
- **Registry corrupted/empty after TRUNCATE + COPY failure:** run `rollback/02_restore_if_broken.sql` (swaps `DCQ_CHECK_REGISTRY` back from the clone).
- **Full undo of the whole deploy:** `DROP PROCEDURE SP_DC_3_18(VARCHAR x8);` + paste both captured DDLs + run `02_restore_if_broken.sql`.

**Doc drift noted (fix later, not urgent):**
- `AGENTS.md` "Git & Remotes" section references a Bitbucket remote that doesn't exist. Only remote is GitHub `origin`.

## Change History

> Newest entries first.

- **2026-07-16**: **DC 3.03 full port to SAS v16.** Rewrote DC 3.03 as two distinct checks matching the SAS report structure. Not yet deployed (repo-only session).
  - **New procs:** `SP_DC_3_03_IVC` (SAS Table IVC, required tables) and `SP_DC_3_03_IVD` (SAS Table IVD, optional tables). Both auto-generated from `sas_code/parse_dc_3_03_catalog.py` reading `sas_code/extracted_newest/infolder/edc_report.sas`.
  - **IVC scope:** 52 fields across DEMOGRAPHIC (17), ENROLLMENT (2), ENCOUNTER (15), DIAGNOSIS (10), PROCEDURES (8). Flag list (10 fields): BIRTH_DATE, SEX, DX_ORIGIN, DX_SOURCE, DIA_ENCOUNTERID (ENCOUNTERID in DIAGNOSIS), DISCHARGE_DATE, DISCHARGE_DISPOSITION, PRO_ENCOUNTERID (ENCOUNTERID in PROCEDURES), PX_DATE, PX_SOURCE.
  - **IVD scope:** 161 fields across 17 tables. Flag list (13 fields): DEATH_SOURCE, ENCOUNTERID, LAB_RESULT_SOURCE, MEDADMIN_ENCOUNTERID, MEDADMIN_CODE, MEDADMIN_SOURCE, RX_ENCOUNTERID, RX_ORDER_DATE, RX_SOURCE, PRO_CODE, PRO_SOURCE, VITAL_ENCOUNTERID, VITAL_SOURCE.
  - **Missing-value semantics fixed:** each field is categorized by SAS numerator clause into one of six "kinds": `category` (NULL/blank + NI/UN/OT), `null_n` (NULL/blank only, for ID/FK cols), `nmiss` (NULL only, for dates/numerics), `null_or_missing`/`cross_null_or_missing`/`stat_null_or_missing` (NULL/blank variants for specific SAS clauses). Previous SP_DC_3_03 only counted NULL/blank, missing all NI/UN/OT — which was why SAS-flagged DISCHARGE_DISPOSITION failures never appeared in the app.
  - **Denominator filter:** 6 ENCOUNTER fields (DISCHARGE_DATE, DISCHARGE_DISPOSITION, DISCHARGE_STATUS, DRG, ADMITTING_SOURCE, PDX) restrict denominator to `ENC_TYPE in ('IP','EI')` per SAS, both in numerator and denominator. Previous proc had no such filter.
  - **Threshold:** flag when `MISSING_PCT >= 10` AND field is in the SAS-specified flag list. Previous proc flagged any `> 0%` for all fields.
  - **Registry:** replaced the two duplicate `DC_3_03` rows (same CHECK_ID, different EDC_TABLE) with distinct `DC_3_03_IVC` and `DC_3_03_IVD` rows. Registry still 49 rows. Duplicate CHECK_ID / same-name race condition eliminated.
  - **Legacy:** old `SP_DC_3_03.sql` moved to `sql/dcq_checks/procedures/_archive/` (kept for reference). Deploy script (`deploy_selective.py`) updated to CREATE OR REPLACE the two new procs and `DROP PROCEDURE IF EXISTS SP_DC_3_03(VARCHAR x8)` in Snowflake.
  - **Files added:** `sas_code/parse_dc_3_03_catalog.py`, `sas_code/dc_3_03_catalog.json`, `sas_code/dc_3_03_ivc_catalog.md`, `sql/dcq_checks/procedures/SP_DC_3_03_IVC.sql`, `sql/dcq_checks/procedures/SP_DC_3_03_IVD.sql`, `sas_code/infolder_newest.zip`.
  - **Files modified:** `snowflake/DCQ_CHECK_REGISTRY_data_full.json` (DC_3_03 rows replaced), `deploy_selective.py` (proc list + DROP step).
  - **Files moved:** `sql/dcq_checks/procedures/SP_DC_3_03.sql` -> `sql/dcq_checks/procedures/_archive/SP_DC_3_03.sql`.
- **2026-07-15**: **DC 3.03 PRO_CODE added.** SAS v16 `edc_report.sas` added `PRO_CODE` (and `PRO_SOURCE`) to the Table IVD threshold-flag list. Ported the intent to Snowflake by adding `PRO_CODE` to the `PRO_CM` required-not-null list in `SP_DC_3_03.sql`. PRO_SOURCE not added (per user). *[SUPERSEDED 2026-07-16 by the full IVC/IVD port.]*
- **2026-07-15**: **DC 3.18 added.** New check `SP_DC_3_18` implements SAS `XTBL_L3_CFIPSZIP_5Y` (Table IB row 3.18). Cohort = PATIDs with F2F encounters (ENC_TYPE in ED/EI/IP/OS/AV) in past 5 years (ADMIT_DATE in [today-63mo, today-3mo]) joined to LDS_ADDRESS_HISTORY rows with CURRENT_ADDRESS_FLAG='Y'. Emits patient-level counts (PATIENTS_N, ADDRESS_ZIP5_N, COUNTY_FIPS_N, BOTH_N), derived PCT_BOTH, and BOTH_FIPSZIP_FLAG (fires when PATIENTS_N>0 AND (PCT_BOTH<80 OR BOTH_N=0)). Registry row `DC_3_18` added. Registry now 49 rows.
- **2026-07-15**: **DC 1.13 SNOMED expansion (SAS PCE v16).** `potential_code_errors.sql` extended:
  - DIAGNOSIS: filter now `dx_type IN ('09','10','SM')`; adds SNOMED heuristic (length 6-18, no alpha, not all-zeros/all-nines).
  - CONDITION: filter now `condition_type IN ('09','10','SM')`; adds SNOMED heuristic.
  - LAB_RESULT_CM: now reads both `LAB_LOINC` (code_type='LC') and `RESULT_SNOMED` (code_type='SM') via UNION ALL; BAD_LAB SNOMED heuristic added.
  - OBS_CLIN: now reads both `OBSCLIN_CODE` (typed LC/SM) and `OBSCLIN_RESULT_SNOMED` (always SM) via UNION ALL. BAD_OBS_CLIN and BAD_OBS_GEN SNOMED string rule tightened to SAS-faithful all-zeros/all-nines check.
  - `DC_1_13` registry `SOURCE_TABLES` expanded to include LAB_RESULT_CM, OBS_CLIN, OBS_GEN (so they are selectable via SOURCE_TABLE mode).
- **2026-05-13**: Added OBS_GEN and OBS_CLIN table validation to `potential_code_errors.sql`. Validates LOINC (LC) and SNOMED (SM) codes in both tables. Updated ALL table list, drop logic, validation sections, and summary queries.
- **2025-03-09**: Updated CDM_PARSEABLE_RAW to PCORnet CDM v7.0 from `2025_05_01_PCORnet_Common_Data_Model_v7dot0_parseable.xlsx` (15,908 rows)
- **2025-03-09**: DC 1.09 threshold updated — orphan ENCOUNTERID flag now requires >= 5% (was > 0). Updated procedure, registry THRESHOLDS and DOC_INTERPRETATION.
- **2025-03-09**: Moved parallel execution from Snowflake Tasks to Python ThreadPoolExecutor. SP_RUN_DCQ refactored to setup-only (returns JSON). Each check runs on its own Snowflake connection.
- **2025-03-09**: Added MAX_PARALLEL parameter (11th) to SP_RUN_DCQ. Streamlit UI gets parallel slider (1-20, default 10), 10-year lookback checkbox, date filter toggle, schema defaults to PUBLIC.
- **2025-03-09**: Initial parallelization attempts via Snowflake Tasks (abandoned due to nested task limitation and scheduler throttling).
