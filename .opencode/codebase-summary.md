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

**As of 2026-07-20 end-of-session (DC 1.22 added + deployed + verified; commit + push pending):**

- **Branch**: `add-dc-3-18-and-dc-1-13-snomed`
- **Latest local commit**: `66f241d` "Fix DC 3.18 denominator: use F2F 5yr cohort (SAS denom_fipszip)". Today's DC 1.22 work still uncommitted — one bundled commit + push pending.
- **Origin state**: last pushed commit is still `ad05f23` (DC 3.03 IVC/IVD port).
- **`main`**: unchanged.

**Session work completed (2026-07-20):**
- Added `sql/dcq_checks/procedures/SP_DC_1_22.sql` — new check for SAS row 11 (LDSADRS_L3_CURRENT category=Y).
- Added `DC_1_22` row to `snowflake/DCQ_CHECK_REGISTRY_data_full.json`. Registry now 50 rows.
- Added `SP_DC_1_22.sql` to `deploy_selective.py` PROC_FILES + updated docstring.
- Session notes: `.opencode/session-2026-07-20-dc-1-22.md`.
- **DEPLOYED** via `python deploy_selective.py` — all 5 procs CREATE OR REPLACE succeeded, registry reloaded to 50 rows.
- **SMOKE-TESTED** on 3 datasets:
  - `CHAR_UFH.PUBLIC`: 942,572 rows / 942,572 patids / 0 excess — no fire (matches independent SQL sanity)
  - `CHAR_UAB.PUBLIC`: 1,461,185 / 1,461,185 / 0 — no fire (matches independent SQL sanity)
  - `CHAR_UAB.OVID_20260612`: 1,390,992 / 1,383,960 / 6,966 patids with multiple / 7,032 excess — **FIRES** (matches independent SQL sanity)
  - Cleanup executed: 6 rows in `CHAR_UFH` + 12 rows in `CHAR_UAB` deleted from DCQ_RESULTS for TEST_1_22_* RUN_IDs.

**Snowflake state (post-deploy 2026-07-20):**
- Deployed: `SP_DC_3_18` (with fix), `SP_DC_3_03_IVC`, `SP_DC_3_03_IVD`, `SP_DC_1_22`, `POTENTIAL_CODE_ERRORS`.
- Legacy `SP_DC_3_03(VARCHAR x8)` and stray `SP_DC_3_03(VARCHAR x4)` both dropped.
- Registry `CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY` = **50 rows**.
- Backup clone `DCQ_CHECK_REGISTRY_BACKUP_20260715` still present (48 rows). Drop after confidence period.

**Verified against SAS (CHAR_UFH.PUBLIC is user's SAS-reference dataset):**
- DC 3.18: PATIENTS_N=1,521,233, BOTH_N=302,041, PCT_BOTH=19.86%, FLAG=1 — matches SAS Table IB row 3.18.
- DC 3.03 IVC (against CHAR_UAB.OVID_20260612): DISCHARGE_DISPOSITION now flagged at 10.02% missing — matches SAS.
- POTENTIAL_CODE_ERRORS: 0 issues found; SNOMED (SM) rows now populated for CONDITION/LAB_RESULT_CM/OBS_CLIN/OBS_GEN.
- **DC 1.22**: SAS predicate `z10_record > z10_distinct` translated to SQL exactly; independent SQL sanity queries confirm proc emits correct counts on all 3 tested datasets.

**Uncommitted repo changes (kept intentionally, will bundle with tomorrow's DC 1.22 work):**
- `deploy_selective.py` — added optional passphrase-file support (`SNOWFLAKE_PRIVATE_KEY_PASSPHRASE_FILE` and `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` env vars). Backwards compatible — no-op when neither is set.
- `.opencode/session-2026-07-16-dc-3-03-port.md` — rewritten with today's full session (deploy + verification + DC 3.18 fix + resume plan).
- `.opencode/package.json`, `.opencode/package-lock.json` — untracked, OpenCode-managed, ignore.

**Environmental prerequisites (already set up, verify on resume):**
- `.env` at repo root with `SNOWFLAKE_USER=allison_burns_opencode`.
- RSA key at `/processing/home/allison.burns/.snowflake/rsa_key_opencode.p8` (unencrypted, perms 600).
- Public key registered in Snowflake (fingerprint `Jw+HkT/YN7eY+dxpVK4mKOMIhp+54XD7FK4H1vx1KXA=`).
- Quick connectivity smoke test:
  ```
  python3 -c "import sys; sys.path.insert(0,'.'); import deploy_selective as ds; conn=ds.get_connection(); cs=conn.cursor(); cs.execute('SELECT CURRENT_USER(), CURRENT_ROLE()'); print(cs.fetchone())"
  ```
  Expected: `('ALLISON_BURNS_OPENCODE', '1FL-AZURE-DB-USERS')`

**TOMORROW'S PLAN — add DC 1.22:**

1. **Identify SAS source for DC 1.22.** Search:
   ```
   grep -in "1\.22\|row=1\.22" sas_code/extracted_newest/infolder/edc_report.sas
   ```
2. **Choose a template proc** structurally similar to DC 1.22 requirements (candidates: `SP_DC_3_18.sql` for XTBL/threshold-flag pattern, `SP_DC_1_13.sql` for table-list-driven checks, `SP_DC_3_03_IVC.sql` for per-field-metric checks).
3. **Create `sql/dcq_checks/procedures/SP_DC_1_22.sql`** following the 8-param signature and standard patterns. Remember: single quotes inside `AS '...'` body must be doubled (`''`).
4. **Add registry row to `snowflake/DCQ_CHECK_REGISTRY_data_full.json`** — 50 rows total after this.
5. **Update `deploy_selective.py`'s `PROC_FILES`** to include SP_DC_1_22.sql.
6. **Deploy:** dry-run then real deploy.
7. **Test-call:** `CALL CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_22('CHAR_UFH','PUBLIC','TEST_1_22','ALL',NULL,NULL,NULL,NULL);` — compare against SAS output.
8. **Streamlit end-to-end test** via `characterization_runner`.
9. **Commit + push:** bundle `SP_DC_1_22.sql` + registry JSON + `deploy_selective.py` passphrase support + session notes rewrite + today's `66f241d` in the push. Push `add-dc-3-18-and-dc-1-13-snomed` to origin.
10. **Merge to `main`** and delete branch (after user confirms).

**Rollback recipes if a future deploy breaks something:**
- **Proc SQL bad:** Snowflake's CREATE OR REPLACE is atomic; malformed SQL means old version stays.
- **Registry corrupted after TRUNCATE + COPY:** run `rollback/02_restore_if_broken.sql` (restore from clone).
- **Full undo of DC 3.03 port:** paste `sql/dcq_checks/procedures/_archive/SP_DC_3_03.sql` in Snowsight, DROP the IVC/IVD procs, restore registry from clone.

**Doc drift noted (fix later, not urgent):**
- `AGENTS.md` "Git & Remotes" section references a Bitbucket remote that doesn't exist. Only remote is GitHub `origin`.

## Change History

> Newest entries first.

- **2026-07-20**: **DC 1.22 added.** New check `SP_DC_1_22` implements SAS row 11 / `LDSADRS_L3_CURRENT` category=Y (`edc_report.sas:4287,4298,4312,4613-4640`). Description: *"Patients have multiple current physical addresses in the LDS_ADDRESS_HISTORY table"* — fires when at least one PATID has multiple LDS_ADDRESS_HISTORY rows with `CURRENT_ADDRESS_FLAG='Y'`. Direct SAS predicate translation: flag if `COUNT(*) > COUNT(DISTINCT PATID)` in the `CURRENT_ADDRESS_FLAG='Y'` slice (SAS: `if z10_distinct < z10_record`). Emits 5 metrics: `CURRENT_ROW_N`, `CURRENT_PATID_DISTINCT_N`, `PATIDS_WITH_MULTIPLE_N`, `EXCESS_ROWS_N`, `MULTI_CURRENT_FLAG`. Threshold: `excess_rows_n_gt = 0`. Signature: standard 8-param; TARGET_TABLE accepts `ALL` or `LDS_ADDRESS_HISTORY`. Registry row `DC_1_22` added with `EDC_TABLE='Table IID'`, `PART='part1'`. Registry now 50 rows. `deploy_selective.py` PROC_FILES + docstring updated to include the new proc. Session notes: `.opencode/session-2026-07-20-dc-1-22.md`.
- **2026-07-16 (evening)**: **DC 3.18 denominator fix (SAS parity).** `SP_DC_3_18` was computing `PCT_BOTH` against the wrong denominator, producing inflated ratios that never fired the threshold flag. The prior proc used `COUNT(DISTINCT PATID) FROM cohort` (the F2F ∩ LDS-valid intersection) as `PATIENTS_N`, which was also then the denominator. SAS Table IB row 3.18 (edc_report.sas 878-908) uses `denom_fipszip` = `ENC_L3_DASH2 period='5 yrs' distinct_patid_n` = the F2F 5yr cohort size **before** joining LDS_ADDRESS_HISTORY. Fixed: `PATIENTS_N` now returns the F2F 5yr cohort size (matching SAS denom); a new metric `INTERSECT_LDS_VALID_N` reports the F2F ∩ LDS-valid intersection (= the old buggy `PATIENTS_N`, kept for traceability with SAS `XTBL_L3_CFIPSZIP_5Y PATIENTS distinct_patid_n`); `PCT_BOTH = BOTH_N / PATIENTS_N * 100` (now correct). Verified on `CHAR_UFH.PUBLIC`: PATIENTS_N=1,521,233 / BOTH_N=302,041 / PCT_BOTH=19.86% / FLAG=1 — matches user's SAS reference numbers exactly. Cleaned up 28 stale pre-fix DC_3_18 rows from DCQ_RESULTS across 4 RUN_IDs in CHAR_UFH + CHAR_UAB. Commit `66f241d`.
- **2026-07-16 (afternoon)**: **DC 3.03 IVC/IVD + DC 3.18 + POTENTIAL_CODE_ERRORS + registry deploy.** Ran `deploy_selective.py` against Snowflake: DROP legacy `SP_DC_3_03(VARCHAR x8)`, CREATE OR REPLACE `SP_DC_3_18` / `SP_DC_3_03_IVC` / `SP_DC_3_03_IVD` / `POTENTIAL_CODE_ERRORS`, TRUNCATE + COPY INTO `DCQ_CHECK_REGISTRY` (48 -> 49 rows). Additionally dropped stray legacy `SP_DC_3_03(VARCHAR x4)`. Smoke-tested all 4 procs against `CHAR_UAB.OVID_20260612` — all pass. Directly-verified DC 3.03 IVC flags `DISCHARGE_DISPOSITION` at 10.02% missing (the target bug fix). Also generated new RSA keypair `~/.snowflake/rsa_key_opencode.p8` for `allison_burns_opencode` Snowflake user and registered public key; extended `deploy_selective.py` to support optional passphrase-file env vars.
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
