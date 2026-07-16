# DC 3.03 Full Port to SAS v16 — Session Notes

**Session date:** 2026-07-16
**Branch:** `add-dc-3-18-and-dc-1-13-snomed`
**Status:** Repo edits complete. Nothing deployed to Snowflake yet.

---

## What we set out to do

Fix DC 3.03 in the Streamlit app so it flags the same data quality issues that SAS flags. Specifically: SAS was flagging DISCHARGE_DISPOSITION as a failure in the user's data, but the Streamlit app was not.

## Root cause identified

The old `SP_DC_3_03.sql` procedure had structural differences from SAS v16:

1. **Only counted NULL / empty string as missing.** SAS also counts `NI`, `UN`, `OT` as missing for category-kind fields. DISCHARGE_DISPOSITION was 30%+ `NI` in the user's data — SAS flagged it, our app read it as 0% missing.
2. **No IP/EI denominator restriction.** SAS restricts denominators for 6 ENCOUNTER fields (DISCHARGE_DATE, DISCHARGE_DISPOSITION, DISCHARGE_STATUS, DRG, ADMITTING_SOURCE, PDX) to `ENC_TYPE IN ('IP','EI')`. Our old proc counted against all encounters.
3. **Wrong threshold.** SAS flags when `perc >= 10%`. Our old proc flagged anything `> 0%`.
4. **Race condition in the registry.** The registry had two `DC_3_03` rows (both with same CHECK_ID, one for `Table IVC` and one for `Table IVD`) — both invoked `SP_DC_3_03` in parallel with the same RUN_ID, causing DELETE + INSERT races on `DCQ_RESULTS`.

## What we built

**Two new procedures** replacing the old `SP_DC_3_03`:

- `SP_DC_3_03_IVC` — SAS Table IVC scope: DEMOGRAPHIC, ENROLLMENT, ENCOUNTER, DIAGNOSIS, PROCEDURES (52 fields, 10 flag fields)
- `SP_DC_3_03_IVD` — SAS Table IVD scope: 17 optional tables (VITAL, DEATH, LAB_RESULT_CM, PRESCRIBING, DISPENSING, CONDITION, PROVIDER, MED_ADMIN, OBS_CLIN, OBS_GEN, LDS_ADDRESS_HISTORY, IMMUNIZATION, LAB_HISTORY, EXTERNAL_MEDS, PAT_RELATIONSHIP, PRO_CM, DEATH_CAUSE). 161 fields, 13 flag fields.

**IVC flag fields** (perc >= 10% triggers exception):
BIRTH_DATE, SEX, DX_ORIGIN, DX_SOURCE, DIA_ENCOUNTERID (ENCOUNTERID in DIAGNOSIS), DISCHARGE_DATE, DISCHARGE_DISPOSITION, PRO_ENCOUNTERID (ENCOUNTERID in PROCEDURES), PX_DATE, PX_SOURCE

**IVD flag fields** (perc >= 10% triggers exception):
DEATH_SOURCE, ENCOUNTERID, LAB_RESULT_SOURCE, MEDADMIN_ENCOUNTERID, MEDADMIN_CODE, MEDADMIN_SOURCE, RX_ENCOUNTERID, RX_ORDER_DATE, RX_SOURCE, **PRO_CODE**, **PRO_SOURCE**, VITAL_ENCOUNTERID, VITAL_SOURCE

**Missing-value definition per field kind** (six categories, derived from which SAS numerator clause matches):
- `category` — NULL, empty, OR value in ('NI','UN','OT'). Applied to distribution-style fields like SEX, ENC_TYPE, DX_SOURCE.
- `null_n` — NULL or empty only. Applied to ID/FK columns (ENCOUNTERID, PROVIDERID, FACILITYID).
- `nmiss` — NULL only. Applied to date/time/numeric columns (BIRTH_DATE, PX_DATE, MEASURE_TIME).
- `null_or_missing`, `cross_null_or_missing`, `stat_null_or_missing` — variants of NULL-or-empty for specific SAS clauses.

**Denominators:**
- `none` — COUNT(*) of the parent table.
- `ip_ei` — COUNT(rows with UPPER(TRIM(ENC_TYPE)) IN ('IP','EI')) on the parent table. Both numerator and denominator are restricted.

## Files changed (in the repo, NOT in Snowflake yet)

**New files:**
- `sql/dcq_checks/procedures/SP_DC_3_03_IVC.sql` — 327 lines
- `sql/dcq_checks/procedures/SP_DC_3_03_IVD.sql` — 436 lines
- `sas_code/parse_dc_3_03_catalog.py` — SAS-to-JSON extractor (parses `edc_report.sas` newest, produces the catalog the two procs are generated from)
- `sas_code/dc_3_03_catalog.json` — extracted catalog (audit trail; regenerate with `python3 sas_code/parse_dc_3_03_catalog.py`)

**Modified files:**
- `snowflake/DCQ_CHECK_REGISTRY_data_full.json` — replaced the two duplicate `DC_3_03` rows (same CHECK_ID) with distinct `DC_3_03_IVC` (CHECK_ID `4dcc7f7f1d8761f8bc8cb95418e8fa40`) + `DC_3_03_IVD` (CHECK_ID `ad244ef6de9f969425ea1f9e788fc494`). Registry stays at 49 rows.
- `deploy_selective.py` — updated `PROC_FILES` to include the two new procs; added `LEGACY_DROPS` step (`DROP PROCEDURE IF EXISTS SP_DC_3_03(VARCHAR x8)`) that runs before proc deploy; updated header docstring.
- `.opencode/codebase-summary.md` — added Change History entry dated 2026-07-16 documenting the port.
- `.gitignore` — added `sas_code/extracted*/` (the extracted SAS source directories don't need to be tracked).

**Moved files:**
- `sql/dcq_checks/procedures/SP_DC_3_03.sql` → `sql/dcq_checks/procedures/_archive/SP_DC_3_03.sql` (legacy proc, kept for reference)
- `sas_code/dc_3_03_ivc_catalog.md` → `sas_code/_archive/dc_3_03_ivc_catalog.md` (mid-session review artifact, superseded by the JSON)

**Ignored (kept locally, not committed):**
- `sas_code/infolder.zip`, `sas_code/infolder_old.zip`, `sas_code/infolder_newest.zip` — SAS source zips
- `sas_code/extracted/`, `sas_code/extracted_old/`, `sas_code/extracted_newest/` — extraction dirs (regenerate with `unzip sas_code/infolder_newest.zip -d sas_code/extracted_newest/`)

## Verification done in-session

- Both new procs pass brace/paren/bracket balance checks (no syntax issues in JS bodies).
- Registry has no duplicate CHECK_NAMEs after edit (49 distinct rows).
- Spot-checked catalog for correctness:
  - `DISCHARGE_DISPOSITION`: table=ENCOUNTER, kind=category, denom=ip_ei, flag=True ✓
  - `DISCHARGE_DATE`: table=ENCOUNTER, kind=category, denom=ip_ei, flag=True ✓
  - `BIRTH_DATE`: table=DEMOGRAPHIC, kind=nmiss, denom=none, flag=True ✓
  - `PRO_CODE`: table=PRO_CM, kind=null_or_missing, flag=True ✓
  - `PRO_SOURCE`: table=PRO_CM, kind=category, flag=True ✓
  - `MEDADMIN_ENCOUNTERID`: table=MED_ADMIN, kind=null_n, flag=True ✓
- `python3 deploy_selective.py --dry-run` succeeds and shows correct 6-step deploy plan (see below).

## Deploy plan (NOT YET RUN)

`python3 deploy_selective.py` will execute in order:

1. `DROP PROCEDURE IF EXISTS CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_03(VARCHAR x8)`
2. `CREATE OR REPLACE PROCEDURE SP_DC_3_18` (unchanged — was already staged from prior session)
3. `CREATE OR REPLACE PROCEDURE SP_DC_3_03_IVC` (new)
4. `CREATE OR REPLACE PROCEDURE SP_DC_3_03_IVD` (new)
5. `CREATE OR REPLACE PROCEDURE POTENTIAL_CODE_ERRORS` (unchanged — was already staged from prior session)
6. `TRUNCATE + COPY INTO CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY` from the JSON (49 rows)

**Backups** (already captured from the previous session, per resume state):
- Registry clone: `CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY_BACKUP_20260715` (48 rows — from before the DC 3.03 edits).
- User was to save `GET_DDL` output for `SP_DC_3_03` and `POTENTIAL_CODE_ERRORS` to `rollback/captured_*.sql` — unconfirmed whether those files were actually saved.

## Next steps (resume here)

1. **Optional:** confirm the two GET_DDL captures from the previous session are saved to `rollback/`. If not, no big deal — worst case the legacy `SP_DC_3_03` can be recreated from `sql/dcq_checks/procedures/_archive/SP_DC_3_03.sql`.
2. **Dry-run:**
   ```
   python3 deploy_selective.py --dry-run
   ```
   Already passing as of this writing.
3. **Real deploy:**
   ```
   python3 deploy_selective.py
   ```
4. **Post-deploy smoke SQL in Snowsight:**
   ```sql
   -- Registry state
   SELECT COUNT(*) FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY;                   -- expect 49
   SELECT CHECK_NAME, PROC_NAME, EDC_TABLE FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY
     WHERE CHECK_NAME IN ('DC_3_03_IVC','DC_3_03_IVD','DC_3_18','DC_1_13');

   -- Procedures exist
   SHOW PROCEDURES LIKE 'SP_DC_3_03%' IN SCHEMA CHARACTERIZATION.DCQ_CHECKS;
   -- Expect two rows: SP_DC_3_03_IVC and SP_DC_3_03_IVD. NOT SP_DC_3_03 (dropped).
   ```
5. **Direct-call test the two new procs against a real target DB** (user picks which — likely a `CHAR_*` or `STAGE_*` database and PUBLIC schema):
   ```sql
   CALL CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_03_IVC('<DB>','<SCHEMA>','TEST_IVC','ALL',NULL,NULL,NULL,NULL);
   CALL CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_03_IVD('<DB>','<SCHEMA>','TEST_IVD','ALL',NULL,NULL,NULL,NULL);
   ```
6. **Verify DISCHARGE_DISPOSITION now flags** (the whole point of the port):
   ```sql
   SELECT SOURCE_TABLE, CODE_TYPE, METRIC, VALUE_STR, EXCEPTION_FLAG
   FROM <DB>.CHARACTERIZATION_DCQ.DCQ_RESULTS
   WHERE RUN_ID='TEST_IVC' AND CODE_TYPE='DISCHARGE_DISPOSITION'
   ORDER BY METRIC;
   -- Expect: DENOM_N (IP/EI encounters), MISSING_N (NULL/NI/UN/OT count),
   --         MISSING_PCT, MISSING_FLAG=1 with EXCEPTION_FLAG=TRUE if >= 10%.
   ```
7. **Streamlit end-to-end test:** Snowsight → Streamlit → `characterization_runner`. Tab 1 (SP_RUN_DCQ), Mode=`CHECK_NAME`, run `DC_3_03_IVC` and `DC_3_03_IVD` individually. Confirm the progress bar completes and results tab shows rows.
8. **Optional full run** with Mode=`ALL`, Max parallel=10, to confirm no regressions on the other 46 checks. Expect 49 SUCCEEDED (or 48 + 1 known-broken SP_DC_2_07 failure — see Known Issues in codebase-summary.md).
9. **Merge + housekeeping:**
   - `git checkout main && git merge --no-ff add-dc-3-18-and-dc-1-13-snomed && git push origin main`
   - Delete the branch on GitHub.
   - Once satisfied: `DROP TABLE CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY_BACKUP_20260715;`

## Rollback recipes (if deploy breaks something)

- **A new proc has a compile error:** Snowflake's CREATE OR REPLACE is atomic; if the SQL is malformed, the old version stays. But the old `SP_DC_3_03` was already DROP'd by step 1 of the deploy, so if IVC/IVD fail to create you'd be missing DC 3.03 entirely. To restore: paste `sql/dcq_checks/procedures/_archive/SP_DC_3_03.sql` (with header quotes stripped) into Snowsight and run it. This recreates the legacy proc.
- **A new proc compiles but has a logic bug at run time:** you can DROP the new proc and recreate the legacy one from `_archive/`.
- **Registry corrupted/empty after TRUNCATE + COPY failure:** run `rollback/02_restore_if_broken.sql` (swaps `DCQ_CHECK_REGISTRY` back from the `_BACKUP_20260715` clone). NOTE: the clone has the OLD registry with duplicate DC_3_03 rows and no IVC/IVD split, so after restore you'd also need to drop the (now orphan) IVC/IVD procs to keep the state consistent.
- **Full undo:**
  ```sql
  DROP PROCEDURE IF EXISTS CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_03_IVC(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR);
  DROP PROCEDURE IF EXISTS CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_03_IVD(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR);
  -- Recreate legacy SP_DC_3_03 from sql/dcq_checks/procedures/_archive/SP_DC_3_03.sql
  -- Then run rollback/02_restore_if_broken.sql to restore the pre-deploy registry.
  ```

## How to regenerate the catalog if SAS updates again

1. Drop the new SAS zip into `sas_code/` (e.g., `infolder_newer.zip`).
2. `unzip sas_code/infolder_newer.zip -d sas_code/extracted_newest/` (overwrite existing).
3. Edit `sas_code/parse_dc_3_03_catalog.py` — update the hard-coded lists (`CATEGORY_DCNAMES`, `NULL_N_DCNAMES`, `IVC_FLAG_VARS`, `IVD_FLAG_VARS`, etc.) to reflect any SAS changes. Verify the flag lists against the SAS source lines noted in the comments.
4. `python3 sas_code/parse_dc_3_03_catalog.py` — regenerates `sas_code/dc_3_03_catalog.json`.
5. Re-run the code-generation snippet (or write a small helper in the parser) to regenerate `SP_DC_3_03_IVC.sql` and `SP_DC_3_03_IVD.sql` from the JSON.
6. `git diff` — spot-check the changes.
7. Deploy.

Currently there's no single-command "regenerate the procs from the JSON" — the generation was done inline in the session. If we do this again it's worth adding a `sas_code/generate_procs.py` script that reads `dc_3_03_catalog.json` and writes the two `.sql` files. Not urgent.

## Known issues / caveats

- **Field ambiguity for LAB_LOINC in IVD:** the catalog has TWO entries for `LAB_RESULT_CM.LAB_LOINC` — one as `null_or_missing` (LAB_L3_LOINC dc_name, checks whether the LOINC code is present) and one as `RESULT_NUM` mapped column (LAB_L3_LOINC_RESULT_NUM dc_name, checks NMISS of RESULT_NUM). The parser aliases the second one to CDM column `RESULT_NUM` per the SAS rename at line 8519. This is intentional but confusing — worth double-checking after deploy that both metrics appear in DCQ_RESULTS with distinct row identities.
- **Pre-existing DC 3.03 registry duplicate:** the two `DC_3_03` rows had the same CHECK_ID hash (`7e707d95d8cf4f69e03b149c01d16a34`). This caused a race condition where SP_DC_3_03 ran twice in parallel with the same RUN_ID. The port eliminates this by giving IVC and IVD distinct CHECK_IDs.
- **AGENTS.md doc drift:** references a Bitbucket remote that doesn't exist. Only remote is GitHub `origin`. Fix later, not urgent.
