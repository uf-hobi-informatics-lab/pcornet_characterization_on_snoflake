-- =============================================================================
-- ROLLBACK RESTORE - Only run this IF the deploy broke something
-- =============================================================================
--
-- PURPOSE
--   Restores the two procedures and the registry table to their pre-deploy
--   state. Use this ONLY if the deploy went badly and you need to get back
--   to where you were.
--
-- WHAT TO DO
--   1. Open Snowsight, new Worksheet.
--   2. Set role and warehouse:
--          USE ROLE "1FL-AZURE-DB-USERS";
--          USE WAREHOUSE CHARACTERIZATION_XS;
--   3. In Section A: paste the output you captured earlier from
--      rollback/captured_20260715_dc_3_03.sql.
--      (This is the "CREATE OR REPLACE PROCEDURE ..." block for DC 3.03.)
--   4. In Section B: paste the output from captured_20260715_pce.sql.
--   5. Uncomment Section C only if the registry table needs restoring.
--   6. Also drop SP_DC_3_18 if it was created (Section D).
--   7. Click "Run All".
--
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Section A: Restore SP_DC_3_03
-- -----------------------------------------------------------------------------
-- Paste the "current_sp_dc_3_03_ddl" output from 01_capture_current_state.sql
-- between the BEGIN and END markers below.
--
-- The pasted DDL will look like:
--   CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_03(...)
--   RETURNS VARCHAR
--   LANGUAGE JAVASCRIPT
--   ...
--   AS '
--   ...body...
--   ';
--

-- --- BEGIN paste for Section A ---

-- <paste captured_20260715_dc_3_03.sql content here>

-- --- END paste for Section A ---


-- -----------------------------------------------------------------------------
-- Section B: Restore POTENTIAL_CODE_ERRORS
-- -----------------------------------------------------------------------------

-- --- BEGIN paste for Section B ---

-- <paste captured_20260715_pce.sql content here>

-- --- END paste for Section B ---


-- -----------------------------------------------------------------------------
-- Section C: Restore DCQ_CHECK_REGISTRY from the clone
-- -----------------------------------------------------------------------------
-- Uncomment the CREATE line below ONLY if the deploy corrupted the registry
-- (e.g., wrong row count, missing check, bad JSON parse). The clone from
-- 01_capture_current_state.sql has your pre-deploy registry.
--
-- WARNING: This wipes out DC_3_18 if it was successfully added. Only use if
-- rolling back everything.

-- CREATE OR REPLACE TABLE CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY
--     CLONE CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY_BACKUP_20260715;

-- Verify:
-- SELECT COUNT(*) AS restored_row_count FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY;
-- Expected: 48 rows (pre-deploy count).


-- -----------------------------------------------------------------------------
-- Section D: Drop SP_DC_3_18 (new proc that didn't exist before deploy)
-- -----------------------------------------------------------------------------
-- If the deploy successfully created SP_DC_3_18 but you're rolling back
-- everything, drop it so the environment matches pre-deploy state.

-- DROP PROCEDURE IF EXISTS CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_18(
--     VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR
-- );


-- -----------------------------------------------------------------------------
-- Verification queries - run after restore to confirm rollback succeeded
-- -----------------------------------------------------------------------------

-- Confirm the two procedures exist and are the old versions:
SHOW PROCEDURES LIKE 'SP_DC_3_03' IN SCHEMA CHARACTERIZATION.DCQ_CHECKS;
SHOW PROCEDURES LIKE 'POTENTIAL_CODE_ERRORS' IN SCHEMA CHARACTERIZATION.DCQ_CHECKS;

-- Confirm the registry row count matches pre-deploy:
SELECT COUNT(*) AS current_row_count FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY;

-- Confirm SP_DC_3_18 is gone (if you dropped it):
-- SHOW PROCEDURES LIKE 'SP_DC_3_18' IN SCHEMA CHARACTERIZATION.DCQ_CHECKS;
-- Expected: 0 rows.
