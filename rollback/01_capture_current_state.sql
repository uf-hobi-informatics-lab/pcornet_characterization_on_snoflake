-- =============================================================================
-- ROLLBACK CAPTURE - Run this BEFORE deploying any changes
-- =============================================================================
--
-- PURPOSE
--   Saves the current deployed versions of the two procedures we are about to
--   change, plus the current DCQ_CHECK_REGISTRY contents. If the deploy breaks
--   something, you paste this output into 02_restore_if_broken.sql and run it
--   to get back to the pre-deploy state.
--
-- WHAT TO DO
--   1. Open Snowsight (Snowflake web UI).
--   2. Open a new Worksheet.
--   3. Set your role and warehouse at the top (e.g.):
--          USE ROLE "1FL-AZURE-DB-USERS";
--          USE WAREHOUSE CHARACTERIZATION_XS;
--   4. Paste EVERYTHING below this comment block into the Worksheet.
--   5. Click "Run All".
--   6. For each of the 3 sections, click the result cell, select all, copy.
--   7. Paste each result into a file on your machine. Suggested names:
--        rollback/captured_20260715_dc_3_03.sql
--        rollback/captured_20260715_pce.sql
--        rollback/captured_20260715_registry_backup_name.txt
--   8. Do NOT deploy until all three captures are saved locally.
--
-- WHY THREE SEPARATE FILES
--   Each procedure's DDL is a big multi-line string. Keeping them in separate
--   files makes them easier to paste back one at a time if only one needs to
--   be rolled back.
--
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Section 1: Capture DC 3.03 procedure DDL
-- -----------------------------------------------------------------------------
-- The result of this query is a full CREATE OR REPLACE PROCEDURE statement
-- for the currently deployed SP_DC_3_03. Save this output to a file.
--
SELECT GET_DDL(
    'PROCEDURE',
    'CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_03(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR)'
) AS current_sp_dc_3_03_ddl;


-- -----------------------------------------------------------------------------
-- Section 2: Capture POTENTIAL_CODE_ERRORS procedure DDL
-- -----------------------------------------------------------------------------
SELECT GET_DDL(
    'PROCEDURE',
    'CHARACTERIZATION.DCQ_CHECKS.POTENTIAL_CODE_ERRORS(VARCHAR,VARCHAR,VARCHAR)'
) AS current_potential_code_errors_ddl;


-- -----------------------------------------------------------------------------
-- Section 3: Clone the registry table (belt-and-suspenders backup)
-- -----------------------------------------------------------------------------
-- Snowflake's zero-copy clone is instantaneous and does not duplicate storage.
-- If the deploy breaks the registry, we can restore it in one command.
--
-- Naming convention: DCQ_CHECK_REGISTRY_BACKUP_YYYYMMDD
--
CREATE OR REPLACE TABLE CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY_BACKUP_20260715
    CLONE CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY;

-- Confirm the backup exists and has rows.
SELECT COUNT(*) AS backup_row_count,
       CURRENT_TIMESTAMP() AS captured_at
FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY_BACKUP_20260715;

-- Expected: backup_row_count = 48 (the pre-deploy count).
