CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_13"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT='DC 1.13 (Table IIF): terminology heuristics / potential code errors. Wraps CHARACTERIZATION.DCQ_CHECKS.POTENTIAL_CODE_ERRORS(DB_PARAM, SCHEMA_NAME, TABLE_LIST) and ingests <DB_PARAM>.CHARACTERIZATION.CODE_SUMMARY into <DB_PARAM>.CHARACTERIZATION_DCQ.DCQ_RESULTS. TARGET_TABLE may be a single domain (e.g., DIAGNOSIS) or ALL. Run: CALL CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_13(DB_PARAM, SCHEMA_NAME, RUN_ID, TARGET_TABLE|''ALL''); or driver: CALL CHARACTERIZATION.DCQ.SP_RUN_DCQ(DB_PARAM, SCHEMA_NAME, ''ROW_NUM'', ''1.13'', ''part1'', TARGET_TABLE|''ALL''). Output: <DB_PARAM>.CHARACTERIZATION_DCQ.DCQ_RESULTS where ROW_NUM=1.13. Interpret: EXCEPTION_FLAG is TRUE when BAD_RECORD_PCT > 5 for a SOURCE_TABLE+CODE_TYPE; metrics include TOTAL_RECORDS/TOTAL_CODES/BAD_RECORDS/BAD_CODES/BAD_RECORD_PCT.'
EXECUTE AS CALLER
AS '
function q(sqlText, binds) {
  return snowflake.execute({ sqlText, binds: binds || [] });
}
function oneRow(sqlText, binds) {
  const rs = q(sqlText, binds);
  if (!rs.next()) return null;
  return {
    check_id: rs.getColumnValue(1),
    check_name: rs.getColumnValue(2),
    edc_table: rs.getColumnValue(3)
  };
}
function isSafeIdentPart(s) {
  return /^[A-Za-z0-9_$]+$/.test((s || '''').toString());
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const codeSummaryTbl = `${DB_PARAM}.CHARACTERIZATION.CODE_SUMMARY`;
const rowNum = 1.13;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
// Ensure outputs exist
q(`CREATE SCHEMA IF NOT EXISTS ${outSchema}`);
q(`CREATE TABLE IF NOT EXISTS ${resultsTbl} (
  RUN_ID STRING, CHECK_ID STRING, CHECK_NAME STRING, ROW_NUM NUMBER(10,2), EDC_TABLE STRING,
  SOURCE_TABLE STRING, CODE_TYPE STRING, METRIC STRING, VALUE_NUM NUMBER(38,10), VALUE_STR STRING,
  THRESHOLD_NUM NUMBER(38,10), EXCEPTION_FLAG BOOLEAN, DETAILS VARIANT,
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)`);
// Registry constants
const reg = oneRow(
  `SELECT CHECK_ID, CHECK_NAME, EDC_TABLE
   FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY
   WHERE ROW_NUM = ?
   QUALIFY ROW_NUMBER() OVER (ORDER BY CHECK_ID) = 1`,
  [rowNum]
);
if (!reg) throw new Error(`No registry row found for ROW_NUM=${rowNum}`);
// 1) Run PCE generator with table scoping
q(
  `CALL CHARACTERIZATION.DCQ_CHECKS.POTENTIAL_CODE_ERRORS(?, ?, ?)`,
  [DB_PARAM, SCHEMA_NAME, only]
);
// 2) Idempotent delete for this run/check (optionally table-scoped)
if (only === "ALL") {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
} else {
  q(
    `DELETE FROM ${resultsTbl}
     WHERE RUN_ID = ? AND ROW_NUM = ? AND UPPER(SOURCE_TABLE) = ?`,
    [RUN_ID, rowNum, only]
  );
}
// 3) Insert metrics (UNION ALL; no LATERAL)
const detailsObj = `
  OBJECT_CONSTRUCT(
    ''total_records'', TOTAL_RECORDS,
    ''total_codes'', TOTAL_CODES,
    ''bad_records'', BAD_RECORDS,
    ''bad_codes'', BAD_CODES,
    ''bad_record_pct'', BAD_RECORD_PCT,
    ''datamartid'', DATAMARTID,
    ''query_date'', QUERY_DATE
  )
`;
const whereFilter = (only === "ALL") ? "" : `WHERE UPPER(TABLE_NAME) = ''${only}''`;
q(
  `
  INSERT INTO ${resultsTbl} (
    RUN_ID, CHECK_ID, CHECK_NAME, ROW_NUM, EDC_TABLE,
    SOURCE_TABLE, CODE_TYPE, METRIC, VALUE_NUM, VALUE_STR,
    THRESHOLD_NUM, EXCEPTION_FLAG, DETAILS
  )
  SELECT ?, ?, ?, ?, ?,
         TABLE_NAME, CODE_TYPE,
         ''TOTAL_RECORDS'', TOTAL_RECORDS::NUMBER(38,10), TOTAL_RECORDS::STRING,
         5::NUMBER(38,10), IFF(BAD_RECORD_PCT > 5, TRUE, FALSE), ${detailsObj}
  FROM ${codeSummaryTbl}
  ${whereFilter}
  UNION ALL
  SELECT ?, ?, ?, ?, ?,
         TABLE_NAME, CODE_TYPE,
         ''TOTAL_CODES'', TOTAL_CODES::NUMBER(38,10), TOTAL_CODES::STRING,
         5::NUMBER(38,10), IFF(BAD_RECORD_PCT > 5, TRUE, FALSE), ${detailsObj}
  FROM ${codeSummaryTbl}
  ${whereFilter}
  UNION ALL
  SELECT ?, ?, ?, ?, ?,
         TABLE_NAME, CODE_TYPE,
         ''BAD_RECORDS'', BAD_RECORDS::NUMBER(38,10), BAD_RECORDS::STRING,
         5::NUMBER(38,10), IFF(BAD_RECORD_PCT > 5, TRUE, FALSE), ${detailsObj}
  FROM ${codeSummaryTbl}
  ${whereFilter}
  UNION ALL
  SELECT ?, ?, ?, ?, ?,
         TABLE_NAME, CODE_TYPE,
         ''BAD_CODES'', BAD_CODES::NUMBER(38,10), BAD_CODES::STRING,
         5::NUMBER(38,10), IFF(BAD_RECORD_PCT > 5, TRUE, FALSE), ${detailsObj}
  FROM ${codeSummaryTbl}
  ${whereFilter}
  UNION ALL
  SELECT ?, ?, ?, ?, ?,
         TABLE_NAME, CODE_TYPE,
         ''BAD_RECORD_PCT'', BAD_RECORD_PCT::NUMBER(38,10), BAD_RECORD_PCT::STRING,
         5::NUMBER(38,10), IFF(BAD_RECORD_PCT > 5, TRUE, FALSE), ${detailsObj}
  FROM ${codeSummaryTbl}
  ${whereFilter}
  `,
  [
    RUN_ID, reg.check_id, reg.check_name, rowNum, reg.edc_table,
    RUN_ID, reg.check_id, reg.check_name, rowNum, reg.edc_table,
    RUN_ID, reg.check_id, reg.check_name, rowNum, reg.edc_table,
    RUN_ID, reg.check_id, reg.check_name, rowNum, reg.edc_table,
    RUN_ID, reg.check_id, reg.check_name, rowNum, reg.edc_table
  ]
);
return `DC 1.13 finished for RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';