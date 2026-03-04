CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_03"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT='DC 1.03 (Table IID): required columns exist in each CDM table. Expected columns are sourced from CHARACTERIZATION.EDC_REF.REQUIRED_STRUCTURE_RAW (MEMNAME/NAME) and compared to <DB_PARAM>.INFORMATION_SCHEMA.COLUMNS for <SCHEMA_NAME>. Run: CALL CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_03(DB_PARAM, SCHEMA_NAME, RUN_ID, TARGET_TABLE|''ALL''); or driver: CALL CHARACTERIZATION.DCQ.SP_RUN_DCQ(DB_PARAM, SCHEMA_NAME, ''ROW_NUM'', ''1.03'', ''part1'', TARGET_TABLE|''ALL''). Output: <DB_PARAM>.CHARACTERIZATION_DCQ.DCQ_RESULTS where ROW_NUM=1.03. Interpret: per SOURCE_TABLE, METRIC=''MISSING_REQUIRED_COLUMNS_FLAG'' with VALUE_NUM=1 / EXCEPTION_FLAG=TRUE indicates at least one required column is missing; DETAILS.missing_column_list shows examples.'
EXECUTE AS CALLER
AS '
function q(sqlText, binds) { return snowflake.execute({ sqlText, binds: binds || [] }); }
function isSafeIdentPart(s) { return /^[A-Za-z0-9_$]+$/.test((s || '''').toString()); }
function scalar(sqlText, binds) { const rs = q(sqlText, binds || []); rs.next(); return rs.getColumnValue(1); }
function insertMetric(resultsTbl, baseBinds, metric, valueNum, valueStr, exceptionFlag, detailsObj) {
  const flagInt = exceptionFlag ? 1 : 0;
  const detailsJson = JSON.stringify(detailsObj || {});
  q(
    `INSERT INTO ${resultsTbl} (
      RUN_ID, CHECK_ID, CHECK_NAME, ROW_NUM, EDC_TABLE,
      SOURCE_TABLE, CODE_TYPE, METRIC, VALUE_NUM, VALUE_STR,
      THRESHOLD_NUM, EXCEPTION_FLAG, DETAILS
    )
    SELECT
      ?, ?, ?, ?, ?,
      ?, NULL, ?, ?, ?,
      0,
      IFF(?=1, TRUE, FALSE),
      PARSE_JSON(?)
    `,
    baseBinds.concat([metric, valueNum, valueStr, flagInt, detailsJson])
  );
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 1.03;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
q(`CREATE SCHEMA IF NOT EXISTS ${outSchema}`);
q(`CREATE TABLE IF NOT EXISTS ${resultsTbl} (
  RUN_ID STRING, CHECK_ID STRING, CHECK_NAME STRING, ROW_NUM NUMBER(10,2), EDC_TABLE STRING,
  SOURCE_TABLE STRING, CODE_TYPE STRING, METRIC STRING, VALUE_NUM NUMBER(38,10), VALUE_STR STRING,
  THRESHOLD_NUM NUMBER(38,10), EXCEPTION_FLAG BOOLEAN, DETAILS VARIANT,
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)`);
// registry constants
const regRs = q(
  `SELECT CHECK_ID, CHECK_NAME, EDC_TABLE
   FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY
   WHERE ROW_NUM = ?
   QUALIFY ROW_NUMBER() OVER (ORDER BY CHECK_ID) = 1`,
  [rowNum]
);
if (!regRs.next()) throw new Error(`No registry row found for ROW_NUM=${rowNum}`);
const checkId = regRs.getColumnValue(1);
const checkName = regRs.getColumnValue(2);
const edcTable = regRs.getColumnValue(3);
// delete prior rows for this run/check (scoped if TARGET_TABLE provided)
if (only === "ALL") {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
} else {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ? AND UPPER(SOURCE_TABLE)=?`, [RUN_ID, rowNum, only]);
}
// Determine which tables to check from the reference list
let tblRs;
if (only === "ALL") {
  tblRs = q(
    `SELECT DISTINCT UPPER(MEMNAME) AS TBL
     FROM CHARACTERIZATION.EDC_REF.REQUIRED_STRUCTURE_RAW
     WHERE MEMNAME IS NOT NULL AND TRIM(MEMNAME) <> ''''`
  );
} else {
  tblRs = q(
    `SELECT DISTINCT UPPER(MEMNAME) AS TBL
     FROM CHARACTERIZATION.EDC_REF.REQUIRED_STRUCTURE_RAW
     WHERE UPPER(MEMNAME) = ?`,
    [only]
  );
}
const tables = [];
while (tblRs.next()) tables.push(tblRs.getColumnValue(1));
if (tables.length === 0) throw new Error(`No tables found in REQUIRED_STRUCTURE_RAW for TARGET_TABLE=''${only}''`);
for (const t of tables) {
  // expected columns from reference
  const expRs = q(
    `SELECT DISTINCT UPPER(NAME) AS COL
     FROM CHARACTERIZATION.EDC_REF.REQUIRED_STRUCTURE_RAW
     WHERE UPPER(MEMNAME) = ?
       AND NAME IS NOT NULL AND TRIM(NAME) <> ''''`,
    [t]
  );
  const expected = [];
  while (expRs.next()) expected.push(expRs.getColumnValue(1));
  // actual columns in target schema (if table doesn''t exist, count all as missing)
  const exists = scalar(
    `SELECT COUNT(*)
     FROM ${DB_PARAM}.INFORMATION_SCHEMA.TABLES
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
    [SCHEMA_NAME.toUpperCase(), t]
  ) > 0;
  let missing = expected.slice();
  if (exists) {
    const actRs = q(
      `SELECT UPPER(COLUMN_NAME) AS COL
       FROM ${DB_PARAM}.INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
      [SCHEMA_NAME.toUpperCase(), t]
    );
    const actualSet = new Set();
    while (actRs.next()) actualSet.add(actRs.getColumnValue(1));
    missing = expected.filter(c => !actualSet.has(c));
  }
  const missingCount = missing.length;
  const details = {
    table_exists: !!exists,
    expected_required_columns: expected.length,
    missing_required_columns: missingCount,
    missing_column_list: missingCount > 0 ? missing.slice(0, 200) : []  // cap to keep DETAILS small
  };
  const base = [RUN_ID, checkId, checkName, rowNum, edcTable, t];
  insertMetric(resultsTbl, base, "EXPECTED_REQUIRED_COLUMNS", expected.length, String(expected.length), false, details);
  insertMetric(resultsTbl, base, "MISSING_REQUIRED_COLUMNS", missingCount, String(missingCount), false, details);
  insertMetric(resultsTbl, base, "MISSING_REQUIRED_COLUMNS_FLAG", missingCount > 0 ? 1 : 0, missingCount > 0 ? "1" : "0", (missingCount > 0), details);
}
return `DC 1.03 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';