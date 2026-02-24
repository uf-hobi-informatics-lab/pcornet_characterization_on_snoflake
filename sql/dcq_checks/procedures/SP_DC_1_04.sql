CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_04"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT='DC 1.04 (Table IID): required columns conform to expected type/length (v1). Uses CHARACTERIZATION.EDC_REF.REQUIRED_STRUCTURE_RAW (R_TYPE/R_LENGTH) vs <DB_PARAM>.INFORMATION_SCHEMA.COLUMNS (DATA_TYPE/CHARACTER_MAXIMUM_LENGTH). Rules: R_TYPE=2 expects string types; enforces CHARACTER_MAXIMUM_LENGTH >= R_LENGTH when provided. R_TYPE=1 expects non-string types; exception: *_TIME columns may be TIME or short string (<=8). Run: CALL CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_04(DB_PARAM, SCHEMA_NAME, RUN_ID, TARGET_TABLE|''ALL''); or driver: CALL CHARACTERIZATION.DCQ.SP_RUN_DCQ(DB_PARAM, SCHEMA_NAME, ''ROW_NUM'', ''1.04'', ''part1'', TARGET_TABLE|''ALL''). Output: <DB_PARAM>.CHARACTERIZATION_DCQ.DCQ_RESULTS where ROW_NUM=1.04. Interpret: per SOURCE_TABLE, METRIC=''ANY_CONFORMANCE_ISSUE_FLAG'' with VALUE_NUM=1 / EXCEPTION_FLAG=TRUE indicates at least one type/length mismatch; DETAILS.examples contains sample mismatches.'
EXECUTE AS CALLER
AS '
function q(sqlText, binds) { return snowflake.execute({ sqlText, binds: binds || [] }); }
function isSafeIdentPart(s) { return /^[A-Za-z0-9_$]+$/.test((s || '''').toString()); }
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
function isStringType(dt) {
  if (dt === null) return false;
  const s = String(dt).toUpperCase();
  return (s.includes("CHAR") || s.includes("STRING") || s.includes("TEXT") || s.includes("VARCHAR"));
}
function isTimeType(dt) {
  if (dt === null) return false;
  return String(dt).toUpperCase().includes("TIME");
}
function isTimeLikeColumn(colName) {
  if (colName === null) return false;
  const c = String(colName).toUpperCase();
  return c.endsWith("_TIME");
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 1.04;
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
// Query expected vs actual
let whereTarget = "";
const binds = [SCHEMA_NAME.toUpperCase()];
if (only !== "ALL") {
  whereTarget = "AND UPPER(r.MEMNAME) = ?";
  binds.push(only);
}
const rs = q(
  `
  SELECT
    UPPER(r.MEMNAME) AS TABLE_NAME,
    UPPER(r.NAME) AS COLUMN_NAME,
    TRIM(r.R_TYPE) AS R_TYPE,
    TRY_TO_NUMBER(r.R_LENGTH) AS R_LENGTH,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH
  FROM CHARACTERIZATION.EDC_REF.REQUIRED_STRUCTURE_RAW r
  LEFT JOIN ${DB_PARAM}.INFORMATION_SCHEMA.COLUMNS c
    ON c.TABLE_SCHEMA = ?
   AND UPPER(c.TABLE_NAME) = UPPER(r.MEMNAME)
   AND UPPER(c.COLUMN_NAME) = UPPER(r.NAME)
  WHERE r.MEMNAME IS NOT NULL AND TRIM(r.MEMNAME) <> ''''
    AND r.NAME IS NOT NULL AND TRIM(r.NAME) <> ''''
    ${whereTarget}
  ORDER BY TABLE_NAME, COLUMN_NAME
  `,
  binds
);
const perTable = new Map();
while (rs.next()) {
  const tableName = rs.getColumnValue(1);
  const colName = rs.getColumnValue(2);
  const rType = rs.getColumnValue(3);        // ''1'' or ''2''
  const rLen = rs.getColumnValue(4);         // number or null
  const dataType = rs.getColumnValue(5);     // actual
  const charMax = rs.getColumnValue(6);      // actual max len
  if (!perTable.has(tableName)) {
    perTable.set(tableName, {
      total_specified: 0,
      checked: 0,
      type_mismatches: 0,
      length_mismatches: 0,
      mismatch_examples: []
    });
  }
  const agg = perTable.get(tableName);
  agg.total_specified += 1;
  // If column missing, skip evaluation (1.03 handles it)
  if (dataType === null) continue;
  agg.checked += 1;
  const expectChar = (String(rType) === "2");
  const actualIsChar = isStringType(dataType);
  let typeMismatch = false;
  let lengthMismatch = false;
  // Allow TIME-like columns to be stored as TIME or short TEXT/VARCHAR even if expectChar=false (R_TYPE=1)
  const timeLike = (!expectChar) && isTimeLikeColumn(colName);
  const allowTimeAsChar =
    timeLike &&
    (isTimeType(dataType) || (actualIsChar && charMax !== null && Number(charMax) <= 8));
  if (expectChar && !actualIsChar) typeMismatch = true;
  if (!expectChar && actualIsChar && !allowTimeAsChar) typeMismatch = true;
  if (expectChar && rLen !== null) {
    if (charMax === null) lengthMismatch = true;
    else if (Number(charMax) < Number(rLen)) lengthMismatch = true;
  }
  if (typeMismatch) agg.type_mismatches += 1;
  if (lengthMismatch) agg.length_mismatches += 1;
  if ((typeMismatch || lengthMismatch) && agg.mismatch_examples.length < 50) {
    agg.mismatch_examples.push({
      column: colName,
      expected_r_type: rType,
      expected_r_length: rLen,
      actual_data_type: dataType,
      actual_char_max_len: charMax,
      type_mismatch: typeMismatch,
      length_mismatch: lengthMismatch,
      time_like_exception_applied: allowTimeAsChar
    });
  }
}
// Emit per-table summary rows
for (const [tableName, agg] of perTable.entries()) {
  const anyMismatch = (agg.type_mismatches + agg.length_mismatches) > 0;
  const details = {
    total_specified: agg.total_specified,
    checked: agg.checked,
    type_mismatches: agg.type_mismatches,
    length_mismatches: agg.length_mismatches,
    examples: agg.mismatch_examples
  };
  const base = [RUN_ID, checkId, checkName, rowNum, edcTable, tableName];
  insertMetric(resultsTbl, base, "FIELDS_SPECIFIED_IN_REF", agg.total_specified, String(agg.total_specified), false, details);
  insertMetric(resultsTbl, base, "FIELDS_CHECKED_PRESENT", agg.checked, String(agg.checked), false, details);
  insertMetric(resultsTbl, base, "TYPE_MISMATCH_COUNT", agg.type_mismatches, String(agg.type_mismatches), false, details);
  insertMetric(resultsTbl, base, "LENGTH_MISMATCH_COUNT", agg.length_mismatches, String(agg.length_mismatches), false, details);
  insertMetric(resultsTbl, base, "ANY_CONFORMANCE_ISSUE_FLAG", anyMismatch ? 1 : 0, anyMismatch ? "1" : "0", anyMismatch, details);
}
return `DC 1.04 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';