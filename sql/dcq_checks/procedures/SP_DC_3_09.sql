CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_09"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
function q(sqlText, binds) { return snowflake.execute({ sqlText, binds: binds || [] }); }
function isSafeIdentPart(s) { return /^[A-Za-z0-9_$]+$/.test((s || '''').toString()); }
function tableExists(db, schema, table) {
  const rs = q(
    `SELECT COUNT(*)
     FROM ${db}.INFORMATION_SCHEMA.TABLES
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
    [schema.toUpperCase(), table.toUpperCase()]
  );
  rs.next();
  return rs.getColumnValue(1) > 0;
}
function insertMetric(resultsTbl, baseBinds, codeType, metric, valueNum, valueStr, thresholdNum, exceptionFlag, detailsObj) {
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
      ''LAB_RESULT_CM'', ?, ?, ?, ?,
      ?, IFF(?=1, TRUE, FALSE), PARSE_JSON(?)
    `,
    baseBinds.concat([codeType, metric, valueNum, valueStr, thresholdNum, flagInt, detailsJson])
  );
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 3.09;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
const thresholdPct = 80;
if (!(only === "ALL" || only === "LAB_RESULT_CM")) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or LAB_RESULT_CM.`);
}
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
const base = [RUN_ID, checkId, checkName, rowNum, edcTable];
q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
if (!tableExists(DB_PARAM, SCHEMA_NAME, "LAB_RESULT_CM")) {
  insertMetric(resultsTbl, base, "LAB_RESULT_CM", "STATUS", null, "SKIPPED", thresholdPct, false, {
    message: "LAB_RESULT_CM table does not exist"
  });
  return `DC 3.09 SKIPPED: LAB_RESULT_CM missing`;
}
// Pull reference rows (labels/criteria text)
const refRs = q(
  `SELECT SOURCE_TABLE_ROW, DESCRIPTION, CODE_CRITERIA, RESULT_NUM_CRITERIA, RESULT_MODIFIER_CRITERIA, OTHER_CRITERIA
   FROM CHARACTERIZATION.EDC_REF.TBL_IVI_REF_RAW
   WHERE SOURCE_TABLE_ROW IN (
     ''KNOWN_TEST'',
     ''KNOWN_TEST_RESULT'',
     ''KNOWN_TEST_RESULT_NUM'',
     ''KNOWN_TEST_RESULT_NUM_UNIT'',
     ''KNOWN_TEST_NUM_RESULT_RANGE''
   )
   ORDER BY SOURCE_TABLE_ROW`
);
const ref = {};
while (refRs.next()) {
  const key = (refRs.getColumnValue(1) || "").toString();
  ref[key] = {
    description: refRs.getColumnValue(2),
    code_criteria: refRs.getColumnValue(3),
    result_num_criteria: refRs.getColumnValue(4),
    result_modifier_criteria: refRs.getColumnValue(5),
    other_criteria: refRs.getColumnValue(6)
  };
}
const lab = `${DB_PARAM}.${SCHEMA_NAME}.LAB_RESULT_CM`;
// One-pass counts
const rs = q(
  `
  WITH x AS (
    SELECT
      1 AS one,
      IFF(LAB_LOINC IS NOT NULL
          AND TRIM(LAB_LOINC) <> ''''
          AND UPPER(TRIM(LAB_LOINC)) NOT IN (''NI'',''UN'',''OT''),
          1, 0) AS mapped,
      IFF(RESULT_NUM IS NOT NULL
          AND RESULT_MODIFIER IS NOT NULL
          AND TRIM(RESULT_MODIFIER) <> ''''
          AND UPPER(TRIM(RESULT_MODIFIER)) NOT IN (''NI'',''UN'',''OT''),
          1, 0) AS has_quant,
      IFF(RESULT_QUAL IS NOT NULL
          AND TRIM(RESULT_QUAL) <> ''''
          AND UPPER(TRIM(RESULT_QUAL)) NOT IN (''NI'',''UN'',''OT''),
          1, 0) AS has_qual,
      /* Use RAW_RESULT as "text result" proxy for RESULT_TEXT */
      IFF(RAW_RESULT IS NOT NULL AND TRIM(RAW_RESULT) <> '''', 1, 0) AS has_text,
      IFF(RESULT_UNIT IS NOT NULL
          AND TRIM(RESULT_UNIT) <> ''''
          AND UPPER(TRIM(RESULT_UNIT)) NOT IN (''NI'',''UN'',''OT''),
          1, 0) AS has_unit,
      /* Normal range fully specified (matches the reference rule patterns) */
      IFF(
        has_quant = 1 AND (
          (UPPER(TRIM(NORM_MODIFIER_LOW)) = ''EQ'' AND UPPER(TRIM(NORM_MODIFIER_HIGH)) = ''EQ''
            AND NORM_RANGE_LOW IS NOT NULL AND NORM_RANGE_HIGH IS NOT NULL)
          OR
          (UPPER(TRIM(NORM_MODIFIER_LOW)) IN (''GT'',''GE'') AND UPPER(TRIM(NORM_MODIFIER_HIGH)) = ''NO''
            AND NORM_RANGE_LOW IS NOT NULL AND NORM_RANGE_HIGH IS NULL)
          OR
          (UPPER(TRIM(NORM_MODIFIER_HIGH)) IN (''LE'',''LT'') AND UPPER(TRIM(NORM_MODIFIER_LOW)) = ''NO''
            AND NORM_RANGE_HIGH IS NOT NULL AND NORM_RANGE_LOW IS NULL)
        ),
        1, 0
      ) AS has_full_range
    FROM ${lab}
  )
  SELECT
    COUNT(*) AS records_n,
    SUM(mapped) AS known_test_n,
    SUM(IFF(mapped=1 AND (has_quant=1 OR has_qual=1 OR has_text=1), 1, 0)) AS known_test_result_n,
    SUM(IFF(mapped=1 AND has_quant=1, 1, 0)) AS known_test_result_num_n,
    SUM(IFF(mapped=1 AND has_quant=1 AND has_unit=1, 1, 0)) AS known_test_result_num_unit_n,
    SUM(IFF(mapped=1 AND has_full_range=1, 1, 0)) AS known_test_num_result_range_n
  FROM x
  `
);
rs.next();
const recordsN = Number(rs.getColumnValue(1));
const knownTestN = Number(rs.getColumnValue(2));
const knownTestResultN = Number(rs.getColumnValue(3));
const knownTestResultNumN = Number(rs.getColumnValue(4));
const knownTestResultNumUnitN = Number(rs.getColumnValue(5));
const knownTestNumResultRangeN = Number(rs.getColumnValue(6));
function pct(n) { return (recordsN > 0) ? (n / recordsN) * 100 : 0; }
const knownTestPct = pct(knownTestN);
const knownTestResultPct = pct(knownTestResultN);
const knownTestResultNumPct = pct(knownTestResultNumN);
const knownTestResultNumUnitPct = pct(knownTestResultNumUnitN);
const knownTestNumResultRangePct = pct(knownTestNumResultRangeN);
// Only KNOWN_TEST_RESULT is the DC 3.09 exception signal (pct < 80)
const flagKnownTestResult = (recordsN > 0 && knownTestResultPct < thresholdPct) ? 1 : 0;
function emitCategory(sourceTableRow, categoryN, categoryPct, applyThreshold, flag) {
  const r = ref[sourceTableRow] || {};
  const details = {
    source_table_row: sourceTableRow,
    description: r.description || null,
    criteria: {
      code: r.code_criteria || null,
      result_num: r.result_num_criteria || null,
      result_modifier: r.result_modifier_criteria || null,
      other: r.other_criteria || null
    },
    records_n: recordsN,
    category_n: categoryN,
    category_pct: categoryPct,
    threshold_pct_lt: applyThreshold ? thresholdPct : null
  };
  const th = applyThreshold ? thresholdPct : null;
  insertMetric(resultsTbl, base, sourceTableRow, "RECORDS_N", recordsN, String(recordsN), th, false, details);
  insertMetric(resultsTbl, base, sourceTableRow, "CATEGORY_N", categoryN, String(categoryN), th, false, details);
  insertMetric(resultsTbl, base, sourceTableRow, "CATEGORY_PCT", categoryPct, String(categoryPct), th, false, details);
  insertMetric(resultsTbl, base, sourceTableRow, "CATEGORY_FLAG", flag, String(flag), th, applyThreshold && flag === 1, details);
}
// informational
emitCategory("KNOWN_TEST", knownTestN, knownTestPct, false, 0);
// 3.09 exception driver
emitCategory("KNOWN_TEST_RESULT", knownTestResultN, knownTestResultPct, true, flagKnownTestResult);
// additional informational categories (useful for IVI/3.10)
emitCategory("KNOWN_TEST_RESULT_NUM", knownTestResultNumN, knownTestResultNumPct, false, 0);
emitCategory("KNOWN_TEST_RESULT_NUM_UNIT", knownTestResultNumUnitN, knownTestResultNumUnitPct, false, 0);
emitCategory("KNOWN_TEST_NUM_RESULT_RANGE", knownTestNumResultRangeN, knownTestNumResultRangePct, false, 0);
return `DC 3.09 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';