CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_10"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
const vStartDate = (START_DATE || '''').toString().trim() || null;
const vEndDate = (END_DATE || '''').toString().trim() || null;
const tableDateCol = {
  LAB_RESULT_CM: ''RESULT_DATE''
};
function dateFilterWhere(tbl) {
  const dc = tableDateCol[tbl] || null;
  if (!dc) return '''';
  let clause = '''';
  if (vStartDate) clause += ` AND TRY_TO_DATE(${dc}) >= TRY_TO_DATE(''${vStartDate}'')`;
  if (vEndDate) clause += ` AND TRY_TO_DATE(${dc}) <= TRY_TO_DATE(''${vEndDate}'')`;
  return clause;
}
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 3.10;
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
  insertMetric(resultsTbl, base, "KNOWN_TEST_NUM_RESULT_RANGE", "STATUS", null, "SKIPPED", thresholdPct, false, {
    message: "LAB_RESULT_CM table does not exist"
  });
  return `DC 3.10 SKIPPED: LAB_RESULT_CM missing`;
}
// Pull the reference row text for KNOWN_TEST_NUM_RESULT_RANGE (for DETAILS)
const refRs = q(
  `SELECT SOURCE_TABLE_ROW, DESCRIPTION, CODE_CRITERIA, RESULT_NUM_CRITERIA, RESULT_MODIFIER_CRITERIA, OTHER_CRITERIA
   FROM CHARACTERIZATION.EDC_REF.TBL_IVI_REF_RAW
   WHERE SOURCE_TABLE_ROW = ''KNOWN_TEST_NUM_RESULT_RANGE''
   QUALIFY ROW_NUMBER() OVER (ORDER BY SOURCE_TABLE_ROW) = 1`
);
let ref = { description: null, code_criteria: null, result_num_criteria: null, result_modifier_criteria: null, other_criteria: null };
if (refRs.next()) {
  ref = {
    description: refRs.getColumnValue(2),
    code_criteria: refRs.getColumnValue(3),
    result_num_criteria: refRs.getColumnValue(4),
    result_modifier_criteria: refRs.getColumnValue(5),
    other_criteria: refRs.getColumnValue(6)
  };
}
const lab = `${DB_PARAM}.${SCHEMA_NAME}.LAB_RESULT_CM`;
const rs = q(
  `
  WITH x AS (
    SELECT
      IFF(LAB_LOINC IS NOT NULL
          AND TRIM(LAB_LOINC) <> ''''
          AND UPPER(TRIM(LAB_LOINC)) NOT IN (''NI'',''UN'',''OT''),
          1, 0) AS mapped,
      IFF(RESULT_NUM IS NOT NULL
          AND RESULT_MODIFIER IS NOT NULL
          AND TRIM(RESULT_MODIFIER) <> ''''
          AND UPPER(TRIM(RESULT_MODIFIER)) NOT IN (''NI'',''UN'',''OT''),
          1, 0) AS quant_ok,
      IFF(
        quant_ok = 1 AND (
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
      ) AS full_range
    FROM ${lab}
    WHERE 1=1 ${dateFilterWhere(''LAB_RESULT_CM'')}
  )
  SELECT
    COUNT(*) AS records_n,
    SUM(IFF(mapped=1 AND quant_ok=1, 1, 0)) AS denom_quant_mapped_n,
    SUM(IFF(mapped=1 AND full_range=1, 1, 0)) AS numer_full_range_n
  FROM x
  `
);
rs.next();
const recordsN = Number(rs.getColumnValue(1));
const denom = Number(rs.getColumnValue(2));
const numer = Number(rs.getColumnValue(3));
const pct = (denom > 0) ? (numer / denom) * 100 : 0;
const flag = (denom > 0 && pct < thresholdPct) ? 1 : 0;
const details = {
  source_table_row: "KNOWN_TEST_NUM_RESULT_RANGE",
  description: ref.description,
  criteria: {
    code: ref.code_criteria,
    result_num: ref.result_num_criteria,
    result_modifier: ref.result_modifier_criteria,
    other: ref.other_criteria
  },
  records_n: recordsN,
  denom_quant_mapped_n: denom,
  numer_full_range_n: numer,
  full_range_pct_of_quant: pct,
  threshold_pct_lt: thresholdPct,
  denominator_definition: "mapped to LAB_LOINC AND quantitative result (RESULT_NUM present and RESULT_MODIFIER not in NI/UN/OT)",
  numerator_definition: "denominator AND normal range fully specified via NORM_* fields"
};
// Emit metrics
insertMetric(resultsTbl, base, "KNOWN_TEST_NUM_RESULT_RANGE", "RECORDS_N", recordsN, String(recordsN), thresholdPct, false, details);
insertMetric(resultsTbl, base, "KNOWN_TEST_NUM_RESULT_RANGE", "DENOM_QUANT_MAPPED_N", denom, String(denom), thresholdPct, false, details);
insertMetric(resultsTbl, base, "KNOWN_TEST_NUM_RESULT_RANGE", "NUM_FULL_RANGE_N", numer, String(numer), thresholdPct, false, details);
insertMetric(resultsTbl, base, "KNOWN_TEST_NUM_RESULT_RANGE", "FULL_RANGE_PCT_OF_QUANT", pct, String(pct), thresholdPct, false, details);
insertMetric(resultsTbl, base, "KNOWN_TEST_NUM_RESULT_RANGE", "FULL_RANGE_FLAG", flag, String(flag), thresholdPct, (flag === 1), details);
return `DC 3.10 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';