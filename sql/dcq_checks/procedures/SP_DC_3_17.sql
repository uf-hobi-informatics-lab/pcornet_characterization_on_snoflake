CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_17"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
function q(sqlText, binds) { return snowflake.execute({ sqlText: sqlText, binds: binds || [] }); }
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

function colExists(db, schema, table, col) {
  const rs = q(
    `SELECT COUNT(*)
     FROM ${db}.INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
    [schema.toUpperCase(), table.toUpperCase(), col.toUpperCase()]
  );
  rs.next();
  return rs.getColumnValue(1) > 0;
}

function insertMetric(resultsTbl, bindsBase, edcTableVal, sourceTableVal, codeTypeVal, metric, valueNum, valueStr, thresholdNum, exceptionFlag, detailsObj) {
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
      ?, ?, ?, ?, ?,
      ?, IFF(?=1, TRUE, FALSE), PARSE_JSON(?)`,
    bindsBase.concat([
      edcTableVal,
      sourceTableVal,
      codeTypeVal,
      metric,
      valueNum,
      valueStr,
      thresholdNum,
      flagInt,
      detailsJson
    ])
  );
}

if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);

const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 3.17;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();
if (!(only === ''ALL'' || only === ''OBS_CLIN'')) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or OBS_CLIN.`);
}

const thresholdPctLt = 80.0;
const validModifiers = [''EQ'',''GE'',''GT'',''LE'',''LT'',''NE''];
const invalidUnits = [''NI'',''UN'',''OT''];

q(`CREATE SCHEMA IF NOT EXISTS ${outSchema}`);
q(`CREATE TABLE IF NOT EXISTS ${resultsTbl} (
  RUN_ID STRING, CHECK_ID STRING, CHECK_NAME STRING, ROW_NUM NUMBER(10,2), EDC_TABLE STRING,
  SOURCE_TABLE STRING, CODE_TYPE STRING, METRIC STRING, VALUE_NUM NUMBER(38,10), VALUE_STR STRING,
  THRESHOLD_NUM NUMBER(38,10), EXCEPTION_FLAG BOOLEAN, DETAILS VARIANT,
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)`);

// registry constants
const regRs = q(
  `SELECT CHECK_ID, CHECK_NAME
   FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY
   WHERE ROW_NUM = ?
   QUALIFY ROW_NUMBER() OVER (ORDER BY CHECK_ID) = 1`,
  [rowNum]
);
if (!regRs.next()) throw new Error(`No registry row found for ROW_NUM=${rowNum}`);
const checkId = regRs.getColumnValue(1);
const checkName = regRs.getColumnValue(2);
const bindsBase = [RUN_ID, checkId, checkName, rowNum];

q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);

// Reference list for known codes: use the loaded LOINC reference.
const refDb = ''CHARACTERIZATION'';
const refSchema = ''EDC_REF'';
const refTable = ''LOINC_REF_RAW'';
if (!tableExists(refDb, refSchema, refTable) || !colExists(refDb, refSchema, refTable, ''LOINC_NUM'')) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), `${refDb}.${refSchema}.${refTable}`, ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    { message: `Missing ${refDb}.${refSchema}.${refTable} with LOINC_NUM. Reload LOINC reference first.` });
  return ''DC 3.17 ERROR: missing LOINC reference'';
}

// OBS_CLIN prerequisites
if (!tableExists(DB_PARAM, SCHEMA_NAME, ''OBS_CLIN'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''OBS_CLIN'', ''OBSCLIN_TYPE'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''OBS_CLIN'', ''OBSCLIN_CODE'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''OBS_CLIN'', ''OBSCLIN_RESULT_NUM'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''OBS_CLIN'', ''OBSCLIN_RESULT_MODIFIER'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''OBS_CLIN'', ''OBSCLIN_RESULT_UNIT'')) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''OBS_CLIN'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    { message: ''OBS_CLIN missing required columns (OBSCLIN_TYPE, OBSCLIN_CODE, OBSCLIN_RESULT_NUM, OBSCLIN_RESULT_MODIFIER, OBSCLIN_RESULT_UNIT)'' });
  return ''DC 3.17 ERROR: OBS_CLIN missing required columns'';
}

const obsFq = `${DB_PARAM}.${SCHEMA_NAME}.OBS_CLIN`;
const refFq = `${refDb}.${refSchema}.${refTable}`;

const rs = q(
  `WITH ref_codes AS (
     SELECT DISTINCT UPPER(TRIM(LOINC_NUM::STRING)) AS loinc
     FROM ${refFq}
     WHERE LOINC_NUM IS NOT NULL
       AND TRIM(LOINC_NUM::STRING) <> ''''
   ),
   obs AS (
     SELECT
       UPPER(TRIM(OBSCLIN_TYPE::STRING)) AS obsclin_type,
       UPPER(TRIM(COALESCE(OBSCLIN_CODE::STRING,''''))) AS obsclin_code,
       OBSCLIN_RESULT_NUM,
       UPPER(TRIM(COALESCE(OBSCLIN_RESULT_MODIFIER::STRING,''''))) AS result_modifier,
       UPPER(TRIM(COALESCE(OBSCLIN_RESULT_UNIT::STRING,''''))) AS result_unit
     FROM ${obsFq}
   ),
   quant_known AS (
     SELECT o.*
     FROM obs o
     JOIN ref_codes r
       ON r.loinc = o.obsclin_code
     WHERE o.obsclin_type = ''LC''
       AND o.OBSCLIN_RESULT_NUM IS NOT NULL
       AND (o.result_modifier = '''' OR o.result_modifier IN (${validModifiers.map(v => `''${v}''`).join('','')}))
   )
   SELECT
     (SELECT COUNT(*) FROM ref_codes) AS ref_loinc_n,
     COUNT(*) AS denom_quant_known_n,
     COUNT_IF(result_unit <> '''' AND result_unit NOT IN (${invalidUnits.map(v => `''${v}''`).join('','')})) AS numer_with_unit_n
   FROM quant_known`
);
rs.next();

const refLoincN = Number(rs.getColumnValue(1));
const denom = Number(rs.getColumnValue(2));
const numer = Number(rs.getColumnValue(3));
const pct = (denom > 0) ? (numer / denom) * 100.0 : null;
const flag = (denom > 0) && ((pct !== null && pct < thresholdPctLt) || numer === 0) ? 1 : 0;

const details = {
  table: ''OBS_CLIN'',
  denom_quant_known_n: denom,
  numer_with_unit_n: numer,
  pct_with_unit: pct,
  threshold_pct_lt: thresholdPctLt,
  valid_result_modifiers: validModifiers,
  invalid_unit_values: invalidUnits,
  ref_table: `${refDb}.${refSchema}.${refTable}`,
  ref_loinc_key: ''LOINC_NUM'',
  ref_loinc_n: refLoincN,
  known_code_definition: "OBSCLIN_TYPE=''LC'' and OBSCLIN_CODE exists in LOINC reference",
  definition: ''Among quantitative (OBSCLIN_RESULT_NUM not null) OBS_CLIN results for OBSCLIN_CODE that exist in the LOINC reference, percent that specify OBSCLIN_RESULT_UNIT.''
};

const codeType = ''KNOWN_TEST_RESULT_NUM_UNIT'';
insertMetric(resultsTbl, bindsBase, ''OBS_CLIN'', ''OBS_CLIN'', codeType, ''DENOM_QUANT_KNOWN_N'', denom, String(denom), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''OBS_CLIN'', ''OBS_CLIN'', codeType, ''NUM_WITH_UNIT_N'', numer, String(numer), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''OBS_CLIN'', ''OBS_CLIN'', codeType, ''PCT_WITH_UNIT'', pct, (pct === null ? null : String(pct)), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''OBS_CLIN'', ''OBS_CLIN'', codeType, ''WITH_UNIT_FLAG'', flag, String(flag), thresholdPctLt, (flag === 1), details);

insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''OK'', thresholdPctLt, false,
  { target_table: only, denom_quant_known_n: denom }
);

return `DC 3.17 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';