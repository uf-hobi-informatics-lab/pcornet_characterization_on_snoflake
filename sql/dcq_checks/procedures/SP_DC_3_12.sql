CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_12"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR)
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

function pickFirstExisting(db, schema, table, candidates) {
  for (const c of candidates) {
    if (colExists(db, schema, table, c)) return c;
  }
  return null;
}

function findFirstLoincLikeColumn(db, schema, table) {
  const rs = q(
    `SELECT COLUMN_NAME
     FROM ${db}.INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = ?
       AND TABLE_NAME = ?
       AND UPPER(COLUMN_NAME) LIKE ''%LOINC%''
     ORDER BY ORDINAL_POSITION`,
    [schema.toUpperCase(), table.toUpperCase()]
  );
  const skip = new Set([
    ''LOINC_SOURCE'',
    ''LAB_LOINC_SOURCE'',
    ''LOINC_REF_SOURCE'',
    ''RAW_LOINC'',
    ''RAW_LAB_LOINC''
  ]);
  while (rs.next()) {
    const name = (rs.getColumnValue(1) || '''').toString();
    const up = name.toUpperCase();
    if (!skip.has(up)) return name;
  }
  return null;
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
const rowNum = 3.12;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();

if (!(only === ''ALL'' || only === ''LAB_RESULT_CM'')) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or LAB_RESULT_CM.`);
}

const thresholdPctLt = 80.0;
const validModifiers = [''EQ'',''GE'',''GT'',''LE'',''LT'',''NE''];

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
const bindsBase = [RUN_ID, checkId, checkName, rowNum];

q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);

// Reference list for "known tests": use the loaded LOINC reference.
// This is more robust than parsing criteria tables and aligns with other LOINC-based checks.
const refDb = ''CHARACTERIZATION'';
const refSchema = ''EDC_REF'';
const refTable = ''LOINC_REF_RAW'';
if (!tableExists(refDb, refSchema, refTable) || !colExists(refDb, refSchema, refTable, ''LOINC_NUM'')) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), `${refDb}.${refSchema}.${refTable}`, ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    { message: `Missing ${refDb}.${refSchema}.${refTable} with LOINC_NUM. Reload LOINC reference first.` });
  return ''DC 3.12 ERROR: missing LOINC reference'';
}

// LAB_RESULT_CM prerequisites
if (!tableExists(DB_PARAM, SCHEMA_NAME, ''LAB_RESULT_CM'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''LAB_RESULT_CM'', ''LAB_LOINC'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''LAB_RESULT_CM'', ''RESULT_NUM'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''LAB_RESULT_CM'', ''RESULT_UNIT'')) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''LAB_RESULT_CM'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    { message: ''LAB_RESULT_CM missing required columns (LAB_LOINC, RESULT_NUM, RESULT_UNIT)'' });
  return ''DC 3.12 ERROR: LAB_RESULT_CM missing required columns'';
}

const labFq = `${DB_PARAM}.${SCHEMA_NAME}.LAB_RESULT_CM`;
const refFq = `${refDb}.${refSchema}.${refTable}`;

const rs = q(
  `WITH ref_codes AS (
     SELECT DISTINCT UPPER(TRIM(LOINC_NUM::STRING)) AS loinc
     FROM ${refFq}
     WHERE LOINC_NUM IS NOT NULL
       AND TRIM(LOINC_NUM::STRING) <> ''''
   ),
   lab AS (
     SELECT
       UPPER(TRIM(LAB_LOINC::STRING)) AS loinc,
       RESULT_NUM,
       UPPER(TRIM(COALESCE(RESULT_MODIFIER::STRING,''''))) AS result_modifier,
       RESULT_UNIT
     FROM ${labFq}
     WHERE LAB_LOINC IS NOT NULL
       AND TRIM(LAB_LOINC::STRING) <> ''''
   ),
   quant AS (
     SELECT
       l.*
     FROM lab l
     JOIN ref_codes r
       ON r.loinc = l.loinc
     WHERE l.RESULT_NUM IS NOT NULL
       AND (l.result_modifier = '''' OR l.result_modifier IN (${validModifiers.map(v => `''${v}''`).join('','')}))
   )
   SELECT
     (SELECT COUNT(*) FROM ref_codes) AS ref_loinc_n,
     COUNT(*) AS denom_quant_known_n,
     COUNT_IF(RESULT_UNIT IS NOT NULL AND TRIM(RESULT_UNIT::STRING) <> '''') AS with_unit_n
   FROM quant`
);
rs.next();
const refLoincN = Number(rs.getColumnValue(1));
const denom = Number(rs.getColumnValue(2));
const numer = Number(rs.getColumnValue(3));
const pct = (denom > 0) ? (numer / denom) * 100.0 : null;
const flag = (denom > 0 && pct !== null && pct < thresholdPctLt) ? 1 : 0;

const details = {
  table: ''LAB_RESULT_CM'',
  denom_quant_known_n: denom,
  numer_with_unit_n: numer,
  pct_with_unit: pct,
  threshold_pct_lt: thresholdPctLt,
  valid_result_modifiers: validModifiers,
  ref_table: `${refDb}.${refSchema}.${refTable}`,
  ref_loinc_n: refLoincN,
  ref_loinc_key: ''LOINC_NUM'',
  definition: ''Among quantitative (RESULT_NUM not null) results for LAB_LOINC that exist in the LOINC reference, percent that specify RESULT_UNIT.''
};

insertMetric(resultsTbl, bindsBase, ''LAB_RESULT_CM'', ''LAB_RESULT_CM'', ''KNOWN_TEST_RESULT_NUM_UNIT'', ''DENOM_QUANT_KNOWN_N'', denom, String(denom), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''LAB_RESULT_CM'', ''LAB_RESULT_CM'', ''KNOWN_TEST_RESULT_NUM_UNIT'', ''NUM_WITH_UNIT_N'', numer, String(numer), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''LAB_RESULT_CM'', ''LAB_RESULT_CM'', ''KNOWN_TEST_RESULT_NUM_UNIT'', ''PCT_WITH_UNIT'', pct, (pct === null ? null : String(pct)), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''LAB_RESULT_CM'', ''LAB_RESULT_CM'', ''KNOWN_TEST_RESULT_NUM_UNIT'', ''WITH_UNIT_FLAG'', flag, String(flag), thresholdPctLt, (flag === 1), details);

insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''OK'', thresholdPctLt, false,
  { target_table: only, denom_quant_known_n: denom }
);

return `DC 3.12 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';