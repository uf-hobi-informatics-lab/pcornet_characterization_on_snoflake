CREATE OR REPLACE PROCEDURE "SP_DC_1_20"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR)
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
const rowNum = 1.20;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();

if (!(only === ''ALL'' || only === ''LAB_RESULT_CM'' || only === ''OBS_CLIN'' || only === ''PRO_CM'')) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, LAB_RESULT_CM, OBS_CLIN, or PRO_CM.`);
}

// Threshold per SAS Table IIG: flag when percent of LOINC records that are panels > 5%.
const thresholdPct = 5;

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

// Reference: LOINC table with PANEL_TYPE (from loinc.cpt -> CHARACTERIZATION.EDC_REF.LOINC_REF_RAW)
const refDb = ''CHARACTERIZATION'';
const refSchema = ''EDC_REF'';
const refTable = ''LOINC_REF_RAW'';
if (!tableExists(refDb, refSchema, refTable)) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''CHARACTERIZATION.EDC_REF.LOINC_REF_RAW'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPct, true,
    { message: ''Missing CHARACTERIZATION.EDC_REF.LOINC_REF_RAW (load loinc.cpt reference first).'' });
  return ''DC 1.20 ERROR: missing CHARACTERIZATION.EDC_REF.LOINC_REF_RAW'';
}

const refLoincCol = pickFirstExisting(refDb, refSchema, refTable, [''LOINC_NUM'', ''LOINC'', ''LOINC_CODE'']);
const refPanelCol = pickFirstExisting(refDb, refSchema, refTable, [''PANEL_TYPE'', ''PANELTYPE'', ''PANEL'']);
if (!refLoincCol || !refPanelCol) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''CHARACTERIZATION.EDC_REF.LOINC_REF_RAW'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPct, true,
    { message: ''LOINC_REF_RAW missing required columns'', ref_loinc_col: refLoincCol, ref_panel_col: refPanelCol });
  return ''DC 1.20 ERROR: LOINC_REF_RAW missing expected columns'';
}

const loincRefFq = `${refDb}.${refSchema}.${refTable}`;
const evaluated = [];
const skipped = [];

function runOne(edcTable, codeCol, typeCol, typeWant) {
  if (!(only === ''ALL'' || only === edcTable)) return;

  if (!tableExists(DB_PARAM, SCHEMA_NAME, edcTable)) {
    skipped.push({ table: edcTable, reason: ''missing_table'' });
    return;
  }
  if (!colExists(DB_PARAM, SCHEMA_NAME, edcTable, codeCol)) {
    skipped.push({ table: edcTable, column: codeCol, reason: ''missing_code_column'' });
    return;
  }
  if (typeCol && !colExists(DB_PARAM, SCHEMA_NAME, edcTable, typeCol)) {
    // If the type column doesn''t exist, run without the type filter.
    typeCol = null;
    typeWant = null;
  }

  const fq = `${DB_PARAM}.${SCHEMA_NAME}.${edcTable}`;
  const codeExpr = `UPPER(TRIM(${codeCol}::STRING))`;
  const hasCodePred = `${codeCol} IS NOT NULL AND TRIM(${codeCol}::STRING) <> ''''`;
  const typePred = (typeCol && typeWant)
    ? `AND UPPER(TRIM(${typeCol}::STRING)) = ''${typeWant}''`
    : '''';

  // Join to reference and count panels.
  const rs = q(
    `WITH base AS (
       SELECT ${codeExpr} AS loinc_code
       FROM ${fq}
       WHERE ${hasCodePred}
       ${typePred}
     ),
     joined AS (
       SELECT
         b.loinc_code,
         r.${refPanelCol} AS panel_type
       FROM base b
       LEFT JOIN ${loincRefFq} r
         ON UPPER(TRIM(r.${refLoincCol}::STRING)) = b.loinc_code
     )
     SELECT
       (SELECT COUNT(*) FROM base) AS loinc_records_n,
       COUNT_IF(UPPER(TRIM(panel_type::STRING)) = ''PANEL'') AS panel_n
     FROM joined`
  );
  rs.next();
  const denom = Number(rs.getColumnValue(1));
  const panelN = Number(rs.getColumnValue(2));
  const pct = (denom > 0) ? (panelN / denom) * 100.0 : 0;
  const flag = (pct > thresholdPct) ? 1 : 0;

  const details = {
    table: edcTable,
    code_column: codeCol,
    type_column: typeCol,
    type_filter: typeWant,
    loinc_records_n: denom,
    panel_n: panelN,
    panel_pct: pct,
    threshold_pct_gt: thresholdPct,
    loinc_ref: `${refDb}.${refSchema}.${refTable}`,
    loinc_ref_code_col: refLoincCol,
    loinc_ref_panel_col: refPanelCol,
    definition: "Percent of LOINC-coded records whose LOINC maps to PANEL_TYPE=''Panel'' in the LOINC reference."
  };

  insertMetric(resultsTbl, bindsBase, edcTable, edcTable, codeCol, ''LOINC_RECORD_N'', denom, String(denom), thresholdPct, false, details);
  insertMetric(resultsTbl, bindsBase, edcTable, edcTable, codeCol, ''PANEL_N'', panelN, String(panelN), thresholdPct, false, details);
  insertMetric(resultsTbl, bindsBase, edcTable, edcTable, codeCol, ''PANEL_PCT'', pct, String(pct), thresholdPct, false, details);
  insertMetric(resultsTbl, bindsBase, edcTable, edcTable, codeCol, ''PANEL_FLAG'', flag, String(flag), thresholdPct, (flag === 1), details);

  evaluated.push({ table: edcTable, column: codeCol });
}

runOne(''LAB_RESULT_CM'', ''LAB_LOINC'', null, null);
runOne(''OBS_CLIN'', ''OBSCLIN_CODE'', ''OBSCLIN_TYPE'', ''LC'');
runOne(''PRO_CM'', ''PRO_CODE'', ''PRO_TYPE'', ''LC'');

const status = (evaluated.length === 0) ? ''ERROR'' : ''OK'';
insertMetric(
  resultsTbl,
  bindsBase,
  (only === ''ALL'' ? ''ALL'' : only),
  ''ALL'',
  ''ALL'',
  ''STATUS'',
  null,
  status,
  thresholdPct,
  (status === ''ERROR''),
  {
    target_table: only,
    evaluated_fields_n: evaluated.length,
    skipped_fields_n: skipped.length,
    skipped: skipped
  }
);

return `DC 1.20 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} evaluated_fields=${evaluated.length}`;
';