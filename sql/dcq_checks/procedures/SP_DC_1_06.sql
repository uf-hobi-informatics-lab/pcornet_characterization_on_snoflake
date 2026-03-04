CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_06"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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

function colInfo(db, schema, table, col) {
  const rs = q(
    `SELECT DATA_TYPE
     FROM ${db}.INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
     QUALIFY ROW_NUMBER() OVER (ORDER BY ORDINAL_POSITION) = 1`,
    [schema.toUpperCase(), table.toUpperCase(), col.toUpperCase()]
  );
  if (!rs.next()) return null;
  return { dataType: (rs.getColumnValue(1) || '''').toString() };
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
      ?, IFF(?=1, TRUE, FALSE), PARSE_JSON(?)
    `,
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
const rowNum = 1.06;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();

// Value sets only. Any non-null value not in the allowed list is out-of-spec.
const thresholdPct = 0;
const cdmDoc = ''2025_01_23_PCORnet_Common_Data_Model_v7dot0_parseable.xlsx'';

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

if (only === ''ALL'') {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
} else {
  if (!isSafeIdentPart(only)) throw new Error(`Invalid TARGET_TABLE: ${TARGET_TABLE}`);
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ? AND UPPER(EDC_TABLE) = ?`, [RUN_ID, rowNum, only]);
}

// Pull allowed values from CDM VALUESETS extracted from CHARACTERIZATION.EDC_REF.CDM_PARSEABLE_RAW.
// Preferred: create the view CHARACTERIZATION.EDC_REF.CDM_VALUESETS via create_cdm_reference_views.sql.
const vsTbl = ''CHARACTERIZATION.EDC_REF.CDM_VALUESETS'';

// Validate the reference exists.
const refOkRs = q(
  `SELECT COUNT(*)
   FROM CHARACTERIZATION.INFORMATION_SCHEMA.TABLES
   WHERE TABLE_SCHEMA = ''EDC_REF''
     AND TABLE_NAME = ''CDM_VALUESETS''`
);
refOkRs.next();
if (refOkRs.getColumnValue(1) <= 0) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPct, true,
    { message: ''Missing central reference view/table CHARACTERIZATION.EDC_REF.CDM_VALUESETS. Create it from CDM_PARSEABLE_RAW first.'' });
  return ''DC 1.06 ERROR: missing CHARACTERIZATION.EDC_REF.CDM_VALUESETS'';
}

const fieldsRs = q(
  `SELECT UPPER(TABLE_NAME) AS TABLE_NAME, UPPER(FIELD_NAME) AS FIELD_NAME
   FROM ${vsTbl}
   GROUP BY 1,2
   ORDER BY 1,2`
);

const evaluated = [];
const skipped = [];
let fieldCount = 0;

while (fieldsRs.next()) {
  const tbl = fieldsRs.getColumnValue(1);
  const col = fieldsRs.getColumnValue(2);
  if (!(only === ''ALL'' || only === tbl)) continue;
  if (!isSafeIdentPart(tbl) || !isSafeIdentPart(col)) continue;

  if (!tableExists(DB_PARAM, SCHEMA_NAME, tbl)) {
    skipped.push({ table: tbl, column: col, reason: ''missing_table'' });
    continue;
  }
  const info = colInfo(DB_PARAM, SCHEMA_NAME, tbl, col);
  if (!info) {
    skipped.push({ table: tbl, column: col, reason: ''missing_column'' });
    continue;
  }

  const dt = (info.dataType || '''').toUpperCase();
  const isString = (dt.includes(''CHAR'') || dt.includes(''TEXT'') || dt.includes(''STRING''));
  if (!isString) {
    skipped.push({ table: tbl, column: col, reason: `non_string_data_type:${dt}` });
    continue;
  }

  // Get allowed list for this field.
  const allowedRs = q(
    `SELECT VALUESET_ITEM
     FROM ${vsTbl}
     WHERE UPPER(TABLE_NAME) = ? AND UPPER(FIELD_NAME) = ?
     ORDER BY VALUESET_ITEM_ORDER, VALUESET_ITEM`,
    [tbl, col]
  );
  const allowed = [];
  while (allowedRs.next()) {
    const v = allowedRs.getColumnValue(1);
    if (v !== null && v !== undefined) allowed.push(v.toString());
  }
  if (allowed.length === 0) {
    skipped.push({ table: tbl, column: col, reason: ''no_allowed_values_in_reference'' });
    continue;
  }

  // Compute out-of-spec counts.
  const fq = `${DB_PARAM}.${SCHEMA_NAME}.${tbl}`;
  const inList = allowed.map(() => ''?'').join('','');
  const sql =
    `SELECT
       COUNT(*) AS denom_n,
       COUNT_IF(${col} IS NOT NULL AND TRIM(${col}::STRING) <> '''') AS non_missing_n,
       COUNT_IF(${col} IS NOT NULL AND TRIM(${col}::STRING) <> '''' AND UPPER(TRIM(${col}::STRING)) NOT IN (${inList})) AS out_of_spec_n
     FROM ${fq}`;
  const binds = allowed.map(v => v.toUpperCase());
  const rs = q(sql, binds);
  rs.next();
  const denom = Number(rs.getColumnValue(1));
  const nonMissing = Number(rs.getColumnValue(2));
  const outN = Number(rs.getColumnValue(3));
  const pct = (nonMissing > 0) ? (outN / nonMissing) * 100.0 : null;
  const flag = (nonMissing > 0 && pct !== null && pct > thresholdPct) ? 1 : 0;

  const details = {
    table: tbl,
    column: col,
    data_type: dt,
    denom_n: denom,
    non_missing_n: nonMissing,
    out_of_spec_n: outN,
    out_of_spec_pct: pct,
    threshold_pct_gt: thresholdPct,
    cdm_doc: cdmDoc,
    allowed_values_n: allowed.length,
    allowed_values_sample: allowed.slice(0, 50),
    definition: ''Out-of-spec: non-missing value not in CDM VALUESETS for the field (case-insensitive).''
  };

  insertMetric(resultsTbl, bindsBase, tbl, tbl, col, ''NON_MISSING_N'', nonMissing, String(nonMissing), thresholdPct, false, details);
  insertMetric(resultsTbl, bindsBase, tbl, tbl, col, ''OUT_OF_SPEC_N'', outN, String(outN), thresholdPct, false, details);
  insertMetric(resultsTbl, bindsBase, tbl, tbl, col, ''OUT_OF_SPEC_PCT'', pct, (pct === null ? null : String(pct)), thresholdPct, false, details);
  insertMetric(resultsTbl, bindsBase, tbl, tbl, col, ''OUT_OF_SPEC_FLAG'', flag, String(flag), thresholdPct, (flag === 1), details);

  evaluated.push({ table: tbl, column: col });
  fieldCount += 1;
}

const status = (fieldCount === 0) ? ''ERROR'' : ''OK'';
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
    skipped: skipped,
    reference_table: vsTbl
  }
);

return `DC 1.06 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} evaluated_fields=${evaluated.length}`;
';