CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_07"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
const rowNum = 1.07;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();

// CDM conformance: "required, not null" fields should not be missing.
// Missing for string fields includes blank after trim.
const thresholdMissingN = 0;

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

// Required-not-null fields from the parseable CDM reference.
// Preferred: create the view CHARACTERIZATION.EDC_REF.CDM_CONSTRAINTS via create_cdm_reference_views.sql.
const consView = ''CHARACTERIZATION.EDC_REF.CDM_CONSTRAINTS'';
const viewOkRs = q(
  `SELECT COUNT(*)
   FROM CHARACTERIZATION.INFORMATION_SCHEMA.VIEWS
   WHERE TABLE_SCHEMA = ''EDC_REF''
     AND TABLE_NAME = ''CDM_CONSTRAINTS''`
);
viewOkRs.next();
if (viewOkRs.getColumnValue(1) <= 0) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdMissingN, true,
    { message: ''Missing central view CHARACTERIZATION.EDC_REF.CDM_CONSTRAINTS. Create it from CDM_PARSEABLE_RAW first.'' });
  return ''DC 1.07 ERROR: missing CHARACTERIZATION.EDC_REF.CDM_CONSTRAINTS'';
}

const reqRs = q(
  `WITH REQ AS (
     SELECT
       TABLE_NAME,
       FIELD_NAME,
       CONSTRAINT_TEXT
     FROM ${consView}
     WHERE LOWER(CONSTRAINT_TEXT) LIKE ''%required%''
       AND LOWER(CONSTRAINT_TEXT) LIKE ''%not null%''
   ),
   FLAT AS (
     SELECT
       TABLE_NAME,
       TRIM(VALUE::STRING) AS FIELD_NAME
     FROM REQ,
     LATERAL FLATTEN(INPUT => SPLIT(FIELD_NAME, ''+''))
   )
   SELECT UPPER(TABLE_NAME) AS TABLE_NAME, UPPER(FIELD_NAME) AS FIELD_NAME
   FROM FLAT
   WHERE FIELD_NAME IS NOT NULL AND FIELD_NAME <> ''''
   GROUP BY 1,2
   ORDER BY 1,2`
);

const evaluated = [];
const skipped = [];
let evalCount = 0;

while (reqRs.next()) {
  const tbl = reqRs.getColumnValue(1);
  const col = reqRs.getColumnValue(2);
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
  const missingPred = isString
    ? `(${col} IS NULL OR TRIM(${col}::STRING) = '''')`
    : `(${col} IS NULL)`;

  const fq = `${DB_PARAM}.${SCHEMA_NAME}.${tbl}`;
  const rs = q(
    `SELECT
       COUNT(*) AS denom_n,
       COUNT_IF(${missingPred}) AS missing_n
     FROM ${fq}`
  );
  rs.next();
  const denom = Number(rs.getColumnValue(1));
  const missingN = Number(rs.getColumnValue(2));
  const pct = (denom > 0) ? (missingN / denom) * 100.0 : null;
  const flag = (missingN > thresholdMissingN) ? 1 : 0;

  const details = {
    table: tbl,
    column: col,
    data_type: dt,
    denom_n: denom,
    missing_n: missingN,
    missing_pct: pct,
    threshold_missing_n_gt: thresholdMissingN,
    key_field_source: ''CDM_CONSTRAINTS_REQUIRED_NOT_NULL'',
    definition: ''Counts missing values for fields marked required, not null in the CDM (NULL; and for strings: also blank).''
  };

  insertMetric(resultsTbl, bindsBase, tbl, tbl, col, ''DENOM_N'', denom, String(denom), thresholdMissingN, false, details);
  insertMetric(resultsTbl, bindsBase, tbl, tbl, col, ''MISSING_N'', missingN, String(missingN), thresholdMissingN, false, details);
  insertMetric(resultsTbl, bindsBase, tbl, tbl, col, ''MISSING_PCT'', pct, (pct === null ? null : String(pct)), thresholdMissingN, false, details);
  insertMetric(resultsTbl, bindsBase, tbl, tbl, col, ''MISSING_FLAG'', flag, String(flag), thresholdMissingN, (flag === 1), details);

  evaluated.push({ table: tbl, column: col });
  evalCount += 1;
}

const status = (evalCount === 0) ? ''ERROR'' : ''OK'';
insertMetric(
  resultsTbl,
  bindsBase,
  (only === ''ALL'' ? ''ALL'' : only),
  ''ALL'',
  ''ALL'',
  ''STATUS'',
  null,
  status,
  thresholdMissingN,
  (status === ''ERROR''),
  {
    target_table: only,
    evaluated_fields_n: evaluated.length,
    skipped_fields_n: skipped.length,
    skipped: skipped,
    reference_view: consView
  }
);

return `DC 1.07 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} evaluated_fields=${evaluated.length}`;
';