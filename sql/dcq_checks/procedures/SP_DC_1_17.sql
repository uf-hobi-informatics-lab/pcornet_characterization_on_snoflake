CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_17"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
const rowNum = 1.17;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();

if (!(only === ''ALL'' || only === ''ENCOUNTER'' || only === ''LDS_ADDRESS_HISTORY'')) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, ENCOUNTER, or LDS_ADDRESS_HISTORY.`);
}

// CDM conformance: zip fields must be digits-only and expected length.
const thresholdBadN = 0;

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

const specs = [
  { table: ''ENCOUNTER'', column: ''FACILITY_LOCATION'', expectLen: 5 },
  { table: ''LDS_ADDRESS_HISTORY'', column: ''ADDRESS_ZIP5'', expectLen: 5 },
  { table: ''LDS_ADDRESS_HISTORY'', column: ''ADDRESS_ZIP9'', expectLen: 9 }
];

const evaluated = [];
const skipped = [];

for (const s of specs) {
  if (!(only === ''ALL'' || only === s.table)) continue;
  if (!tableExists(DB_PARAM, SCHEMA_NAME, s.table)) {
    skipped.push({ table: s.table, column: s.column, reason: ''missing_table'' });
    continue;
  }
  const info = colInfo(DB_PARAM, SCHEMA_NAME, s.table, s.column);
  if (!info) {
    skipped.push({ table: s.table, column: s.column, reason: ''missing_column'' });
    continue;
  }
  const dt = (info.dataType || '''').toUpperCase();
  const isString = (dt.includes(''CHAR'') || dt.includes(''TEXT'') || dt.includes(''STRING'') || dt.includes(''VARCHAR''));
  if (!isString) {
    skipped.push({ table: s.table, column: s.column, reason: `non_string_data_type:${dt}` });
    continue;
  }

  const fq = `${DB_PARAM}.${SCHEMA_NAME}.${s.table}`;
  const badPred = `(
    ${s.column} IS NOT NULL
    AND TRIM(${s.column}::STRING) <> ''''
    AND (
      REGEXP_LIKE(TRIM(${s.column}::STRING), ''.*[A-Za-z].*'')
      OR NOT REGEXP_LIKE(TRIM(${s.column}::STRING), ''^\\\\d{${s.expectLen}}$'')
    )
  )`;

  const rs = q(
    `SELECT
       COUNT(*) AS denom_n,
       COUNT_IF(${s.column} IS NOT NULL AND TRIM(${s.column}::STRING) <> '''') AS non_missing_n,
       COUNT_IF(${badPred}) AS bad_n
     FROM ${fq}`
  );
  rs.next();
  const denom = Number(rs.getColumnValue(1));
  const nonMissing = Number(rs.getColumnValue(2));
  const badN = Number(rs.getColumnValue(3));
  const badPct = (nonMissing > 0) ? (badN / nonMissing) * 100.0 : null;
  const flag = (badN > thresholdBadN) ? 1 : 0;

  const details = {
    table: s.table,
    column: s.column,
    expected_len: s.expectLen,
    denom_n: denom,
    non_missing_n: nonMissing,
    bad_n: badN,
    bad_pct: badPct,
    threshold_bad_n_gt: thresholdBadN,
    definition: ''Bad if non-missing value contains alphabetic characters OR does not have expected number of digits.''
  };

  insertMetric(resultsTbl, bindsBase, s.table, s.table, s.column, ''NON_MISSING_N'', nonMissing, String(nonMissing), thresholdBadN, false, details);
  insertMetric(resultsTbl, bindsBase, s.table, s.table, s.column, ''BAD_ZIP_N'', badN, String(badN), thresholdBadN, false, details);
  insertMetric(resultsTbl, bindsBase, s.table, s.table, s.column, ''BAD_ZIP_PCT'', badPct, (badPct === null ? null : String(badPct)), thresholdBadN, false, details);
  insertMetric(resultsTbl, bindsBase, s.table, s.table, s.column, ''BAD_ZIP_FLAG'', flag, String(flag), thresholdBadN, (flag === 1), details);

  evaluated.push({ table: s.table, column: s.column });
}

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
  thresholdBadN,
  (status === ''ERROR''),
  {
    target_table: only,
    evaluated_fields_n: evaluated.length,
    skipped_fields_n: skipped.length,
    skipped: skipped
  }
);

return `DC 1.17 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} evaluated_fields=${evaluated.length}`;
';