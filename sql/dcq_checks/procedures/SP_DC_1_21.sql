CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_21"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
const rowNum = 1.21;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();

if (!(only === ''ALL'' || only === ''LDS_ADDRESS_HISTORY'')) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or LDS_ADDRESS_HISTORY.`);
}

// CDM conformance per SAS exception text:
// - STATE_FIPS must be exactly 2 digits
// - COUNTY_FIPS must be exactly 5 digits AND first 2 digits match STATE_FIPS
// - RUCA_ZIP must be digits-only (CDM field is Text(2), but observed values may include 1-digit codes)
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

const tableName = ''LDS_ADDRESS_HISTORY'';
if (!tableExists(DB_PARAM, SCHEMA_NAME, tableName)) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), tableName, ''ALL'', ''STATUS'', null, ''ERROR'', thresholdBadN, true,
    { message: ''Missing table LDS_ADDRESS_HISTORY'' });
  return ''DC 1.21 ERROR: missing LDS_ADDRESS_HISTORY'';
}

const needCols = [''STATE_FIPS'', ''COUNTY_FIPS'', ''RUCA_ZIP''];
for (const c of needCols) {
  const info = colInfo(DB_PARAM, SCHEMA_NAME, tableName, c);
  if (!info) {
    insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), tableName, c, ''STATUS'', null, ''ERROR'', thresholdBadN, true,
      { message: `Missing required column ${c} in LDS_ADDRESS_HISTORY` });
    return `DC 1.21 ERROR: missing LDS_ADDRESS_HISTORY.${c}`;
  }
}

const fq = `${DB_PARAM}.${SCHEMA_NAME}.${tableName}`;
// Non-missing defined as non-null and non-blank.
const nonMissingState = `(STATE_FIPS IS NOT NULL AND TRIM(STATE_FIPS::STRING) <> '''')`;
const nonMissingCounty = `(COUNTY_FIPS IS NOT NULL AND TRIM(COUNTY_FIPS::STRING) <> '''')`;
const nonMissingRuca = `(RUCA_ZIP IS NOT NULL AND TRIM(RUCA_ZIP::STRING) <> '''')`;

// Use explicit digit ranges (avoid backslash-escape inconsistencies).
const badState = `(${nonMissingState} AND NOT REGEXP_LIKE(TRIM(STATE_FIPS::STRING), ''^[0-9]{2}$''))`;
const badCountyDigits = `(${nonMissingCounty} AND NOT REGEXP_LIKE(TRIM(COUNTY_FIPS::STRING), ''^[0-9]{5}$''))`;
const badCountyStateMismatch = `(${nonMissingCounty} AND ${nonMissingState} AND REGEXP_LIKE(TRIM(COUNTY_FIPS::STRING), ''^[0-9]{5}$'') AND REGEXP_LIKE(TRIM(STATE_FIPS::STRING), ''^[0-9]{2}$'') AND SUBSTR(TRIM(COUNTY_FIPS::STRING), 1, 2) <> TRIM(STATE_FIPS::STRING))`;
const badRuca = `(${nonMissingRuca} AND NOT REGEXP_LIKE(TRIM(RUCA_ZIP::STRING), ''^[0-9]{1,2}$''))`;

const rs = q(
  `SELECT
     COUNT(*) AS denom_n,
     COUNT_IF(${nonMissingState}) AS state_non_missing_n,
     COUNT_IF(${badState}) AS state_bad_n,
     COUNT_IF(${nonMissingCounty}) AS county_non_missing_n,
     COUNT_IF(${badCountyDigits}) AS county_bad_digits_n,
     COUNT_IF(${badCountyStateMismatch}) AS county_state_mismatch_n,
     COUNT_IF(${nonMissingRuca}) AS ruca_non_missing_n,
     COUNT_IF(${badRuca}) AS ruca_bad_n
   FROM ${fq}`
);
rs.next();

const denom = Number(rs.getColumnValue(1));
const stateNonMissing = Number(rs.getColumnValue(2));
const stateBad = Number(rs.getColumnValue(3));
const countyNonMissing = Number(rs.getColumnValue(4));
const countyBadDigits = Number(rs.getColumnValue(5));
const countyMismatch = Number(rs.getColumnValue(6));
const rucaNonMissing = Number(rs.getColumnValue(7));
const rucaBad = Number(rs.getColumnValue(8));

const totalBad = stateBad + countyBadDigits + countyMismatch + rucaBad;
const flag = (totalBad > thresholdBadN) ? 1 : 0;

const details = {
  table: tableName,
  denom_n: denom,
  state_fips_non_missing_n: stateNonMissing,
  state_fips_bad_n: stateBad,
  county_fips_non_missing_n: countyNonMissing,
  county_fips_bad_digits_n: countyBadDigits,
  county_fips_state_mismatch_n: countyMismatch,
  ruca_zip_non_missing_n: rucaNonMissing,
  ruca_zip_bad_n: rucaBad,
  total_bad_n: totalBad,
  threshold_bad_n_gt: thresholdBadN,
  definition: ''Bad if geo fields contain alphabetic characters or do not have expected digits; additionally COUNTY_FIPS first 2 digits must match STATE_FIPS.''
};

insertMetric(resultsTbl, bindsBase, tableName, tableName, ''STATE_FIPS'', ''BAD_N'', stateBad, String(stateBad), thresholdBadN, false, details);
insertMetric(resultsTbl, bindsBase, tableName, tableName, ''COUNTY_FIPS'', ''BAD_DIGITS_N'', countyBadDigits, String(countyBadDigits), thresholdBadN, false, details);
insertMetric(resultsTbl, bindsBase, tableName, tableName, ''COUNTY_FIPS'', ''STATE_MISMATCH_N'', countyMismatch, String(countyMismatch), thresholdBadN, false, details);
insertMetric(resultsTbl, bindsBase, tableName, tableName, ''RUCA_ZIP'', ''BAD_N'', rucaBad, String(rucaBad), thresholdBadN, false, details);
insertMetric(resultsTbl, bindsBase, tableName, tableName, ''ALL'', ''TOTAL_BAD_N'', totalBad, String(totalBad), thresholdBadN, false, details);
insertMetric(resultsTbl, bindsBase, tableName, tableName, ''ALL'', ''GEO_FLAG'', flag, String(flag), thresholdBadN, (flag === 1), details);

insertMetric(
  resultsTbl,
  bindsBase,
  (only === ''ALL'' ? ''ALL'' : only),
  tableName,
  ''ALL'',
  ''STATUS'',
  null,
  ''OK'',
  thresholdBadN,
  false,
  { evaluated_table: tableName }
);

return `DC 1.21 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';