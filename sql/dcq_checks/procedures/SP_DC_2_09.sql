CREATE OR REPLACE PROCEDURE "SP_DC_2_09"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
function q(sqlText, binds) { return snowflake.execute({ sqlText, binds: binds || [] }); }
function scalar(sqlText, binds) { const rs = q(sqlText, binds); rs.next(); return rs.getColumnValue(1); }
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
const rowNum = 2.09;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();
if (!(only === ''ALL'' || only === ''DEMOGRAPHIC'' || only === ''ENCOUNTER'')) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, DEMOGRAPHIC, or ENCOUNTER.`);
}

const thresholdPctLt = 80.0;

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
const bindsBase = [RUN_ID, checkId, checkName, rowNum];

q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);

// prerequisites
if (!tableExists(DB_PARAM, SCHEMA_NAME, ''DEMOGRAPHIC'') || !colExists(DB_PARAM, SCHEMA_NAME, ''DEMOGRAPHIC'', ''PATID'')) {
  insertMetric(resultsTbl, bindsBase, edcTable, ''DEMOGRAPHIC'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    { message: ''DEMOGRAPHIC missing required table/column PATID'' });
  return ''DC 2.09 ERROR: DEMOGRAPHIC missing'';
}
if (!tableExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'') || !colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''PATID'') || !colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ADMIT_DATE'')) {
  insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    { message: ''ENCOUNTER missing required columns PATID, ADMIT_DATE'' });
  return ''DC 2.09 ERROR: ENCOUNTER missing required columns'';
}

const demo = `${DB_PARAM}.${SCHEMA_NAME}.DEMOGRAPHIC`;
const enc = `${DB_PARAM}.${SCHEMA_NAME}.ENCOUNTER`;

// Use max ADMIT_DATE as the anchor for the 5-year window.
const anchorDateStr = scalar(`SELECT TO_VARCHAR(MAX(ADMIT_DATE)::DATE, ''YYYY-MM-DD'') FROM ${enc} WHERE ADMIT_DATE IS NOT NULL`);
if (!anchorDateStr) {
  insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER'', ''ALL'', ''STATUS'', null, ''OK'', thresholdPctLt, false,
    { message: ''No non-null ENCOUNTER.ADMIT_DATE; cannot compute 5-year window.'', note: ''Treating as not applicable.'' });
  return `DC 2.09 finished RUN_ID=${RUN_ID} (no ADMIT_DATE)`;
}

const rs = q(
  `WITH anchor AS (
     SELECT TO_DATE(?) AS anchor_date,
            DATEADD(YEAR, -5, TO_DATE(?)) AS start_date
   ),
   denom AS (
     SELECT COUNT(DISTINCT PATID) AS denom_patid_n
     FROM ${demo}
     WHERE PATID IS NOT NULL
       AND TRIM(PATID::STRING) <> ''''
   ),
   enc_pats AS (
     SELECT COUNT(DISTINCT e.PATID) AS enc_patid_n
     FROM ${enc} e
     JOIN anchor a
       ON 1=1
     WHERE e.PATID IS NOT NULL
       AND TRIM(e.PATID::STRING) <> ''''
       AND e.ADMIT_DATE IS NOT NULL
       AND e.ADMIT_DATE::DATE >= a.start_date
       AND e.ADMIT_DATE::DATE <= a.anchor_date
   )
   SELECT
     d.denom_patid_n,
     e.enc_patid_n,
     IFF(d.denom_patid_n > 0, (e.enc_patid_n::FLOAT / d.denom_patid_n::FLOAT) * 100.0, NULL) AS pct
   FROM denom d
   CROSS JOIN enc_pats e`,
  [anchorDateStr, anchorDateStr]
);
rs.next();

const denomN = Number(rs.getColumnValue(1));
const encN = Number(rs.getColumnValue(2));
const pct = (rs.getColumnValue(3) === null) ? null : Number(rs.getColumnValue(3));
const flag = (pct !== null && pct < thresholdPctLt) ? 1 : 0;

const details = {
  anchor_date: anchorDateStr,
  window_years: 5,
  denom_demographic_distinct_patid_n: denomN,
  enc_distinct_patid_n_in_window: encN,
  pct: pct,
  threshold_pct_lt: thresholdPctLt,
  definition: ''Percent of DEMOGRAPHIC PATIDs with >=1 ENCOUNTER in the 5-year window ending at max ADMIT_DATE.''
};

insertMetric(resultsTbl, bindsBase, edcTable, ''DEMOGRAPHIC'', ''ALL'', ''DENOM_DISTINCT_PATID_N'', denomN, String(denomN), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER'', ''ALL'', ''NUM_DISTINCT_PATID_WITH_ENC_5YR_N'', encN, String(encN), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER'', ''ALL'', ''PCT_PATID_WITH_ENC_5YR'', pct, (pct === null ? null : String(pct)), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER'', ''ALL'', ''PCT_PATID_WITH_ENC_5YR_FLAG'', flag, String(flag), thresholdPctLt, (flag === 1), details);

insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''OK'', thresholdPctLt, false,
  { target_table: only }
);

return `DC 2.09 finished RUN_ID=${RUN_ID} anchor_date=${anchorDateStr}`;
';