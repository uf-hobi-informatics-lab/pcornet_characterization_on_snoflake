CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_22"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
const rowNum = 1.22;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();

if (!(only === ''ALL'' || only === ''LDS_ADDRESS_HISTORY'')) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or LDS_ADDRESS_HISTORY.`);
}

// SAS provenance (edc_report.sas rows 4287, 4298, 4312, 4613-4640):
//   Row 1.22: "Patients have multiple current physical addresses in the
//   LDS_ADDRESS_HISTORY table". Exception fires when a PATID has multiple
//   ADDRESSIDs where CURRENT_ADDRESS_FLAG=Y. In SAS terms this is
//   LDSADRS_L3_CURRENT category=Y: if DISTINCT_PATID_N < RECORD_N then flag.
//   Equivalent SQL predicate:
//     COUNT(*) among CURRENT_ADDRESS_FLAG=Y rows > COUNT(DISTINCT PATID) among same.
const thresholdExcessN = 0;

q(`CREATE SCHEMA IF NOT EXISTS ${outSchema}`);
q(`CREATE TABLE IF NOT EXISTS ${resultsTbl} (
  RUN_ID STRING, CHECK_ID STRING, CHECK_NAME STRING, ROW_NUM NUMBER(10,2), EDC_TABLE STRING,
  SOURCE_TABLE STRING, CODE_TYPE STRING, METRIC STRING, VALUE_NUM NUMBER(38,10), VALUE_STR STRING,
  THRESHOLD_NUM NUMBER(38,10), EXCEPTION_FLAG BOOLEAN, DETAILS VARIANT,
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)`);

// Registry lookup for CHECK_ID and CHECK_NAME.
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

// Idempotent: clear any prior results for this RUN_ID + ROW_NUM.
q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);

const tableName = ''LDS_ADDRESS_HISTORY'';
if (!tableExists(DB_PARAM, SCHEMA_NAME, tableName)) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), tableName, ''LDSADRS_L3_CURRENT'', ''STATUS'', null, ''ERROR'', thresholdExcessN, true,
    { message: ''Missing table LDS_ADDRESS_HISTORY'' });
  return ''DC 1.22 ERROR: missing LDS_ADDRESS_HISTORY'';
}

const needCols = [''PATID'', ''ADDRESSID'', ''CURRENT_ADDRESS_FLAG''];
for (const c of needCols) {
  const info = colInfo(DB_PARAM, SCHEMA_NAME, tableName, c);
  if (!info) {
    insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), tableName, c, ''STATUS'', null, ''ERROR'', thresholdExcessN, true,
      { message: `Missing required column ${c} in LDS_ADDRESS_HISTORY` });
    return `DC 1.22 ERROR: missing LDS_ADDRESS_HISTORY.${c}`;
  }
}

const fq = `${DB_PARAM}.${SCHEMA_NAME}.${tableName}`;

// Filter to CURRENT_ADDRESS_FLAG=''Y'' (case/whitespace tolerant).
// SAS %n_pct groups on the raw value; the SAS category value ''Y'' matches
// literal ''Y''. Uppercase/trim here for robustness against dirty data.
const currentPred = `UPPER(TRIM(CURRENT_ADDRESS_FLAG::STRING)) = ''Y''`;

// One-scan aggregate of the CURRENT_ADDRESS_FLAG=Y slice.
// PATIDS_WITH_MULTIPLE_N requires a per-PATID grouping; compute via a
// windowed subquery in the same statement to keep the scan single-pass.
const rs = q(
  `WITH y_rows AS (
     SELECT PATID
     FROM ${fq}
     WHERE ${currentPred}
   ),
   per_patid AS (
     SELECT PATID, COUNT(*) AS rows_n
     FROM y_rows
     WHERE PATID IS NOT NULL
     GROUP BY PATID
   )
   SELECT
     (SELECT COUNT(*) FROM y_rows)                                 AS current_row_n,
     (SELECT COUNT(DISTINCT PATID) FROM y_rows)                    AS current_patid_distinct_n,
     COALESCE((SELECT COUNT(*) FROM per_patid WHERE rows_n > 1),0) AS patids_with_multiple_n`
);
rs.next();

const currentRowN = Number(rs.getColumnValue(1));
const currentPatidDistinctN = Number(rs.getColumnValue(2));
const patidsWithMultipleN = Number(rs.getColumnValue(3));
const excessRowsN = currentRowN - currentPatidDistinctN;
const flag = (excessRowsN > thresholdExcessN) ? 1 : 0;

const details = {
  table: tableName,
  current_row_n: currentRowN,
  current_patid_distinct_n: currentPatidDistinctN,
  patids_with_multiple_n: patidsWithMultipleN,
  excess_rows_n: excessRowsN,
  threshold_excess_n_gt: thresholdExcessN,
  definition: ''SAS LDSADRS_L3_CURRENT category=Y: flag when RECORD_N > DISTINCT_PATID_N, i.e. at least one PATID has multiple ADDRESSIDs with CURRENT_ADDRESS_FLAG=Y.''
};

insertMetric(resultsTbl, bindsBase, tableName, tableName, ''LDSADRS_L3_CURRENT'', ''CURRENT_ROW_N'',            currentRowN,            String(currentRowN),            thresholdExcessN, false, details);
insertMetric(resultsTbl, bindsBase, tableName, tableName, ''LDSADRS_L3_CURRENT'', ''CURRENT_PATID_DISTINCT_N'', currentPatidDistinctN, String(currentPatidDistinctN), thresholdExcessN, false, details);
insertMetric(resultsTbl, bindsBase, tableName, tableName, ''LDSADRS_L3_CURRENT'', ''PATIDS_WITH_MULTIPLE_N'',   patidsWithMultipleN,   String(patidsWithMultipleN),   thresholdExcessN, false, details);
insertMetric(resultsTbl, bindsBase, tableName, tableName, ''LDSADRS_L3_CURRENT'', ''EXCESS_ROWS_N'',            excessRowsN,           String(excessRowsN),           thresholdExcessN, false, details);
insertMetric(resultsTbl, bindsBase, tableName, tableName, ''LDSADRS_L3_CURRENT'', ''MULTI_CURRENT_FLAG'',       flag,                   String(flag),                   thresholdExcessN, (flag === 1), details);

insertMetric(
  resultsTbl,
  bindsBase,
  (only === ''ALL'' ? ''ALL'' : only),
  tableName,
  ''LDSADRS_L3_CURRENT'',
  ''STATUS'',
  null,
  ''OK'',
  thresholdExcessN,
  false,
  { evaluated_table: tableName }
);

return `DC 1.22 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} EXCESS_ROWS_N=${excessRowsN} FLAG=${flag}`;
';
