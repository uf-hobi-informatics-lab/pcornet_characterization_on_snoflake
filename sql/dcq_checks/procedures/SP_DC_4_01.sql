CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_4_01"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
function q(sqlText, binds) { return snowflake.execute({ sqlText: sqlText, binds: binds || [] }); }
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
if (!isSafeIdentPart(PREV_DB_PARAM)) throw new Error(`Unsafe PREV_DB_PARAM: ${PREV_DB_PARAM}`);
if (!isSafeIdentPart(PREV_SCHEMA_NAME)) throw new Error(`Unsafe PREV_SCHEMA_NAME: ${PREV_SCHEMA_NAME}`);

const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 4.01;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();
if (!(only === ''ALL'' || isSafeIdentPart(only))) throw new Error(`Invalid TARGET_TABLE: ${TARGET_TABLE}`);

// SAS flags decreases >5% (i.e., percent change < -5.00) or current=0 when previous>0.
const thresholdPctLt = -5.0;

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

// Tables and counting rules aligned to SAS Table VA.
// record_pk: count of all records in table
// patient_col: count distinct patients (where applicable)
const defs = [
  { table: ''DEMOGRAPHIC'', record_pk: null, patient_col: ''PATID'' },
  { table: ''ENROLLMENT'', record_pk: null, patient_col: ''PATID'' },
  { table: ''ENCOUNTER'', record_pk: null, patient_col: ''PATID'' },
  { table: ''DIAGNOSIS'', record_pk: null, patient_col: ''PATID'' },
  { table: ''PROCEDURES'', record_pk: null, patient_col: ''PATID'' },
  { table: ''VITAL'', record_pk: null, patient_col: ''PATID'' },
  { table: ''DEATH'', record_pk: null, patient_col: ''PATID'' },
  { table: ''PRESCRIBING'', record_pk: null, patient_col: ''PATID'' },
  { table: ''DISPENSING'', record_pk: null, patient_col: ''PATID'' },
  { table: ''LAB_RESULT_CM'', record_pk: null, patient_col: ''PATID'' },
  { table: ''CONDITION'', record_pk: null, patient_col: ''PATID'' },
  { table: ''DEATH_CAUSE'', record_pk: null, patient_col: ''PATID'' },
  { table: ''PRO_CM'', record_pk: null, patient_col: ''PATID'' },
  { table: ''PROVIDER'', record_pk: null, patient_col: ''PROVIDERID'' },
  { table: ''MED_ADMIN'', record_pk: null, patient_col: ''PATID'' },
  { table: ''OBS_CLIN'', record_pk: null, patient_col: ''PATID'' },
  { table: ''OBS_GEN'', record_pk: null, patient_col: ''PATID'' },
  { table: ''HASH_TOKEN'', record_pk: null, patient_col: ''PATID'' },
  { table: ''IMMUNIZATION'', record_pk: null, patient_col: ''PATID'' },
  { table: ''LDS_ADDRESS_HISTORY'', record_pk: null, patient_col: ''PATID'' },
  { table: ''LAB_HISTORY'', record_pk: null, patient_col: ''LAB_FACILITYID'' },
  { table: ''EXTERNAL_MEDS'', record_pk: null, patient_col: ''PATID'' },
  { table: ''PAT_RELATIONSHIP'', record_pk: null, patient_col: ''PATID_1'' }
];

function getCounts(db, schema, table, patientCol) {
  if (!tableExists(db, schema, table)) {
    return { present: false, records_n: 0, patients_n: 0, missing_reason: ''missing_table'' };
  }
  const fq = `${db}.${schema}.${table}`;
  const recordsRs = q(`SELECT COUNT(*)::NUMBER FROM ${fq}`);
  recordsRs.next();
  const recordsN = Number(recordsRs.getColumnValue(1));

  let patientsN = null;
  if (patientCol && colExists(db, schema, table, patientCol)) {
    const patRs = q(`SELECT COUNT(DISTINCT ${patientCol})::NUMBER FROM ${fq}`);
    patRs.next();
    patientsN = Number(patRs.getColumnValue(1));
  } else {
    patientsN = null;
  }
  return { present: true, records_n: recordsN, patients_n: patientsN, missing_reason: null };
}

let anyFlag = 0;
const evaluated = [];
const skipped = [];

for (const d of defs) {
  if (!(only === ''ALL'' || only === d.table)) continue;

  const cur = getCounts(DB_PARAM, SCHEMA_NAME, d.table, d.patient_col);
  const prev = getCounts(PREV_DB_PARAM, PREV_SCHEMA_NAME, d.table, d.patient_col);

  if (!cur.present || !prev.present) {
    skipped.push({ table: d.table, current_present: cur.present, prev_present: prev.present, current_reason: cur.missing_reason, prev_reason: prev.missing_reason });
    continue;
  }

  const prevRec = prev.records_n;
  const curRec = cur.records_n;
  const recPct = (prevRec > 0) ? ((curRec - prevRec) / prevRec) * 100.0 : null;

  const prevPat = prev.patients_n;
  const curPat = cur.patients_n;
  const patPct = (prevPat !== null && prevPat > 0 && curPat !== null) ? ((curPat - prevPat) / prevPat) * 100.0 : null;

  const recFlag = ((curRec === 0 && prevRec > 0) || (recPct !== null && recPct < thresholdPctLt));
  const patFlag = ((curPat !== null && prevPat !== null) && ((curPat === 0 && prevPat > 0) || (patPct !== null && patPct < thresholdPctLt)));
  const flag = recFlag || patFlag;
  if (flag) anyFlag = 1;

  const details = {
    table: d.table,
    current: { db: DB_PARAM, schema: SCHEMA_NAME, records_n: curRec, patients_n: curPat },
    previous: { db: PREV_DB_PARAM, schema: PREV_SCHEMA_NAME, records_n: prevRec, patients_n: prevPat },
    record_pct_change: recPct,
    patient_pct_change: patPct,
    threshold_pct_lt: thresholdPctLt,
    flag_rules: ''Flag if current=0 and previous>0 OR percent_change < -5.0'',
    patient_count_col: d.patient_col
  };

  insertMetric(resultsTbl, bindsBase, d.table, d.table, ''RECORDS'', ''PREV_N'', prevRec, String(prevRec), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, d.table, d.table, ''RECORDS'', ''CUR_N'', curRec, String(curRec), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, d.table, d.table, ''RECORDS'', ''PCT_CHANGE'', recPct, (recPct === null ? null : String(recPct)), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, d.table, d.table, ''RECORDS'', ''DECREASE_FLAG'', (recFlag ? 1 : 0), String(recFlag ? 1 : 0), thresholdPctLt, recFlag, details);

  if (curPat !== null && prevPat !== null) {
    insertMetric(resultsTbl, bindsBase, d.table, d.table, ''PATIENTS'', ''PREV_DISTINCT_N'', prevPat, String(prevPat), thresholdPctLt, false, details);
    insertMetric(resultsTbl, bindsBase, d.table, d.table, ''PATIENTS'', ''CUR_DISTINCT_N'', curPat, String(curPat), thresholdPctLt, false, details);
    insertMetric(resultsTbl, bindsBase, d.table, d.table, ''PATIENTS'', ''PCT_CHANGE'', patPct, (patPct === null ? null : String(patPct)), thresholdPctLt, false, details);
    insertMetric(resultsTbl, bindsBase, d.table, d.table, ''PATIENTS'', ''DECREASE_FLAG'', (patFlag ? 1 : 0), String(patFlag ? 1 : 0), thresholdPctLt, patFlag, details);
  } else {
    insertMetric(resultsTbl, bindsBase, d.table, d.table, ''PATIENTS'', ''STATUS'', null, ''SKIPPED_NO_PATIENT_COL'', thresholdPctLt, false,
      { table: d.table, patient_count_col: d.patient_col, message: ''Patient distinct count skipped: missing patient id column in one or both snapshots.'' }
    );
  }

  insertMetric(resultsTbl, bindsBase, d.table, d.table, ''ALL'', ''ANY_DECREASE_FLAG'', (flag ? 1 : 0), String(flag ? 1 : 0), thresholdPctLt, flag, details);
  evaluated.push(d.table);
}

insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''OK'', thresholdPctLt, false,
  { target_table: only, prev_db: PREV_DB_PARAM, prev_schema: PREV_SCHEMA_NAME, evaluated: evaluated, skipped: skipped }
);

return `DC 4.01 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} PREV=${PREV_DB_PARAM}.${PREV_SCHEMA_NAME}`;
';