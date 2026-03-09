CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_2_07"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
const vStartDate = (START_DATE || '''').toString().trim() || null;
const vEndDate = (END_DATE || '''').toString().trim() || null;
const tableDateCol = {
  CONDITION: ''REPORT_DATE'',
  DEATH: ''DEATH_DATE'',
  DEMOGRAPHIC: null,
  DIAGNOSIS: ''DX_DATE'',
  DISPENSING: ''DISPENSE_DATE'',
  ENCOUNTER: ''ADMIT_DATE'',
  ENROLLMENT: ''ENR_START_DATE'',
  EXTERNAL_MEDS: ''EXT_RECORD_DATE'',
  HARVEST: null,
  HASH_TOKEN: null,
  IMMUNIZATION: ''VX_RECORD_DATE'',
  LAB_HISTORY: null,
  LAB_RESULT_CM: ''RESULT_DATE'',
  LDS_ADDRESS_HISTORY: null,
  MED_ADMIN: ''MEDADMIN_START_DATE'',
  OBS_CLIN: ''OBSCLIN_START_DATE'',
  OBS_GEN: ''OBSGEN_START_DATE'',
  PAT_RELATIONSHIP: null,
  PCORNET_TRIAL: null,
  PRESCRIBING: ''RX_ORDER_DATE'',
  PROCEDURES: ''PX_DATE'',
  PROVIDER: null,
  PRO_CM: ''PRO_DATE'',
  VITAL: ''MEASURE_DATE''
};
function dateFilterWhere(tbl) {
  const dc = tableDateCol[tbl] || null;
  if (!dc) return '''';
  let clause = '''';
  if (vStartDate) clause += ` AND TRY_TO_DATE(${dc}) >= TRY_TO_DATE(''${vStartDate}'')`;
  if (vEndDate) clause += ` AND TRY_TO_DATE(${dc}) <= TRY_TO_DATE(''${vEndDate}'')`;
  return clause;
}

const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 2.07;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();
if (!(only === ''ALL'' || only === ''ENCOUNTER'' || only === ''DIAGNOSIS'')) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, ENCOUNTER, or DIAGNOSIS.`);
}

// SAS Table IVE: flag when principal diagnoses per encounter with any principal diagnosis > 2.0 (IP/EI only)
const thresholdRateGt = 2.0;

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
if (!tableExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ENCOUNTERID'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ENC_TYPE'')) {
  insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdRateGt, true,
    { message: ''ENCOUNTER missing required table/columns (ENCOUNTERID, ENC_TYPE)'' });
  return ''DC 2.07 ERROR: ENCOUNTER missing required columns'';
}
if (!tableExists(DB_PARAM, SCHEMA_NAME, ''DIAGNOSIS'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''DIAGNOSIS'', ''ENCOUNTERID'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''DIAGNOSIS'', ''DX_ORIGIN'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''DIAGNOSIS'', ''PDX'')) {
  insertMetric(resultsTbl, bindsBase, edcTable, ''DIAGNOSIS'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdRateGt, true,
    { message: ''DIAGNOSIS missing required columns (ENCOUNTERID, DX_ORIGIN, PDX)'' });
  return ''DC 2.07 ERROR: DIAGNOSIS missing required columns'';
}

const enc = `${DB_PARAM}.${SCHEMA_NAME}.ENCOUNTER`;
const dia = `${DB_PARAM}.${SCHEMA_NAME}.DIAGNOSIS`;

const rs = q(
  `WITH enc_ip_ei AS (
     SELECT ENCOUNTERID, ENC_TYPE
     FROM ${enc}
     WHERE ENCOUNTERID IS NOT NULL
       AND ENC_TYPE IN (''IP'',''EI'')${dateFilterWhere(''ENCOUNTER'')}
   ),
   diag_known AS (
     SELECT
       d.ENCOUNTERID,
       UPPER(TRIM(d.DX_ORIGIN)) AS dx_origin,
       UPPER(TRIM(d.PDX)) AS pdx
     FROM ${dia} d
     JOIN enc_ip_ei e
       ON d.ENCOUNTERID = e.ENCOUNTERID
     WHERE d.ENCOUNTERID IS NOT NULL
       AND d.DX_ORIGIN IS NOT NULL
       AND TRIM(d.DX_ORIGIN) <> ''''
       AND UPPER(TRIM(d.DX_ORIGIN)) NOT IN (''NI'',''UN'',''OT'')${dateFilterWhere(''DIAGNOSIS'')}
   ),
   per_enc_origin AS (
     SELECT
       encounterid,
       dx_origin,
       SUM(IFF(pdx = ''P'', 1, 0)) AS principal_dx_records_n
     FROM diag_known
     GROUP BY encounterid, dx_origin
   ),
   agg AS (
     SELECT
       dx_origin,
       COUNT_IF(principal_dx_records_n > 0) AS enc_with_any_principal_dx_n,
       SUM(principal_dx_records_n) AS principal_dx_records_n
     FROM per_enc_origin
     GROUP BY dx_origin
   )
   SELECT
     dx_origin,
     enc_with_any_principal_dx_n,
     principal_dx_records_n,
     IFF(enc_with_any_principal_dx_n > 0,
         (principal_dx_records_n::FLOAT / enc_with_any_principal_dx_n::FLOAT),
         NULL
     ) AS principal_dx_per_enc_rate
   FROM agg
   ORDER BY dx_origin`
);

let wrote = 0;
let anyEnc = false;
let anyDiag = false;
while (rs.next()) {
  const origin = rs.getColumnValue(1);
  const encN = Number(rs.getColumnValue(2));
  const recN = Number(rs.getColumnValue(3));
  const rate = (rs.getColumnValue(4) === null) ? null : Number(rs.getColumnValue(4));
  const flag = (rate !== null && rate > thresholdRateGt) ? 1 : 0;
  const details = {
    enc_types: [''IP'',''EI''],
    dx_origin: origin,
    enc_with_any_principal_dx_n: encN,
    principal_dx_records_n: recN,
    principal_dx_per_enc_rate: rate,
    threshold_rate_gt: thresholdRateGt,
    definition: "For IP/EI encounters by DX_ORIGIN, rate = (# DIAGNOSIS rows with PDX=''P'') / (# encounters with >=1 principal diagnosis)"
  };
  insertMetric(resultsTbl, bindsBase, edcTable, ''DIAGNOSIS'', origin, ''ENC_WITH_ANY_PRINCIPAL_DX_N'', encN, String(encN), thresholdRateGt, false, details);
  insertMetric(resultsTbl, bindsBase, edcTable, ''DIAGNOSIS'', origin, ''PRINCIPAL_DX_RECORDS_N'', recN, String(recN), thresholdRateGt, false, details);
  insertMetric(resultsTbl, bindsBase, edcTable, ''DIAGNOSIS'', origin, ''PRINCIPAL_DX_PER_ENC_RATE'', rate, (rate === null ? null : String(rate)), thresholdRateGt, false, details);
  insertMetric(resultsTbl, bindsBase, edcTable, ''DIAGNOSIS'', origin, ''PRINCIPAL_DX_PER_ENC_FLAG'', flag, String(flag), thresholdRateGt, (flag === 1), details);
  wrote += 1;
}

// Determine whether zero output is expected (no IP/EI encounters) vs an actual evaluation failure.
const encCtRs = q(
  `SELECT COUNT(*)
   FROM ${enc}
   WHERE ENCOUNTERID IS NOT NULL
     AND ENC_TYPE IN (''IP'',''EI'')${dateFilterWhere(''ENCOUNTER'')}`
);
encCtRs.next();
anyEnc = Number(encCtRs.getColumnValue(1)) > 0;

const diagCtRs = q(
  `SELECT COUNT(*)
   FROM ${dia} d
   JOIN ${enc} e
     ON d.ENCOUNTERID = e.ENCOUNTERID
   WHERE e.ENC_TYPE IN (''IP'',''EI'')
     AND d.ENCOUNTERID IS NOT NULL
     AND d.DX_ORIGIN IS NOT NULL
     AND TRIM(d.DX_ORIGIN) <> ''''
     AND UPPER(TRIM(d.DX_ORIGIN)) NOT IN (''NI'',''UN'',''OT'')${dateFilterWhere(''ENCOUNTER'')}${dateFilterWhere(''DIAGNOSIS'')}`
);
diagCtRs.next();
anyDiag = Number(diagCtRs.getColumnValue(1)) > 0;

let status = ''OK'';
let statusException = false;
let note = '''';
if (wrote === 0 && !anyEnc) {
  status = ''OK'';
  statusException = false;
  note = ''No IP/EI encounters; check not applicable for this partner.'';
} else if (wrote === 0 && anyEnc && !anyDiag) {
  status = ''OK'';
  statusException = false;
  note = ''IP/EI encounters exist but no DIAGNOSIS rows with known DX_ORIGIN for those encounters; rate not computed.'';
} else if (wrote === 0 && anyEnc && anyDiag) {
  status = ''OK'';
  statusException = false;
  note = ''IP/EI encounters and known DX_ORIGIN diagnoses exist, but no DX_ORIGIN groups produced (likely no PDX=\\''P\\''); rate not computed.'';
}

insertMetric(
  resultsTbl,
  bindsBase,
  (only === ''ALL'' ? ''ALL'' : only),
  ''ALL'',
  ''ALL'',
  ''STATUS'',
  null,
  status,
  thresholdRateGt,
  statusException,
  {
    target_table: only,
    enc_types: [''IP'',''EI''],
    wrote_groups_n: wrote,
    has_ip_ei_encounters: anyEnc,
    has_ip_ei_known_origin_diagnoses: anyDiag,
    note: note
  }
);

return `DC 2.07 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';