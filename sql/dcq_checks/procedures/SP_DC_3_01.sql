CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_01"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
function insertMetric(resultsTbl, baseBinds, codeType, metric, valueNum, valueStr, thresholdNum, exceptionFlag, detailsObj) {
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
      ''DIAGNOSIS'', ?, ?, ?, ?,
      ?, IFF(?=1, TRUE, FALSE), PARSE_JSON(?)
    `,
    baseBinds.concat([codeType, metric, valueNum, valueStr, thresholdNum, flagInt, detailsJson])
  );
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 3.01;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
const threshold = 1.0;
if (!(only === "ALL" || only === "DIAGNOSIS" || only === "ENCOUNTER")) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, DIAGNOSIS, or ENCOUNTER.`);
}
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
const base = [RUN_ID, checkId, checkName, rowNum, edcTable];
q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
if (!tableExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER", "ENCOUNTERID") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER", "ENC_TYPE")) {
  insertMetric(resultsTbl, base, "ALL", "STATUS", null, "ERROR", threshold, true, { message: "ENCOUNTER missing required columns" });
  return `DC 3.01 ERROR: ENCOUNTER missing`;
}
if (!tableExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS", "ENCOUNTERID") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS", "DX_TYPE")) {
  insertMetric(resultsTbl, base, "ALL", "STATUS", null, "ERROR", threshold, true, { message: "DIAGNOSIS missing required columns" });
  return `DC 3.01 ERROR: DIAGNOSIS missing`;
}
function normDateParam(x) { if (x === null || x === undefined) return null; var v = x.toString().trim(); var u = v.toUpperCase(); return (u === '''' || u === ''NONE'' || u === ''NULL'' || u === ''(NONE)'') ? null : v; }
const vStartDate = normDateParam(START_DATE);
const vEndDate = normDateParam(END_DATE);
const tableDateCol = {
  ENCOUNTER: ''ADMIT_DATE'',
  DIAGNOSIS: ''DX_DATE''
};
function dateFilterWhere(tbl) {
  const dc = tableDateCol[tbl] || null;
  if (!dc) return '''';
  let clause = '''';
  if (vStartDate) clause += ` AND TRY_TO_DATE(${dc}) >= TRY_TO_DATE(''${vStartDate}'')`;
  if (vEndDate) clause += ` AND TRY_TO_DATE(${dc}) <= TRY_TO_DATE(''${vEndDate}'')`;
  return clause;
}
const enc = `${DB_PARAM}.${SCHEMA_NAME}.ENCOUNTER`;
const dia = `${DB_PARAM}.${SCHEMA_NAME}.DIAGNOSIS`;
const rs = q(
  `
  WITH enc_base AS (
    SELECT ENCOUNTERID, ENC_TYPE
    FROM ${enc}
    WHERE ENCOUNTERID IS NOT NULL
      AND ENC_TYPE IN (''AV'',''IP'',''ED'',''EI'',''TH'')
      ${dateFilterWhere(''ENCOUNTER'')}
  ),
  enc_den AS (
    SELECT ENC_TYPE, COUNT(DISTINCT ENCOUNTERID) AS encounter_distinct_n
    FROM enc_base
    GROUP BY ENC_TYPE
  ),
  dia_num AS (
    SELECT e.ENC_TYPE, COUNT(*) AS diag_records_known_dxtype_n
    FROM ${dia} d
    JOIN enc_base e
      ON d.ENCOUNTERID = e.ENCOUNTERID
    WHERE d.ENCOUNTERID IS NOT NULL
      AND d.DX_TYPE IS NOT NULL
      AND TRIM(d.DX_TYPE) <> ''''
      AND UPPER(TRIM(d.DX_TYPE)) NOT IN (''NI'',''UN'',''OT'')
    GROUP BY e.ENC_TYPE
  ),
  types AS (SELECT column1 AS enc_type FROM VALUES (''AV''),(''IP''),(''ED''),(''EI''),(''TH''))
  SELECT
    t.enc_type,
    COALESCE(n.diag_records_known_dxtype_n, 0) AS diag_records_known_dxtype_n,
    COALESCE(d.encounter_distinct_n, 0) AS encounter_distinct_n,
    IFF(COALESCE(d.encounter_distinct_n,0) > 0,
        COALESCE(n.diag_records_known_dxtype_n,0)::FLOAT / COALESCE(d.encounter_distinct_n,0)::FLOAT,
        NULL
    ) AS avg_diag_per_encounter
  FROM types t
  LEFT JOIN enc_den d ON d.enc_type = t.enc_type
  LEFT JOIN dia_num n ON n.enc_type = t.enc_type
  ORDER BY t.enc_type
  `
);
while (rs.next()) {
  const encType = rs.getColumnValue(1);
  const num = Number(rs.getColumnValue(2));
  const den = Number(rs.getColumnValue(3));
  const avg = rs.getColumnValue(4) === null ? null : Number(rs.getColumnValue(4));
  const flag = (avg !== null && avg < threshold) ? 1 : 0;
  const details = {
    enc_type: encType,
    diag_records_known_dxtype_n: num,
    encounter_distinct_n: den,
    avg_diag_per_encounter: avg,
    threshold_avg_lt: threshold,
    numerator_filter: "DX_TYPE not in (NI,UN,OT,'''') and not null"
  };
  insertMetric(resultsTbl, base, encType, "DIAG_RECORDS_KNOWN_DXTYPE_N", num, String(num), threshold, false, details);
  insertMetric(resultsTbl, base, encType, "ENCOUNTER_DISTINCT_N", den, String(den), threshold, false, details);
  insertMetric(resultsTbl, base, encType, "AVG_DIAG_PER_ENCOUNTER", avg, avg === null ? "NULL" : String(avg), threshold, false, details);
  insertMetric(resultsTbl, base, encType, "AVG_BELOW_THRESHOLD_FLAG", flag, String(flag), threshold, (flag === 1), details);
}
return `DC 3.01 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';