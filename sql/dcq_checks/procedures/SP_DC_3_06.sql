CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_06"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
const rowNum = 3.06;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
const thresholdPct = 10;
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
// prerequisites
if (!tableExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER", "ENCOUNTERID") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER", "ENC_TYPE")) {
  insertMetric(resultsTbl, base, "ALL", "STATUS", null, "ERROR", thresholdPct, true, { message: "ENCOUNTER missing required columns" });
  return `DC 3.06 ERROR: ENCOUNTER missing`;
}
if (!tableExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS", "ENCOUNTERID") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS", "DX_ORIGIN") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS", "PDX")) {
  insertMetric(resultsTbl, base, "ALL", "STATUS", null, "ERROR", thresholdPct, true, { message: "DIAGNOSIS missing required columns" });
  return `DC 3.06 ERROR: DIAGNOSIS missing`;
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
  WITH enc_ip_ei AS (
    SELECT ENCOUNTERID, ENC_TYPE
    FROM ${enc}
    WHERE ENCOUNTERID IS NOT NULL
      AND ENC_TYPE IN (''IP'',''EI'')
      ${dateFilterWhere(''ENCOUNTER'')}
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
      AND UPPER(TRIM(d.DX_ORIGIN)) NOT IN (''NI'',''UN'',''OT'')
  ),
  per_enc_origin AS (
    SELECT
      encounterid,
      dx_origin,
      MAX(IFF(pdx = ''P'', 1, 0)) AS has_pdx_for_origin
    FROM diag_known
    GROUP BY encounterid, dx_origin
  ),
  agg AS (
    SELECT
      dx_origin,
      COUNT(*) AS encounters_with_known_origin_n,
      SUM(IFF(has_pdx_for_origin=0, 1, 0)) AS encounters_missing_pdx_for_origin_n
    FROM per_enc_origin
    GROUP BY dx_origin
  )
  SELECT
    dx_origin,
    encounters_with_known_origin_n,
    encounters_missing_pdx_for_origin_n,
    IFF(encounters_with_known_origin_n > 0,
        (encounters_missing_pdx_for_origin_n::FLOAT / encounters_with_known_origin_n::FLOAT) * 100,
        0
    ) AS missing_pdx_for_origin_pct
  FROM agg
  ORDER BY dx_origin
  `
);
while (rs.next()) {
  const origin = rs.getColumnValue(1);
  const denom = Number(rs.getColumnValue(2));
  const numer = Number(rs.getColumnValue(3));
  const pct = Number(rs.getColumnValue(4));
  const flag = (denom > 0 && pct > thresholdPct) ? 1 : 0;
  const details = {
    enc_types: ["IP","EI"],
    dx_origin: origin,
    encounters_with_known_origin_n: denom,
    encounters_missing_pdx_for_origin_n: numer,
    missing_pdx_for_origin_pct: pct,
    threshold_pct_gt: thresholdPct,
    definition: "Encounter+DX_ORIGIN has no DIAGNOSIS row with PDX=''P''"
  };
  insertMetric(resultsTbl, base, origin, "ENCOUNTERS_WITH_KNOWN_ORIGIN_N", denom, String(denom), thresholdPct, false, details);
  insertMetric(resultsTbl, base, origin, "ENCOUNTERS_MISSING_PDX_FOR_ORIGIN_N", numer, String(numer), thresholdPct, false, details);
  insertMetric(resultsTbl, base, origin, "MISSING_PDX_FOR_ORIGIN_PCT", pct, String(pct), thresholdPct, false, details);
  insertMetric(resultsTbl, base, origin, "MISSING_PDX_FOR_ORIGIN_FLAG", flag, String(flag), thresholdPct, (flag === 1), details);
}
return `DC 3.06 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';