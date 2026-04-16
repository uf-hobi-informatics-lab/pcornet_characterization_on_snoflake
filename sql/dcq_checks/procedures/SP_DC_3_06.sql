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
  const numStr = (valueNum === null || valueNum === undefined) ? null : String(valueNum);
  q(
    `INSERT INTO ${resultsTbl} (
      RUN_ID, CHECK_ID, CHECK_NAME, ROW_NUM, EDC_TABLE,
      SOURCE_TABLE, CODE_TYPE, METRIC, VALUE_NUM, VALUE_STR,
      THRESHOLD_NUM, EXCEPTION_FLAG, DETAILS
    )
    SELECT
      ?, ?, ?, ?, ?,
      ''DIAGNOSIS'', ?, ?, TRY_TO_NUMBER(?, 38, 10), ?,
      ?, IFF(?=1, TRUE, FALSE), PARSE_JSON(?)
    `,
    baseBinds.concat([codeType, metric, numStr, valueStr, thresholdNum, flagInt, detailsJson])
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
  insertMetric(resultsTbl, base, "ALL", "STATUS", null, "ERROR", thresholdPct, true, { message: "ENCOUNTER missing required columns" });
  return "DC 3.06 ERROR: ENCOUNTER missing";
}
if (!tableExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS", "ENCOUNTERID") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS", "DX_ORIGIN") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS", "PDX")) {
  insertMetric(resultsTbl, base, "ALL", "STATUS", null, "ERROR", thresholdPct, true, { message: "DIAGNOSIS missing required columns" });
  return "DC 3.06 ERROR: DIAGNOSIS missing";
}

function normDateParam(x) { if (x === null || x === undefined) return null; var v = x.toString().trim(); var u = v.toUpperCase(); return (u === '''' || u === ''NONE'' || u === ''NULL'' || u === ''(NONE)'') ? null : v; }
const vStartDate = normDateParam(START_DATE);
const vEndDate = normDateParam(END_DATE);
const tableDateCol = { ENCOUNTER: ''ADMIT_DATE'' };
function dateFilterWhere(tbl, alias) {
  const dc = tableDateCol[tbl] || null;
  if (!dc) return '''';
  const col = alias ? `${alias}.${dc}` : dc;
  let clause = '''';
  if (vStartDate) clause += ` AND TRY_TO_DATE(${col}) >= TRY_TO_DATE(''${vStartDate}'')`;
  if (vEndDate) clause += ` AND TRY_TO_DATE(${col}) <= TRY_TO_DATE(''${vEndDate}'')`;
  return clause;
}

const enc = `${DB_PARAM}.${SCHEMA_NAME}.ENCOUNTER`;
const dia = `${DB_PARAM}.${SCHEMA_NAME}.DIAGNOSIS`;

// ENC_TYPEs per spec: EI, IP, IS, OS
// DX_ORIGIN values: BI, CL, DR, OD
const rs = q(
  `WITH enc_base AS (
     SELECT ENCOUNTERID, UPPER(TRIM(ENC_TYPE)) AS enc_type
     FROM ${enc}
     WHERE ENCOUNTERID IS NOT NULL
       AND UPPER(TRIM(ENC_TYPE)) IN (''EI'',''IP'',''IS'',''OS'')${dateFilterWhere(''ENCOUNTER'')}
   ),
   dx_base AS (
     SELECT
       d.ENCOUNTERID,
       e.enc_type,
       UPPER(TRIM(d.DX_ORIGIN)) AS dx_origin,
       UPPER(TRIM(d.PDX)) AS pdx
     FROM ${dia} d
     JOIN enc_base e ON d.ENCOUNTERID = e.ENCOUNTERID
     WHERE d.ENCOUNTERID IS NOT NULL${dateFilterWhere(''DIAGNOSIS'', ''d'')}
   ),
   -- Cross join of enc_types and dx_origins to ensure all combos appear
   type_origin AS (
     SELECT t.enc_type, o.dx_origin
     FROM (SELECT column1 AS enc_type FROM VALUES (''EI''),(''IP''),(''IS''),(''OS'')) t,
          (SELECT column1 AS dx_origin FROM VALUES (''BI''),(''CL''),(''DR''),(''OD'')) o
   ),
   -- Per encounter+origin: does it have any PDX=P?
   enc_origin AS (
     SELECT
       enc_type,
       dx_origin,
       ENCOUNTERID,
       MAX(IFF(pdx = ''P'', 1, 0)) AS has_pdx,
       COUNT_IF(pdx = ''P'')         AS pdx_count
     FROM dx_base
     GROUP BY enc_type, dx_origin, ENCOUNTERID
   ),
   agg AS (
     SELECT
       enc_type,
       dx_origin,
       COUNT(*)                           AS enc_with_origin_n,
       COUNT_IF(has_pdx = 1)              AS enc_with_pdx_n,
       COUNT_IF(has_pdx = 0)              AS enc_without_pdx_n,
       SUM(pdx_count)                     AS pdx_total_n
     FROM enc_origin
     GROUP BY enc_type, dx_origin
   )
   SELECT
     t.enc_type,
     t.dx_origin,
     COALESCE(a.enc_with_pdx_n, 0)        AS enc_with_pdx_n,
     COALESCE(a.enc_without_pdx_n, 0)     AS enc_without_pdx_n,
     COALESCE(a.enc_with_origin_n, 0)     AS enc_with_origin_n,
     COALESCE(a.pdx_total_n, 0)           AS pdx_total_n
   FROM type_origin t
   LEFT JOIN agg a ON a.enc_type = t.enc_type AND a.dx_origin = t.dx_origin
   ORDER BY t.enc_type, t.dx_origin`
);

let wrote = 0;
while (rs.next()) {
  const encType       = rs.getColumnValue(1);
  const dxOrigin      = rs.getColumnValue(2);
  const encWithPdx    = Number(rs.getColumnValue(3));
  const encWithoutPdx = Number(rs.getColumnValue(4));
  const encTotal      = Number(rs.getColumnValue(5));
  const pdxTotalN     = Number(rs.getColumnValue(6));

  const pctWithoutPdx = (encTotal > 0) ? (encWithoutPdx / encTotal) * 100 : 0;
  const pdxPerEnc     = (encWithPdx > 0) ? pdxTotalN / encWithPdx : null;
  const flag          = (encTotal > 0 && pctWithoutPdx > thresholdPct) ? 1 : 0;

  const codeType = `${encType}:${dxOrigin}`;
  const details = {
    enc_type: encType,
    dx_origin: dxOrigin,
    enc_with_pdx_n: encWithPdx,
    enc_without_pdx_n: encWithoutPdx,
    pct_without_pdx: pctWithoutPdx,
    pdx_total_n: pdxTotalN,
    pdx_per_enc_with_pdx: pdxPerEnc,
    threshold_pct_gt: thresholdPct,
    source_tables: ["DIA_L3_PDX_ENCTYPE","DIA_L3_PDXGRP_ENCTYPE"]
  };

  insertMetric(resultsTbl, base, codeType, "ENC_WITH_PDX_N",          encWithPdx,    String(encWithPdx),    thresholdPct, false, details);
  insertMetric(resultsTbl, base, codeType, "ENC_WITHOUT_PDX_N",       encWithoutPdx, String(encWithoutPdx), thresholdPct, false, details);
  insertMetric(resultsTbl, base, codeType, "PCT_WITHOUT_PDX",         pctWithoutPdx, String(pctWithoutPdx), thresholdPct, false, details);
  insertMetric(resultsTbl, base, codeType, "PDX_TOTAL_N",             pdxTotalN,     String(pdxTotalN),     thresholdPct, false, details);
  insertMetric(resultsTbl, base, codeType, "PDX_PER_ENC_WITH_PDX",    pdxPerEnc,     pdxPerEnc === null ? null : String(pdxPerEnc), thresholdPct, false, details);
  insertMetric(resultsTbl, base, codeType, "PCT_WITHOUT_PDX_FLAG",    flag,          String(flag),          thresholdPct, (flag === 1), details);
  wrote += 1;
}

insertMetric(resultsTbl, base, "ALL", "STATUS", null, "OK", thresholdPct, false, { combinations_evaluated: wrote });
return `DC 3.06 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} combos=${wrote}`;
';
