CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_04"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
function insertMetric(resultsTbl, baseBinds, metric, valueNum, valueStr, thresholdNum, exceptionFlag, detailsObj) {
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
      ''DIAGNOSIS'', ''PATID_COVERAGE'', ?, ?, ?,
      ?, IFF(?=1, TRUE, FALSE), PARSE_JSON(?)
    `,
    baseBinds.concat([metric, valueNum, valueStr, thresholdNum, flagInt, detailsJson])
  );
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 3.04;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
const thresholdPct = 50;
if (!(only === "ALL" || only === "ENCOUNTER" || only === "DIAGNOSIS")) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, ENCOUNTER, or DIAGNOSIS.`);
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
// idempotent delete for this run/check
q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
if (!tableExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER") || !colExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER", "PATID")) {
  insertMetric(resultsTbl, base, "STATUS", null, "ERROR", thresholdPct, true, { message: "ENCOUNTER or ENCOUNTER.PATID missing" });
  return `DC 3.04 ERROR: ENCOUNTER missing`;
}
if (!tableExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS") || !colExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS", "PATID")) {
  insertMetric(resultsTbl, base, "STATUS", null, "ERROR", thresholdPct, true, { message: "DIAGNOSIS or DIAGNOSIS.PATID missing" });
  return `DC 3.04 ERROR: DIAGNOSIS missing`;
}
const vStartDate = (START_DATE || '''').toString().trim() || null;
const vEndDate = (END_DATE || '''').toString().trim() || null;
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
  WITH e AS (
    SELECT DISTINCT PATID
    FROM ${enc}
    WHERE PATID IS NOT NULL
      ${dateFilterWhere(''ENCOUNTER'')}
  ),
  d AS (
    SELECT DISTINCT PATID
    FROM ${dia}
    WHERE PATID IS NOT NULL
      ${dateFilterWhere(''DIAGNOSIS'')}
  )
  SELECT
    (SELECT COUNT(*) FROM e) AS encounter_patid_distinct_n,
    (SELECT COUNT(*) FROM d) AS diagnosis_patid_distinct_n
  `
);
rs.next();
const denom = Number(rs.getColumnValue(1));
const numer = Number(rs.getColumnValue(2));
const pct = (denom > 0) ? (numer / denom) * 100 : 0;
const flag = (denom > 0 && pct < thresholdPct) ? 1 : 0;
const details = {
  encounter_patid_distinct_n: denom,
  diagnosis_patid_distinct_n: numer,
  diagnosis_coverage_pct: pct,
  threshold_pct_lt: thresholdPct
};
insertMetric(resultsTbl, base, "ENCOUNTER_PATID_DISTINCT_N", denom, String(denom), thresholdPct, false, details);
insertMetric(resultsTbl, base, "DIAGNOSIS_PATID_DISTINCT_N", numer, String(numer), thresholdPct, false, details);
insertMetric(resultsTbl, base, "DIAGNOSIS_COVERAGE_PCT", pct, String(pct), thresholdPct, false, details);
insertMetric(resultsTbl, base, "DIAGNOSIS_COVERAGE_FLAG", flag, String(flag), thresholdPct, (flag === 1), details);
return `DC 3.04 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';