CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_11"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
function insertMetric(resultsTbl, baseBinds, metric, valueNum, valueStr, exceptionFlag, detailsObj) {
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
      ''ENCOUNTER'', NULL, ?, ?, ?,
      5,
      IFF(?=1, TRUE, FALSE),
      PARSE_JSON(?)
    `,
    baseBinds.concat([metric, valueNum, valueStr, flagInt, detailsJson])
  );
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 1.11;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
if (!(only === "ALL" || only === "ENCOUNTER")) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or ENCOUNTER.`);
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
// delete prior rows for this run/check
q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
if (!tableExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER")) {
  const base = [RUN_ID, checkId, checkName, rowNum, edcTable];
  insertMetric(resultsTbl, base, "STATUS", null, "ERROR", true, { message: "ENCOUNTER table does not exist" });
  return `DC 1.11 ERROR: ENCOUNTER missing`;
}
const fullEnc = `${DB_PARAM}.${SCHEMA_NAME}.ENCOUNTER`;
const rs = q(
  `
  WITH per_enc AS (
    SELECT
      ENCOUNTERID,
      COUNT(DISTINCT PATID) AS patid_n
    FROM ${fullEnc}
    WHERE ENCOUNTERID IS NOT NULL
    GROUP BY ENCOUNTERID
  ),
  agg AS (
    SELECT
      COUNT(*) AS encounterid_distinct_n,
      SUM(IFF(patid_n > 1, 1, 0)) AS multi_patid_encounterid_n
    FROM per_enc
  )
  SELECT
    encounterid_distinct_n,
    multi_patid_encounterid_n,
    IFF(encounterid_distinct_n > 0,
        (multi_patid_encounterid_n::FLOAT / encounterid_distinct_n::FLOAT) * 100,
        0
    ) AS multi_patid_encounterid_pct
  FROM agg
  `
);
rs.next();
const denom = Number(rs.getColumnValue(1));
const numer = Number(rs.getColumnValue(2));
const pct = Number(rs.getColumnValue(3));
const flag = (pct > 5) ? 1 : 0;
const details = {
  encounterid_distinct_n: denom,
  multi_patid_encounterid_n: numer,
  multi_patid_encounterid_pct: pct,
  threshold_pct: 5
};
const base = [RUN_ID, checkId, checkName, rowNum, edcTable];
insertMetric(resultsTbl, base, "ENCOUNTERID_DISTINCT_N", denom, String(denom), false, details);
insertMetric(resultsTbl, base, "MULTI_PATID_ENCOUNTERID_N", numer, String(numer), false, details);
insertMetric(resultsTbl, base, "MULTI_PATID_ENCOUNTERID_PCT", pct, String(pct), false, details);
insertMetric(resultsTbl, base, "MULTI_PATID_ENCOUNTERID_FLAG", flag, String(flag), (flag === 1), details);
return `DC 1.11 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';