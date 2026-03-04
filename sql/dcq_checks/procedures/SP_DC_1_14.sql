CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_14"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
      ''HASH_TOKEN'', NULL, ?, ?, ?,
      0,
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
const rowNum = 1.14;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
if (!(only === "ALL" || only === "DEMOGRAPHIC" || only === "HASH_TOKEN")) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, DEMOGRAPHIC, or HASH_TOKEN.`);
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
if (!tableExists(DB_PARAM, SCHEMA_NAME, "DEMOGRAPHIC")) {
  const base = [RUN_ID, checkId, checkName, rowNum, edcTable];
  insertMetric(resultsTbl, base, "STATUS", null, "ERROR", true, { message: "DEMOGRAPHIC table does not exist" });
  return `DC 1.14 ERROR: DEMOGRAPHIC missing`;
}
if (!tableExists(DB_PARAM, SCHEMA_NAME, "HASH_TOKEN")) {
  const base = [RUN_ID, checkId, checkName, rowNum, edcTable];
  insertMetric(resultsTbl, base, "STATUS", null, "ERROR", true, { message: "HASH_TOKEN table does not exist" });
  return `DC 1.14 ERROR: HASH_TOKEN missing`;
}
const fullDemo = `${DB_PARAM}.${SCHEMA_NAME}.DEMOGRAPHIC`;
const fullHash = `${DB_PARAM}.${SCHEMA_NAME}.HASH_TOKEN`;
const rs = q(
  `
  WITH demo AS (
    SELECT DISTINCT PATID
    FROM ${fullDemo}
    WHERE PATID IS NOT NULL
  ),
  ht AS (
    SELECT DISTINCT PATID
    FROM ${fullHash}
    WHERE PATID IS NOT NULL
  ),
  missing AS (
    SELECT d.PATID
    FROM demo d
    LEFT JOIN ht h
      ON d.PATID = h.PATID
    WHERE h.PATID IS NULL
  )
  SELECT
    (SELECT COUNT(*) FROM demo) AS demographic_patid_distinct_n,
    (SELECT COUNT(*) FROM ht) AS hash_token_patid_distinct_n,
    (SELECT COUNT(*) FROM missing) AS missing_hash_token_patid_distinct_n
  `
);
rs.next();
const demoN = Number(rs.getColumnValue(1));
const hashN = Number(rs.getColumnValue(2));
const missN = Number(rs.getColumnValue(3));
const missPct = (demoN > 0) ? (missN / demoN) * 100 : 0;
const flag = (missN > 0) ? 1 : 0;
const details = {
  demographic_patid_distinct_n: demoN,
  hash_token_patid_distinct_n: hashN,
  missing_hash_token_patid_distinct_n: missN,
  missing_hash_token_patid_pct: missPct
};
const base = [RUN_ID, checkId, checkName, rowNum, edcTable];
insertMetric(resultsTbl, base, "DEMOGRAPHIC_PATID_DISTINCT_N", demoN, String(demoN), false, details);
insertMetric(resultsTbl, base, "HASH_TOKEN_PATID_DISTINCT_N", hashN, String(hashN), false, details);
insertMetric(resultsTbl, base, "MISSING_HASH_TOKEN_PATID_DISTINCT_N", missN, String(missN), false, details);
insertMetric(resultsTbl, base, "MISSING_HASH_TOKEN_PATID_PCT", missPct, String(missPct), false, details);
insertMetric(resultsTbl, base, "MISSING_HASH_TOKEN_PATID_FLAG", flag, String(flag), (flag === 1), details);
return `DC 1.14 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';