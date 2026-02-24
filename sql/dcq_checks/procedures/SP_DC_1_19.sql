CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_19"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR)
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
function insertMetric(resultsTbl, baseBinds, codeType, metric, valueNum, valueStr, exceptionFlag, detailsObj) {
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
      ''HASH_TOKEN'', ?, ?, ?, ?,
      10,
      IFF(?=1, TRUE, FALSE),
      PARSE_JSON(?)
    `,
    baseBinds.concat([codeType, metric, valueNum, valueStr, flagInt, detailsJson])
  );
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 1.19;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
if (!(only === "ALL" || only === "HASH_TOKEN")) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or HASH_TOKEN.`);
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
q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
if (!tableExists(DB_PARAM, SCHEMA_NAME, "HASH_TOKEN")) {
  const base = [RUN_ID, checkId, checkName, rowNum, edcTable];
  insertMetric(resultsTbl, base, "HASH_TOKEN", "STATUS", null, "ERROR", true, { message: "HASH_TOKEN table does not exist" });
  return `DC 1.19 ERROR: HASH_TOKEN missing`;
}
// Discover token columns (TOKEN_*)
const colsRs = q(
  `SELECT COLUMN_NAME
   FROM ${DB_PARAM}.INFORMATION_SCHEMA.COLUMNS
   WHERE TABLE_SCHEMA = ?
     AND TABLE_NAME = ''HASH_TOKEN''
     AND STARTSWITH(UPPER(COLUMN_NAME), ''TOKEN_'')
     AND UPPER(COLUMN_NAME) <> ''TOKEN_ENCRYPTION_KEY''
   ORDER BY COLUMN_NAME`,
  [SCHEMA_NAME.toUpperCase()]
);
const tokenCols = [];
while (colsRs.next()) tokenCols.push(colsRs.getColumnValue(1));
const base = [RUN_ID, checkId, checkName, rowNum, edcTable];
// Diagnostic row so we always get output
insertMetric(
  resultsTbl,
  base,
  "HASH_TOKEN",
  "TOKEN_FIELD_COUNT",
  tokenCols.length,
  String(tokenCols.length),
  false,
  { schema: SCHEMA_NAME, token_field_count: tokenCols.length }
);
if (tokenCols.length === 0) {
  insertMetric(
    resultsTbl,
    base,
    "HASH_TOKEN",
    "STATUS",
    null,
    "ERROR",
    true,
    { message: "No TOKEN_* columns discovered in INFORMATION_SCHEMA.COLUMNS for HASH_TOKEN" }
  );
  return `DC 1.19 ERROR: no token columns discovered`;
}
const fullHash = `${DB_PARAM}.${SCHEMA_NAME}.HASH_TOKEN`;
// Valid token rule: non-null, non-empty, not in common error codes
const invalidList = ["", "NI", "UN", "OT", "NA"];
function invalidPredicate(col) {
  const list = invalidList.map(v => `''${v.replace("''", "''''")}''`).join(",");
  return `(
    UPPER(TRIM(${col})) IN (${list})
    OR STARTSWITH(UPPER(TRIM(${col})), ''XXX'')
  )`;
}
for (const c of tokenCols) {
  const sql = `
    WITH base_rows AS (
      SELECT PATID, ${c} AS token_val
      FROM ${fullHash}
      WHERE ${c} IS NOT NULL
        AND NOT (${invalidPredicate(c)})
        AND PATID IS NOT NULL
    ),
    per_token AS (
      SELECT token_val,
             COUNT(DISTINCT PATID) AS patid_n
      FROM base_rows
      GROUP BY token_val
    ),
    agg AS (
      SELECT
        (SELECT COUNT(*) FROM base_rows) AS valid_n,
        (SELECT COUNT(DISTINCT token_val) FROM base_rows) AS valid_distinct_n,
        SUM(IFF(patid_n > 1, 1, 0)) AS valid_multi_patid_token_n
      FROM per_token
    )
    SELECT
      valid_n,
      valid_distinct_n,
      valid_multi_patid_token_n,
      IFF(valid_distinct_n > 0,
          (valid_multi_patid_token_n::FLOAT / valid_distinct_n::FLOAT) * 100,
          0
      ) AS valid_multi_patid_token_pct
    FROM agg
  `;
  const rs = q(sql);
  rs.next();
  const validN = Number(rs.getColumnValue(1));
  const validDistinct = Number(rs.getColumnValue(2));
  const multiTokenN = Number(rs.getColumnValue(3));
  const pct = Number(rs.getColumnValue(4));
  const flag = (pct > 10) ? 1 : 0;
  const details = {
    token_field: c,
    valid_n: validN,
    valid_distinct_n: validDistinct,
    valid_multi_patid_token_n: multiTokenN,
    valid_multi_patid_token_pct: pct,
    threshold_pct: 10
  };
  insertMetric(resultsTbl, base, c, "VALID_N", validN, String(validN), false, details);
  insertMetric(resultsTbl, base, c, "VALID_DISTINCT_N", validDistinct, String(validDistinct), false, details);
  insertMetric(resultsTbl, base, c, "VALID_MULTI_PATID_TOKEN_N", multiTokenN, String(multiTokenN), false, details);
  insertMetric(resultsTbl, base, c, "VALID_MULTI_PATID_TOKEN_PCT", pct, String(pct), false, details);
  insertMetric(resultsTbl, base, c, "TOKEN_MULTI_PATID_FLAG", flag, String(flag), (flag === 1), details);
}
return `DC 1.19 finished RUN_ID=${RUN_ID} token_fields=${tokenCols.length}`;
';