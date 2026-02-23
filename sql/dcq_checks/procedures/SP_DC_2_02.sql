CREATE OR REPLACE PROCEDURE "SP_DC_2_02"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR)
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
function insertMetric(resultsTbl, baseBinds, sourceTable, codeType, metric, valueNum, valueStr, thresholdNum, exceptionFlag, detailsObj) {
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
      ?, IFF(?=1, TRUE, FALSE), PARSE_JSON(?)
    `,
    baseBinds.concat([sourceTable, codeType, metric, valueNum, valueStr, thresholdNum, flagInt, detailsJson])
  );
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 2.02;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
const thresholdPct = 10;
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
// delete prior rows for this run/check (scoped if TARGET_TABLE provided)
if (only === "ALL") {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
} else {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ? AND UPPER(SOURCE_TABLE)=?`, [RUN_ID, rowNum, only]);
}
function emitExtreme(sourceTable, field, denom, lowN, highN, details) {
  const extremeN = lowN + highN;
  const pct = (denom > 0) ? (extremeN / denom) * 100 : 0;
  const flag = (pct > thresholdPct) ? 1 : 0;
  insertMetric(resultsTbl, base, sourceTable, field, "RECORDS_EVAL_N", denom, String(denom), thresholdPct, false, details);
  insertMetric(resultsTbl, base, sourceTable, field, "LOW_EXTREME_N", lowN, String(lowN), thresholdPct, false, details);
  insertMetric(resultsTbl, base, sourceTable, field, "HIGH_EXTREME_N", highN, String(highN), thresholdPct, false, details);
  insertMetric(resultsTbl, base, sourceTable, field, "EXTREME_N", extremeN, String(extremeN), thresholdPct, false, details);
  insertMetric(resultsTbl, base, sourceTable, field, "EXTREME_PCT", pct, String(pct), thresholdPct, false, details);
  insertMetric(resultsTbl, base, sourceTable, field, "EXTREME_FLAG", flag, String(flag), thresholdPct, (flag === 1), details);
}
// Checks configuration
const checks = [
  { table: "VITAL", field: "HT", low: 21, high: 76, lowOp: "<", highOp: ">" },
  { table: "VITAL", field: "WT", low: 0, high: 350, lowOp: "<", highOp: ">" },
  { table: "VITAL", field: "DIASTOLIC", low: 40, high: 120, lowOp: "<", highOp: ">" },
  { table: "VITAL", field: "SYSTOLIC", low: 40, high: 210, lowOp: "<", highOp: ">" },
  { table: "VITAL", field: "BMI", low: 0, high: 100, lowOp: "<", highOp: ">" },  // adjust if you want
  { table: "DISPENSING", field: "DISPENSE_SUP", low: 1, high: 90, lowOp: "<", highOp: ">" }
];
let selected = checks;
if (only !== "ALL") {
  selected = checks.filter(c => c.table === only);
  if (selected.length === 0) {
    throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, VITAL, or DISPENSING.`);
  }
}
for (const c of selected) {
  if (!tableExists(DB_PARAM, SCHEMA_NAME, c.table)) continue;
  if (!colExists(DB_PARAM, SCHEMA_NAME, c.table, c.field)) continue;
  const fullTable = `${DB_PARAM}.${SCHEMA_NAME}.${c.table}`;
  const sql = `
    WITH v AS (
      SELECT TRY_TO_DOUBLE(${c.field}) AS x
      FROM ${fullTable}
      WHERE TRY_TO_DOUBLE(${c.field}) IS NOT NULL
    )
    SELECT
      COUNT(*) AS denom,
      COUNT_IF(x ${c.lowOp} ${c.low}) AS low_n,
      COUNT_IF(x ${c.highOp} ${c.high}) AS high_n
    FROM v
  `;
  const rs = q(sql);
  rs.next();
  const denom = Number(rs.getColumnValue(1));
  const lowN = Number(rs.getColumnValue(2));
  const highN = Number(rs.getColumnValue(3));
  const details = {
    table: c.table,
    field: c.field,
    low_rule: `x ${c.lowOp} ${c.low}`,
    high_rule: `x ${c.highOp} ${c.high}`,
    threshold_pct: thresholdPct
  };
  emitExtreme(c.table, c.field, denom, lowN, highN, details);
}
return `DC 2.02 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';