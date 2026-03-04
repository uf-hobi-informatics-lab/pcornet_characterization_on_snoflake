CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_08("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
function q(sqlText, binds) { return snowflake.execute({ sqlText, binds: binds || [] }); }
function isSafeIdentPart(s) { return /^[A-Za-z0-9_$]+$/.test((s || '''').toString()); }
function scalar(sqlText, binds) { const rs = q(sqlText, binds || []); rs.next(); return rs.getColumnValue(1); }
function quoteIdent(name) {
  const dq = String.fromCharCode(34);
  return dq + (name || "").toString().replace(/"/g, dq + dq) + dq;
}
function resolveColumn(db, schema, table, colName) {
  const rs = q(
    `SELECT COLUMN_NAME
     FROM ${db}.INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = ?
       AND TABLE_NAME = ?
       AND UPPER(COLUMN_NAME) = UPPER(?)
     QUALIFY ROW_NUMBER() OVER (ORDER BY COLUMN_NAME) = 1`,
    [schema.toUpperCase(), table.toUpperCase(), colName]
  );
  if (!rs.next()) return null;
  return rs.getColumnValue(1);
}
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
      ?, ?, ?, ?, ?,
      0,
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
const rowNum = 1.08;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
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
// delete prior rows for this run/check (scoped if TARGET_TABLE provided)
if (only === "ALL") {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
} else {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ? AND UPPER(SOURCE_TABLE)=?`, [RUN_ID, rowNum, only]);
}
// DEMOGRAPHIC must exist
if (!tableExists(DB_PARAM, SCHEMA_NAME, "DEMOGRAPHIC")) {
  const base = [RUN_ID, checkId, checkName, rowNum, edcTable, "DEMOGRAPHIC"];
  insertMetric(
    resultsTbl,
    base,
    "PATID",
    "STATUS",
    null,
    "ERROR",
    true,
    { message: "DEMOGRAPHIC table does not exist; cannot evaluate orphan PATIDs." }
  );
  return `DC 1.08 ERROR: DEMOGRAPHIC missing`;
}

const demoPatidColName = resolveColumn(DB_PARAM, SCHEMA_NAME, "DEMOGRAPHIC", "PATID");
if (!demoPatidColName) {
  const base = [RUN_ID, checkId, checkName, rowNum, edcTable, "DEMOGRAPHIC"];
  insertMetric(
    resultsTbl,
    base,
    "PATID",
    "STATUS",
    null,
    "ERROR",
    true,
    { message: "DEMOGRAPHIC.PATID column does not exist; cannot evaluate orphan PATIDs." }
  );
  return `DC 1.08 ERROR: DEMOGRAPHIC.PATID missing`;
}
const demoPatidRef = quoteIdent(demoPatidColName);
// Tables/columns to check
const targets = [
  { table: "ENROLLMENT", cols: ["PATID"] },
  { table: "ENCOUNTER", cols: ["PATID"] },
  { table: "DIAGNOSIS", cols: ["PATID"] },
  { table: "PROCEDURES", cols: ["PATID"] },
  { table: "VITAL", cols: ["PATID"] },
  { table: "DISPENSING", cols: ["PATID"] },
  { table: "PRESCRIBING", cols: ["PATID"] },
  { table: "LAB_RESULT_CM", cols: ["PATID"] },
  { table: "CONDITION", cols: ["PATID"] },
  { table: "PRO_CM", cols: ["PATID"] },
  { table: "PCORNET_TRIAL", cols: ["PATID"] },
  { table: "DEATH", cols: ["PATID"] },
  { table: "DEATH_CAUSE", cols: ["PATID"] },
  { table: "MED_ADMIN", cols: ["PATID"] },
  { table: "OBS_CLIN", cols: ["PATID"] },
  { table: "OBS_GEN", cols: ["PATID"] },
  { table: "HASH_TOKEN", cols: ["PATID"] },
  { table: "LDS_ADDRESS_HISTORY", cols: ["PATID"] },
  { table: "IMMUNIZATION", cols: ["PATID"] },
  { table: "LAB_HISTORY", cols: ["PATID"] },
  { table: "EXTERNAL_MEDS", cols: ["PATID"] },
  { table: "PAT_RELATIONSHIP", cols: ["PATID_1","PATID_2"] }
];
let selected = targets;
if (only !== "ALL") {
  selected = targets.filter(t => t.table === only);
  if (selected.length === 0) {
    throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or a CDM table with PATID (e.g., DIAGNOSIS, ENCOUNTER, PAT_RELATIONSHIP).`);
  }
}
for (const t of selected) {
  const exists = tableExists(DB_PARAM, SCHEMA_NAME, t.table);
  if (!exists) {
    const base = [RUN_ID, checkId, checkName, rowNum, edcTable, t.table];
    for (const c of t.cols) {
      insertMetric(resultsTbl, base, c, "STATUS", null, "SKIPPED", false, { message: "Table does not exist" });
    }
    continue;
  }
  for (const c of t.cols) {
    const fullTable = `${DB_PARAM}.${SCHEMA_NAME}.${t.table}`;
    const colName = resolveColumn(DB_PARAM, SCHEMA_NAME, t.table, c);
    if (!colName) {
      const base = [RUN_ID, checkId, checkName, rowNum, edcTable, t.table];
      insertMetric(
        resultsTbl,
        base,
        c,
        "STATUS",
        null,
        "SKIPPED",
        false,
        { message: `Column ${c} does not exist` }
      );
      continue;
    }
    const colRef = quoteIdent(colName);
    // Use anti-join to DEMOGRAPHIC on PATID
    const sql = `
      WITH demo AS (
        SELECT DISTINCT ${demoPatidRef} AS PATID
        FROM ${DB_PARAM}.${SCHEMA_NAME}.DEMOGRAPHIC
        WHERE ${demoPatidRef} IS NOT NULL
      ),
      base AS (
        SELECT DISTINCT ${colRef} AS pid
        FROM ${fullTable}
        WHERE ${colRef} IS NOT NULL
      ),
      orphan AS (
        SELECT b.pid
        FROM base b
        LEFT JOIN demo d
          ON b.pid = d.PATID
        WHERE d.PATID IS NULL
      )
      SELECT
        (SELECT COUNT(*) FROM base) AS table_patid_distinct_n,
        (SELECT COUNT(*) FROM orphan) AS orphan_patid_distinct_n
    `;
    const rs = q(sql);
    rs.next();
    const tableDistinct = Number(rs.getColumnValue(1));
    const orphanDistinct = Number(rs.getColumnValue(2));
    const orphanPct = (tableDistinct > 0) ? (orphanDistinct / tableDistinct) * 100 : 0;
    const orphanFlag = orphanDistinct > 0 ? 1 : 0;
    const details = {
      patid_field: c,
      table_patid_distinct_n: tableDistinct,
      orphan_patid_distinct_n: orphanDistinct,
      orphan_patid_pct: orphanPct
    };
    const baseBinds = [RUN_ID, checkId, checkName, rowNum, edcTable, t.table];
    insertMetric(resultsTbl, baseBinds, c, "TABLE_PATID_DISTINCT_N", tableDistinct, String(tableDistinct), false, details);
    insertMetric(resultsTbl, baseBinds, c, "ORPHAN_PATID_DISTINCT_N", orphanDistinct, String(orphanDistinct), false, details);
    insertMetric(resultsTbl, baseBinds, c, "ORPHAN_PATID_PCT", orphanPct, String(orphanPct), false, details);
    insertMetric(resultsTbl, baseBinds, c, "ORPHAN_PATID_FLAG", orphanFlag, String(orphanFlag), (orphanFlag === 1), details);
  }
}
return `DC 1.08 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';
