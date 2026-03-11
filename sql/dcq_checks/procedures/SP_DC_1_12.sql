CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_12"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
function q(sqlText, binds) { return snowflake.execute({ sqlText, binds: binds || [] }); }
function isSafeIdentPart(s) { return /^[A-Za-z0-9_$]+$/.test((s || '''').toString()); }
function scalar(sqlText, binds) { const rs = q(sqlText, binds || []); rs.next(); return rs.getColumnValue(1); }
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
function normDateParam(x) { if (x === null || x === undefined) return null; var v = x.toString().trim(); var u = v.toUpperCase(); return (u === '''' || u === ''NONE'' || u === ''NULL'' || u === ''(NONE)'') ? null : v; }
const vStartDate = normDateParam(START_DATE);
const vEndDate = normDateParam(END_DATE);
const tableDateCol = {
  DIAGNOSIS: ''DX_DATE'',
  ENCOUNTER: ''ADMIT_DATE'',
  IMMUNIZATION: ''VX_RECORD_DATE'',
  MED_ADMIN: ''MEDADMIN_START_DATE'',
  OBS_CLIN: ''OBSCLIN_START_DATE'',
  OBS_GEN: ''OBSGEN_START_DATE'',
  PRESCRIBING: ''RX_ORDER_DATE'',
  PROCEDURES: ''PX_DATE''
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
const rowNum = 1.12;
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
// PROVIDER must exist
if (!tableExists(DB_PARAM, SCHEMA_NAME, "PROVIDER")) {
  const base = [RUN_ID, checkId, checkName, rowNum, edcTable, "PROVIDER"];
  insertMetric(
    resultsTbl,
    base,
    "PROVIDERID",
    "STATUS",
    null,
    "ERROR",
    true,
    { message: "PROVIDER table does not exist; cannot evaluate orphan PROVIDERIDs." }
  );
  return `DC 1.12 ERROR: PROVIDER missing`;
}
// Standard provider-id fields by table (with safe skips if column missing)
const targets = [
  { table: "ENCOUNTER", cols: ["PROVIDERID"] },
  { table: "DIAGNOSIS", cols: ["PROVIDERID"] },
  { table: "PROCEDURES", cols: ["PROVIDERID"] },
  { table: "PRESCRIBING", cols: ["RX_PROVIDERID"] },
  { table: "IMMUNIZATION", cols: ["VX_PROVIDERID"] },
  { table: "MED_ADMIN", cols: ["MEDADMIN_PROVIDERID","PROVIDERID"] },
  { table: "OBS_CLIN", cols: ["OBSCLIN_PROVIDERID","PROVIDERID"] },
  { table: "OBS_GEN", cols: ["OBSGEN_PROVIDERID","PROVIDERID"] }
];
let selected = targets;
if (only !== "ALL") {
  selected = targets.filter(t => t.table === only);
  if (selected.length === 0) {
    throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or one of: ${targets.map(x => x.table).join('', '')}`);
  }
}
for (const t of selected) {
  const exists = tableExists(DB_PARAM, SCHEMA_NAME, t.table);
  const base = [RUN_ID, checkId, checkName, rowNum, edcTable, t.table];
  if (!exists) {
    for (const c of t.cols) {
      insertMetric(resultsTbl, base, c, "STATUS", null, "SKIPPED", false, { message: "Table does not exist" });
    }
    continue;
  }
  for (const c of t.cols) {
    if (!colExists(DB_PARAM, SCHEMA_NAME, t.table, c)) {
      insertMetric(resultsTbl, base, c, "STATUS", null, "SKIPPED", false, { message: "Column does not exist" });
      continue;
    }
    const fullTable = `${DB_PARAM}.${SCHEMA_NAME}.${t.table}`;
    const sql = `
      WITH prov AS (
        SELECT DISTINCT PROVIDERID
        FROM ${DB_PARAM}.${SCHEMA_NAME}.PROVIDER
        WHERE PROVIDERID IS NOT NULL
      ),
      base_ids AS (
        SELECT DISTINCT ${c} AS pid
        FROM ${fullTable}
        WHERE ${c} IS NOT NULL ${dateFilterWhere(t.table)}
      ),
      orphan AS (
        SELECT b.pid
        FROM base_ids b
        LEFT JOIN prov p
          ON b.pid = p.PROVIDERID
        WHERE p.PROVIDERID IS NULL
      )
      SELECT
        (SELECT COUNT(*) FROM base_ids) AS table_providerid_distinct_n,
        (SELECT COUNT(*) FROM orphan) AS orphan_providerid_distinct_n
    `;
    const rs = q(sql);
    rs.next();
    const tableDistinct = Number(rs.getColumnValue(1));
    const orphanDistinct = Number(rs.getColumnValue(2));
    const orphanPct = (tableDistinct > 0) ? (orphanDistinct / tableDistinct) * 100 : 0;
    const orphanFlag = orphanDistinct > 0 ? 1 : 0;
    const details = {
      providerid_field: c,
      table_providerid_distinct_n: tableDistinct,
      orphan_providerid_distinct_n: orphanDistinct,
      orphan_providerid_pct: orphanPct
    };
    insertMetric(resultsTbl, base, c, "TABLE_PROVIDERID_DISTINCT_N", tableDistinct, String(tableDistinct), false, details);
    insertMetric(resultsTbl, base, c, "ORPHAN_PROVIDERID_DISTINCT_N", orphanDistinct, String(orphanDistinct), false, details);
    insertMetric(resultsTbl, base, c, "ORPHAN_PROVIDERID_PCT", orphanPct, String(orphanPct), false, details);
    insertMetric(resultsTbl, base, c, "ORPHAN_PROVIDERID_FLAG", orphanFlag, String(orphanFlag), (orphanFlag === 1), details);
  }
}
return `DC 1.12 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';