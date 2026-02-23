CREATE OR REPLACE PROCEDURE "SP_DC_2_01"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR)
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
const rowNum = 2.01;
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
const base = [RUN_ID, checkId, checkName, rowNum, edcTable];
if (!tableExists(DB_PARAM, SCHEMA_NAME, "HARVEST")) {
  insertMetric(resultsTbl, base, "HARVEST", "REFRESH_MAX", "STATUS", null, "ERROR", 0, true, {
    message: "HARVEST table does not exist; cannot compute max refresh date."
  });
  return `DC 2.01 ERROR: HARVEST missing`;
}
// Compute MAX refresh date from HARVEST REFRESH_*_DATE fields
const harvest = `${DB_PARAM}.${SCHEMA_NAME}.HARVEST`;
const mxRs = q(
  `
  WITH h AS (
    SELECT OBJECT_CONSTRUCT(*) AS o
    FROM ${harvest}
    QUALIFY ROW_NUMBER() OVER (ORDER BY 1) = 1
  ),
  mx AS (
    SELECT MAX(TRY_TO_DATE(v.value::string)) AS mxrefresh
    FROM h, LATERAL FLATTEN(input => o) v
    WHERE REGEXP_LIKE(UPPER(v.key::string), ''^REFRESH_.*_DATE$'')
  )
  SELECT
    mxrefresh,
    TO_CHAR(mxrefresh, ''YYYY-MM-DD'') AS mxrefresh_str
  FROM mx
  `
);
mxRs.next();
const mxrefreshStr = mxRs.getColumnValue(2); // ''YYYY-MM-DD'' or null
insertMetric(
  resultsTbl,
  base,
  "HARVEST",
  "REFRESH_MAX",
  "MAX_REFRESH_DATE",
  null,
  mxrefreshStr === null ? "NULL" : String(mxrefreshStr),
  0,
  (mxrefreshStr === null),
  { max_refresh_date: mxrefreshStr === null ? null : String(mxrefreshStr) }
);
if (mxrefreshStr === null) {
  return `DC 2.01 ERROR: max refresh date is NULL`;
}
// Date fields to check (safe skips if missing)
const checks = [
  { table: "DEMOGRAPHIC", cols: ["BIRTH_DATE"] },
  { table: "ENROLLMENT", cols: ["ENR_START_DATE","ENR_END_DATE"] },
  { table: "ENCOUNTER", cols: ["ADMIT_DATE","DISCHARGE_DATE"] },
  { table: "DIAGNOSIS", cols: ["ADMIT_DATE","DX_DATE"] },
  { table: "PROCEDURES", cols: ["ADMIT_DATE","PX_DATE"] },
  { table: "VITAL", cols: ["MEASURE_DATE"] },
  { table: "DISPENSING", cols: ["DISPENSE_DATE"] },
  { table: "PRESCRIBING", cols: ["RX_ORDER_DATE"] },
  { table: "LAB_RESULT_CM", cols: ["RESULT_DATE"] },
  { table: "CONDITION", cols: ["REPORT_DATE","ONSET_DATE","RESOLVE_DATE"] },
  { table: "DEATH", cols: ["DEATH_DATE"] },
  { table: "MED_ADMIN", cols: ["MEDADMIN_START_DATE","MEDADMIN_STOP_DATE"] },
  { table: "OBS_CLIN", cols: ["OBSCLIN_START_DATE","OBSCLIN_STOP_DATE"] },
  { table: "OBS_GEN", cols: ["OBSGEN_START_DATE","OBSGEN_STOP_DATE"] },
  { table: "PRO_CM", cols: ["PRO_DATE"] },
  { table: "IMMUNIZATION", cols: ["VX_RECORD_DATE","VX_ADMIN_DATE"] },
  { table: "LDS_ADDRESS_HISTORY", cols: ["ADDRESS_PERIOD_START","ADDRESS_PERIOD_END"] },
  { table: "PAT_RELATIONSHIP", cols: ["RELATIONSHIP_START_DATE","RELATIONSHIP_END_DATE"] },
  { table: "EXTERNAL_MEDS", cols: ["EXT_RECORD_DATE","RX_START_DATE","RX_END_DATE"] }
];
let selected = checks;
if (only !== "ALL") {
  selected = checks.filter(x => x.table === only);
  if (selected.length === 0) {
    throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or a supported CDM table in DC 2.01.`);
  }
}
for (const t of selected) {
  if (!tableExists(DB_PARAM, SCHEMA_NAME, t.table)) continue;
  const fullTable = `${DB_PARAM}.${SCHEMA_NAME}.${t.table}`;
  for (const c of t.cols) {
    if (!colExists(DB_PARAM, SCHEMA_NAME, t.table, c)) continue;
    const sql = `
      SELECT
        COUNT_IF(TRY_TO_DATE(${c}) IS NOT NULL) AS date_populated_n,
        COUNT_IF(TRY_TO_DATE(${c}) IS NOT NULL AND TRY_TO_DATE(${c}) > TO_DATE(?)) AS future_date_n
      FROM ${fullTable}
    `;
    const rs = q(sql, [mxrefreshStr]);
    rs.next();
    const denom = Number(rs.getColumnValue(1));
    const numer = Number(rs.getColumnValue(2));
    const pct = (denom > 0) ? (numer / denom) * 100 : 0;
    const flag = (pct > 5) ? 1 : 0;
    const details = {
      max_refresh_date: String(mxrefreshStr),
      date_field: c,
      date_populated_n: denom,
      future_date_n: numer,
      future_date_pct: pct,
      threshold_pct: 5
    };
    insertMetric(resultsTbl, base, t.table, c, "DATE_POPULATED_N", denom, String(denom), 5, false, details);
    insertMetric(resultsTbl, base, t.table, c, "FUTURE_DATE_N", numer, String(numer), 5, false, details);
    insertMetric(resultsTbl, base, t.table, c, "FUTURE_DATE_PCT", pct, String(pct), 5, false, details);
    insertMetric(resultsTbl, base, t.table, c, "FUTURE_DATE_FLAG", flag, String(flag), 5, (flag === 1), details);
  }
}
return `DC 2.01 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';