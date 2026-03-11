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
function insertMetric(resultsTbl, baseBinds, sourceTable, metric, valueNum, valueStr, exceptionFlag, detailsObj) {
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
      ?, NULL, ?, ?, ?,
      5,
      IFF(?=1, TRUE, FALSE),
      PARSE_JSON(?)
    `,
    baseBinds.concat([sourceTable, metric, valueNum, valueStr, flagInt, detailsJson])
  );
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
function normDateParam(x) { if (x === null || x === undefined) return null; var v = x.toString().trim(); var u = v.toUpperCase(); return (u === '''' || u === ''NONE'' || u === ''NULL'' || u === ''(NONE)'') ? null : v; }
const vStartDate = normDateParam(START_DATE);
const vEndDate = normDateParam(END_DATE);
const tableDateCol = {
  CONDITION: ''REPORT_DATE'',
  DIAGNOSIS: ''DX_DATE'',
  ENCOUNTER: ''ADMIT_DATE'',
  IMMUNIZATION: ''VX_RECORD_DATE'',
  LAB_RESULT_CM: ''RESULT_DATE'',
  MED_ADMIN: ''MEDADMIN_START_DATE'',
  OBS_CLIN: ''OBSCLIN_START_DATE'',
  OBS_GEN: ''OBSGEN_START_DATE'',
  PRESCRIBING: ''RX_ORDER_DATE'',
  PROCEDURES: ''PX_DATE'',
  PRO_CM: ''PRO_DATE'',
  VITAL: ''MEASURE_DATE''
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
const rowNum = 1.11;
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
const base = [RUN_ID, checkId, checkName, rowNum, edcTable];
// delete prior rows for this run/check
q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);

// All CDM tables that have ENCOUNTERID and PATID
const encIdTables = [
  "CONDITION","DIAGNOSIS","ENCOUNTER","IMMUNIZATION","LAB_RESULT_CM",
  "MED_ADMIN","OBS_CLIN","OBS_GEN","PRESCRIBING","PROCEDURES","PRO_CM","VITAL"
];

let tablesToCheck = encIdTables;
if (only !== "ALL") {
  if (!encIdTables.includes(only)) {
    throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or one of: ${encIdTables.join('', '')}.`);
  }
  tablesToCheck = [only];
}

for (const tbl of tablesToCheck) {
  if (!tableExists(DB_PARAM, SCHEMA_NAME, tbl) ||
      !colExists(DB_PARAM, SCHEMA_NAME, tbl, ''ENCOUNTERID'') ||
      !colExists(DB_PARAM, SCHEMA_NAME, tbl, ''PATID'')) {
    insertMetric(resultsTbl, base, tbl, "STATUS", null, "SKIPPED", false,
      { message: `${tbl} missing or lacks ENCOUNTERID/PATID columns` });
    continue;
  }

  const fullTbl = `${DB_PARAM}.${SCHEMA_NAME}.${tbl}`;
  const rs = q(
    `WITH per_enc AS (
       SELECT ENCOUNTERID, COUNT(DISTINCT PATID) AS patid_n
       FROM ${fullTbl}
       WHERE ENCOUNTERID IS NOT NULL${dateFilterWhere(tbl)}
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
     FROM agg`
  );
  rs.next();
  const denom = Number(rs.getColumnValue(1));
  const numer = Number(rs.getColumnValue(2));
  const pct = Number(rs.getColumnValue(3));
  const flag = (pct > 5) ? 1 : 0;
  const details = {
    source_table: tbl,
    encounterid_distinct_n: denom,
    multi_patid_encounterid_n: numer,
    multi_patid_encounterid_pct: pct,
    threshold_pct: 5
  };
  insertMetric(resultsTbl, base, tbl, "ENCOUNTERID_DISTINCT_N", denom, String(denom), false, details);
  insertMetric(resultsTbl, base, tbl, "MULTI_PATID_ENCOUNTERID_N", numer, String(numer), false, details);
  insertMetric(resultsTbl, base, tbl, "MULTI_PATID_ENCOUNTERID_PCT", pct, String(pct), false, details);
  insertMetric(resultsTbl, base, tbl, "MULTI_PATID_ENCOUNTERID_FLAG", flag, String(flag), (flag === 1), details);
}

return `DC 1.11 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';
