CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_2_03"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
const vStartDate = (START_DATE || '').toString().trim() || null;
const vEndDate = (END_DATE || '').toString().trim() || null;
function dateFilter(colName) {
  let clause = '';
  if (vStartDate) clause += ` AND TRY_TO_DATE(${colName}) >= TRY_TO_DATE(''${vStartDate}'')`;
  if (vEndDate) clause += ` AND TRY_TO_DATE(${colName}) <= TRY_TO_DATE(''${vEndDate}'')`;
  return clause;
}
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 2.03;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
const thresholdPct = 5;
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
// Helper: emit 4 standard metrics given denom/numer
function emit4(sourceTable, codeType, denom, numer, details) {
  const pct = (denom > 0) ? (numer / denom) * 100 : 0;
  const flag = (pct > thresholdPct) ? 1 : 0;
  insertMetric(resultsTbl, base, sourceTable, codeType, "RECORDS_EVAL_N", denom, String(denom), thresholdPct, false, details);
  insertMetric(resultsTbl, base, sourceTable, codeType, "ILLOGICAL_N", numer, String(numer), thresholdPct, false, details);
  insertMetric(resultsTbl, base, sourceTable, codeType, "ILLOGICAL_PCT", pct, String(pct), thresholdPct, false, details);
  insertMetric(resultsTbl, base, sourceTable, codeType, "ILLOGICAL_FLAG", flag, String(flag), thresholdPct, (flag === 1), details);
}
// Require DEMOGRAPHIC for before-birth logic
const hasDemo = tableExists(DB_PARAM, SCHEMA_NAME, "DEMOGRAPHIC") && colExists(DB_PARAM, SCHEMA_NAME, "DEMOGRAPHIC", "PATID") && colExists(DB_PARAM, SCHEMA_NAME, "DEMOGRAPHIC", "BIRTH_DATE");
// DEATH optional; we use earliest DEATH_DATE per PATID if present
const hasDeath = tableExists(DB_PARAM, SCHEMA_NAME, "DEATH") && colExists(DB_PARAM, SCHEMA_NAME, "DEATH", "PATID") && colExists(DB_PARAM, SCHEMA_NAME, "DEATH", "DEATH_DATE");
// A) Dates before birth / after death for selected date fields
const serviceDateChecks = [
  { table: "ENCOUNTER", cols: ["ADMIT_DATE","DISCHARGE_DATE"] },
  { table: "DIAGNOSIS", cols: ["DX_DATE"] },
  { table: "PROCEDURES", cols: ["PX_DATE"] },
  { table: "VITAL", cols: ["MEASURE_DATE"] },
  { table: "LAB_RESULT_CM", cols: ["RESULT_DATE"] },
  { table: "PRESCRIBING", cols: ["RX_ORDER_DATE"] },
  { table: "DISPENSING", cols: ["DISPENSE_DATE"] },
  { table: "CONDITION", cols: ["REPORT_DATE","ONSET_DATE","RESOLVE_DATE"] },
  { table: "IMMUNIZATION", cols: ["VX_RECORD_DATE","VX_ADMIN_DATE"] },
  { table: "MED_ADMIN", cols: ["MEDADMIN_START_DATE","MEDADMIN_STOP_DATE"] },
  { table: "OBS_CLIN", cols: ["OBSCLIN_START_DATE","OBSCLIN_STOP_DATE"] },
  { table: "OBS_GEN", cols: ["OBSGEN_START_DATE","OBSGEN_STOP_DATE"] },
  { table: "PRO_CM", cols: ["PRO_DATE"] },
  { table: "EXTERNAL_MEDS", cols: ["EXT_RECORD_DATE","RX_START_DATE","RX_END_DATE"] },
  { table: "PAT_RELATIONSHIP", cols: ["RELATIONSHIP_START_DATE","RELATIONSHIP_END_DATE"] }
];
let selectedSvc = serviceDateChecks;
if (only !== "ALL") {
  selectedSvc = serviceDateChecks.filter(x => x.table === only);
}
for (const t of selectedSvc) {
  if (!tableExists(DB_PARAM, SCHEMA_NAME, t.table)) continue;
  if (!colExists(DB_PARAM, SCHEMA_NAME, t.table, "PATID")) continue;
  const fullTable = `${DB_PARAM}.${SCHEMA_NAME}.${t.table}`;
  for (const c of t.cols) {
    if (!colExists(DB_PARAM, SCHEMA_NAME, t.table, c)) continue;
    // BEFORE_BIRTH
    if (hasDemo) {
      const sql = `
        WITH demo AS (
          SELECT PATID, TRY_TO_DATE(BIRTH_DATE) AS birth_date
          FROM ${DB_PARAM}.${SCHEMA_NAME}.DEMOGRAPHIC
          WHERE PATID IS NOT NULL AND TRY_TO_DATE(BIRTH_DATE) IS NOT NULL
        ),
        src AS (
          SELECT PATID, TRY_TO_DATE(${c}) AS dt
          FROM ${fullTable}
          WHERE PATID IS NOT NULL AND TRY_TO_DATE(${c}) IS NOT NULL
        ),
        j AS (
          SELECT s.PATID, s.dt, d.birth_date
          FROM src s
          JOIN demo d ON s.PATID = d.PATID
        )
        SELECT
          COUNT(*) AS denom,
          COUNT_IF(dt < birth_date) AS numer
        FROM j
      `;
      const rs = q(sql);
      rs.next();
      const denom = Number(rs.getColumnValue(1));
      const numer = Number(rs.getColumnValue(2));
      emit4(
        t.table,
        `BEFORE_BIRTH:${c}`,
        denom,
        numer,
        { rule: "BEFORE_BIRTH", table: t.table, date_field: c, threshold_pct: thresholdPct }
      );
    }
    // AFTER_DEATH
    if (hasDeath) {
      const sql = `
        WITH death_min AS (
          SELECT PATID, MIN(TRY_TO_DATE(DEATH_DATE)) AS death_date
          FROM ${DB_PARAM}.${SCHEMA_NAME}.DEATH
          WHERE PATID IS NOT NULL AND TRY_TO_DATE(DEATH_DATE) IS NOT NULL
          GROUP BY PATID
        ),
        src AS (
          SELECT PATID, TRY_TO_DATE(${c}) AS dt
          FROM ${fullTable}
          WHERE PATID IS NOT NULL AND TRY_TO_DATE(${c}) IS NOT NULL
        ),
        j AS (
          SELECT s.PATID, s.dt, d.death_date
          FROM src s
          JOIN death_min d ON s.PATID = d.PATID
        )
        SELECT
          COUNT(*) AS denom,
          COUNT_IF(dt > death_date) AS numer
        FROM j
      `;
      const rs = q(sql);
      rs.next();
      const denom = Number(rs.getColumnValue(1));
      const numer = Number(rs.getColumnValue(2));
      emit4(
        t.table,
        `AFTER_DEATH:${c}`,
        denom,
        numer,
        { rule: "AFTER_DEATH", table: t.table, date_field: c, threshold_pct: thresholdPct }
      );
    }
  }
}
// C) Procedure date outside encounter window (+/- 5 days)
if ((only === "ALL" || only === "PROCEDURES") &&
    tableExists(DB_PARAM, SCHEMA_NAME, "PROCEDURES") &&
    tableExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER") &&
    colExists(DB_PARAM, SCHEMA_NAME, "PROCEDURES", "ENCOUNTERID") &&
    colExists(DB_PARAM, SCHEMA_NAME, "PROCEDURES", "PX_DATE") &&
    colExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER", "ENCOUNTERID") &&
    colExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER", "ADMIT_DATE") &&
    colExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER", "DISCHARGE_DATE")) {
  const sql = `
    WITH p AS (
      SELECT ENCOUNTERID, TRY_TO_DATE(PX_DATE) AS px_date
      FROM ${DB_PARAM}.${SCHEMA_NAME}.PROCEDURES
      WHERE ENCOUNTERID IS NOT NULL AND TRY_TO_DATE(PX_DATE) IS NOT NULL
    ),
    e AS (
      SELECT ENCOUNTERID,
             TRY_TO_DATE(ADMIT_DATE) AS admit_date,
             TRY_TO_DATE(DISCHARGE_DATE) AS discharge_date
      FROM ${DB_PARAM}.${SCHEMA_NAME}.ENCOUNTER
      WHERE ENCOUNTERID IS NOT NULL
    ),
    j AS (
      SELECT p.ENCOUNTERID, p.px_date, e.admit_date, e.discharge_date
      FROM p JOIN e ON p.ENCOUNTERID = e.ENCOUNTERID
      WHERE e.admit_date IS NOT NULL OR e.discharge_date IS NOT NULL
    )
    SELECT
      COUNT(*) AS denom,
      COUNT_IF(
        (admit_date IS NOT NULL AND px_date < DATEADD(day, -5, admit_date))
        OR
        (discharge_date IS NOT NULL AND px_date > DATEADD(day, 5, discharge_date))
      ) AS numer
    FROM j
  `;
  const rs = q(sql);
  rs.next();
  const denom = Number(rs.getColumnValue(1));
  const numer = Number(rs.getColumnValue(2));
  emit4(
    "PROCEDURES",
    "PXDATE_OUTSIDE_ENCOUNTER",
    denom,
    numer,
    { rule: "PXDATE_OUTSIDE_ENCOUNTER", window_days: 5, threshold_pct: thresholdPct }
  );
}
// D) Stop date before start date
const stopStart = [
  { table: "EXTERNAL_MEDS", start: "RX_START_DATE", stop: "RX_END_DATE" },
  { table: "MED_ADMIN", start: "MEDADMIN_START_DATE", stop: "MEDADMIN_STOP_DATE" },
  { table: "OBS_CLIN", start: "OBSCLIN_START_DATE", stop: "OBSCLIN_STOP_DATE" },
  { table: "OBS_GEN", start: "OBSGEN_START_DATE", stop: "OBSGEN_STOP_DATE" },
  { table: "PAT_RELATIONSHIP", start: "RELATIONSHIP_START_DATE", stop: "RELATIONSHIP_END_DATE" }
];
let selectedSS = stopStart;
if (only !== "ALL") selectedSS = stopStart.filter(x => x.table === only);
for (const s of selectedSS) {
  if (!tableExists(DB_PARAM, SCHEMA_NAME, s.table)) continue;
  if (!colExists(DB_PARAM, SCHEMA_NAME, s.table, s.start)) continue;
  if (!colExists(DB_PARAM, SCHEMA_NAME, s.table, s.stop)) continue;
  const fullTable = `${DB_PARAM}.${SCHEMA_NAME}.${s.table}`;
  const sql = `
    SELECT
      COUNT_IF(TRY_TO_DATE(${s.start}) IS NOT NULL AND TRY_TO_DATE(${s.stop}) IS NOT NULL) AS denom,
      COUNT_IF(TRY_TO_DATE(${s.start}) IS NOT NULL AND TRY_TO_DATE(${s.stop}) IS NOT NULL
               AND TRY_TO_DATE(${s.stop}) < TRY_TO_DATE(${s.start})) AS numer
    FROM ${fullTable}
  `;
  const rs = q(sql);
  rs.next();
  const denom = Number(rs.getColumnValue(1));
  const numer = Number(rs.getColumnValue(2));
  emit4(
    s.table,
    `STOP_BEFORE_START:${s.start}:${s.stop}`,
    denom,
    numer,
    { rule: "STOP_BEFORE_START", table: s.table, start_field: s.start, stop_field: s.stop, threshold_pct: thresholdPct }
  );
}
return `DC 2.03 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';