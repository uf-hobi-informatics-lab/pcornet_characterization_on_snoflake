CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_18"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT='DC 1.18 (Table ID): populated CDM tables have refresh dates documented in HARVEST (REFRESH_* fields). For each table, computes NOBS and whether the corresponding HARVEST refresh field is present. TARGET_TABLE may be a CDM table name, HARVEST, or ALL. Run: CALL CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_18(DB_PARAM, SCHEMA_NAME, RUN_ID, TARGET_TABLE|''ALL''); or driver: CALL CHARACTERIZATION.DCQ.SP_RUN_DCQ(DB_PARAM, SCHEMA_NAME, ''ROW_NUM'', ''1.18'', ''part1'', TARGET_TABLE|''ALL''). Output: <DB_PARAM>.CHARACTERIZATION_DCQ.DCQ_RESULTS where ROW_NUM=1.18. Interpret: per SOURCE_TABLE, METRIC=''MISSING_REFRESH_DATE_FLAG'' with VALUE_NUM=1 / EXCEPTION_FLAG=TRUE indicates a table is populated (NOBS>0) but the HARVEST refresh date is missing; DETAILS.harvest_column and DETAILS.refresh_value show the source field/value.'
EXECUTE AS CALLER
AS '
function q(sqlText, binds) {
  return snowflake.execute({ sqlText, binds: binds || [] });
}
function isSafeIdentPart(s) {
  return /^[A-Za-z0-9_$]+$/.test((s || '''').toString());
}
function scalar(sqlText, binds) {
  const rs = q(sqlText, binds || []);
  rs.next();
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
function insertMetric(resultsTbl, binds, metric, valueNum, valueStr, exceptionFlag, detailsJson) {
  const flagInt = exceptionFlag ? 1 : 0;
  q(
    `INSERT INTO ${resultsTbl} (
       RUN_ID, CHECK_ID, CHECK_NAME, ROW_NUM, EDC_TABLE,
       SOURCE_TABLE, CODE_TYPE, METRIC, VALUE_NUM, VALUE_STR,
       THRESHOLD_NUM, EXCEPTION_FLAG, DETAILS
     )
     SELECT
       ?, ?, ?, ?, ?,
       ?, NULL, ?, ?, ?,
       0,
       IFF(?=1, TRUE, FALSE),
       PARSE_JSON(?)
    `,
    binds.concat([metric, valueNum, valueStr, flagInt, detailsJson])
  );
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 1.18;
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
// delete existing rows for this run/check (scoped if TARGET_TABLE is set)
if (only === "ALL") {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
} else {
  q(
    `DELETE FROM ${resultsTbl}
     WHERE RUN_ID = ? AND ROW_NUM = ? AND UPPER(SOURCE_TABLE) = ?`,
    [RUN_ID, rowNum, only]
  );
}
// HARVEST must exist for this check
if (!tableExists(DB_PARAM, SCHEMA_NAME, "HARVEST")) {
  const details = JSON.stringify({ message: "HARVEST table does not exist" });
  insertMetric(
    resultsTbl,
    [RUN_ID, checkId, checkName, rowNum, edcTable, "HARVEST"],
    "STATUS",
    null,
    "ERROR",
    true,
    details
  );
  return `DC 1.18 ERROR: HARVEST missing`;
}
// Map CDM table -> HARVEST refresh field
const tableToHarvestCol = {
  DEMOGRAPHIC: "REFRESH_DEMOGRAPHIC_DATE",
  ENROLLMENT: "REFRESH_ENROLLMENT_DATE",
  ENCOUNTER: "REFRESH_ENCOUNTER_DATE",
  DIAGNOSIS: "REFRESH_DIAGNOSIS_DATE",
  PROCEDURES: "REFRESH_PROCEDURES_DATE",
  VITAL: "REFRESH_VITAL_DATE",
  DISPENSING: "REFRESH_DISPENSING_DATE",
  LAB_RESULT_CM: "REFRESH_LAB_RESULT_CM_DATE",
  CONDITION: "REFRESH_CONDITION_DATE",
  PRO_CM: "REFRESH_PRO_CM_DATE",
  PRESCRIBING: "REFRESH_PRESCRIBING_DATE",
  PCORNET_TRIAL: "REFRESH_PCORNET_TRIAL_DATE",
  DEATH: "REFRESH_DEATH_DATE",
  DEATH_CAUSE: "REFRESH_DEATH_CAUSE_DATE",
  MED_ADMIN: "REFRESH_MED_ADMIN_DATE",
  OBS_CLIN: "REFRESH_OBS_CLIN_DATE",
  OBS_GEN: "REFRESH_OBS_GEN_DATE",
  PROVIDER: "REFRESH_PROVIDER_DATE",
  HASH_TOKEN: "REFRESH_HASH_TOKEN_DATE",
  LDS_ADDRESS_HISTORY: "REFRESH_LDS_ADDRESS_HX_DATE",
  IMMUNIZATION: "REFRESH_IMMUNIZATION_DATE"
};
function getHarvestValue(colName) {
  if (!colExists(DB_PARAM, SCHEMA_NAME, "HARVEST", colName)) return null;
  const rs = q(
    `SELECT ${colName}
     FROM ${DB_PARAM}.${SCHEMA_NAME}.HARVEST
     QUALIFY ROW_NUMBER() OVER (ORDER BY 1) = 1`
  );
  if (!rs.next()) return null;
  return rs.getColumnValue(1);
}
// determine tables to evaluate
let tables = Object.keys(tableToHarvestCol);
if (only !== "ALL") {
  if (only === "HARVEST") {
    tables = ["HARVEST"];
  } else if (tableToHarvestCol[only]) {
    tables = [only];
  } else {
    throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, HARVEST, or a CDM table in the refresh map.`);
  }
}
for (const t of tables) {
  if (t === "HARVEST") {
    const n = scalar(`SELECT COUNT(*) FROM ${DB_PARAM}.${SCHEMA_NAME}.HARVEST`);
    const details = JSON.stringify({ table_exists: true, nobs: n });
    insertMetric(resultsTbl, [RUN_ID, checkId, checkName, rowNum, edcTable, "HARVEST"], "NOBS", n, String(n), false, details);
    continue;
  }
  const exists = tableExists(DB_PARAM, SCHEMA_NAME, t);
  const nobs = exists ? scalar(`SELECT COUNT(*) FROM ${DB_PARAM}.${SCHEMA_NAME}.${t}`) : 0;
  const harvestCol = tableToHarvestCol[t];
  const refreshVal = harvestCol ? getHarvestValue(harvestCol) : null;
  const refreshStr = (refreshVal === null) ? null : String(refreshVal);
  const refreshPresent = (refreshStr !== null && refreshStr.trim() !== "");
  const missingFlag = (exists && nobs > 0 && !refreshPresent) ? 1 : 0;
  const details = JSON.stringify({
    harvest_column: harvestCol || null,
    refresh_value: refreshStr,
    table_exists: exists,
    nobs: nobs
  });
  const base = [RUN_ID, checkId, checkName, rowNum, edcTable, t];
  // NOBS: never an exception flag
  insertMetric(resultsTbl, base, "NOBS", nobs, String(nobs), false, details);
  // REFRESH_DATE_PRESENT: informational only
  insertMetric(resultsTbl, base, "REFRESH_DATE_PRESENT", refreshPresent ? 1 : 0, refreshPresent ? "1" : "0", false, details);
  // MISSING_REFRESH_DATE_FLAG: this is the exception signal
  insertMetric(resultsTbl, base, "MISSING_REFRESH_DATE_FLAG", missingFlag, String(missingFlag), (missingFlag === 1), details);
}
return `DC 1.18 finished for RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';