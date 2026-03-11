CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_01"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT='DC 1.01 (Table IID): checks whether all 25 PCORnet CDM tables exist in <DB_PARAM>.<SCHEMA_NAME>. Core required: DEMOGRAPHIC, ENROLLMENT, ENCOUNTER, DIAGNOSIS, PROCEDURES, HARVEST. EHR-only tables (LAB_RESULT_CM, PRESCRIBING, VITAL) are flagged as exceptions only when EHR is inferred from HARVEST. Remaining CDM tables are checked informationally. Output: <DB_PARAM>.CHARACTERIZATION_DCQ.DCQ_RESULTS where ROW_NUM=1.01.'
EXECUTE AS CALLER
AS '
function q(sqlText, binds) { return snowflake.execute({ sqlText, binds: binds || [] }); }
function isSafeIdentPart(s) { return /^[A-Za-z0-9_$]+$/.test((s || '''').toString()); }
function tableMeta(db, schema, tables) {
  const out = {};
  for (const t of tables) out[t] = { exists: false, row_count: null };
  if (!tables.length) return out;
  const placeholders = tables.map(() => "?").join(",");
  const rs = q(
    `SELECT TABLE_NAME, ROW_COUNT
     FROM ${db}.INFORMATION_SCHEMA.TABLES
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN (${placeholders})`,
    [schema.toUpperCase()].concat(tables)
  );
  while (rs.next()) {
    const name = rs.getColumnValue(1);
    const rc = rs.getColumnValue(2);
    out[name] = { exists: true, row_count: rc };
  }
  return out;
}
function insertMetric(resultsTbl, baseBinds, metric, valueNum, valueStr, exceptionFlag, detailsJson) {
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
    baseBinds.concat([metric, valueNum, valueStr, flagInt, detailsJson])
  );
}
function getRegistry(rowNum) {
  const rs = q(
    `SELECT CHECK_ID, CHECK_NAME, EDC_TABLE
     FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY
     WHERE ROW_NUM = ?
     QUALIFY ROW_NUMBER() OVER (ORDER BY CHECK_ID) = 1`,
    [rowNum]
  );
  if (!rs.next()) throw new Error(`No registry row found for ROW_NUM=${rowNum}`);
  return { checkId: rs.getColumnValue(1), checkName: rs.getColumnValue(2), edcTable: rs.getColumnValue(3) };
}
function harvestSuggestsEhr(db, schema) {
  // Conservative heuristic: if a recognizable column exists and indicates EHR, treat as EHR.
  // Otherwise return null (unknown).
  const candidates = ["EHR_CAPTURE", "DATA_SOURCE", "DATASOURCE", "CDM_SOURCE", "SOURCE_DATA"];
  // check HARVEST exists
  const harvestMeta = tableMeta(db, schema, ["HARVEST"]);
  if (!harvestMeta["HARVEST"]?.exists) return null;
  for (const c of candidates) {
    const rsCol = q(
      `SELECT COUNT(*)
       FROM ${db}.INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ''HARVEST'' AND COLUMN_NAME = ?`,
      [schema.toUpperCase(), c]
    );
    rsCol.next();
    if (rsCol.getColumnValue(1) > 0) {
      const rsVal = q(`SELECT ${c} FROM ${db}.${schema}.HARVEST QUALIFY ROW_NUMBER() OVER (ORDER BY 1) = 1`);
      if (!rsVal.next()) return null;
      const v = rsVal.getColumnValue(1);
      if (v === null) return null;
      const s = String(v).trim().toUpperCase();
      if (["Y","YES","TRUE","1"].includes(s)) return true;
      if (s.includes("EHR")) return true;
      if (["N","NO","FALSE","0"].includes(s)) return false;
      if (s.includes("CLAIM")) return false;
      return null;
    }
  }
  return null;
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 1.01;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
q(`CREATE SCHEMA IF NOT EXISTS ${outSchema}`);
q(`CREATE TABLE IF NOT EXISTS ${resultsTbl} (
  RUN_ID STRING, CHECK_ID STRING, CHECK_NAME STRING, ROW_NUM NUMBER(10,2), EDC_TABLE STRING,
  SOURCE_TABLE STRING, CODE_TYPE STRING, METRIC STRING, VALUE_NUM NUMBER(38,10), VALUE_STR STRING,
  THRESHOLD_NUM NUMBER(38,10), EXCEPTION_FLAG BOOLEAN, DETAILS VARIANT,
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)`);
const reg = getRegistry(rowNum);
// delete prior rows for this run/check (scoped if TARGET_TABLE provided)
if (only === "ALL") {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
} else {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ? AND UPPER(SOURCE_TABLE)=?`, [RUN_ID, rowNum, only]);
}
const allCdmTables = [
  "CONDITION","DEATH","DEATH_CAUSE","DEMOGRAPHIC","DIAGNOSIS","DISPENSING",
  "ENCOUNTER","ENROLLMENT","EXTERNAL_MEDS","HARVEST","HASH_TOKEN",
  "IMMUNIZATION","LAB_HISTORY","LAB_RESULT_CM","LDS_ADDRESS_HISTORY",
  "MED_ADMIN","OBS_CLIN","OBS_GEN","PAT_RELATIONSHIP","PCORNET_TRIAL",
  "PRESCRIBING","PROCEDURES","PROVIDER","PRO_CM","VITAL"
];
const coreRequired = ["DEMOGRAPHIC","ENROLLMENT","ENCOUNTER","DIAGNOSIS","PROCEDURES","HARVEST"];
const ehrRequired = ["LAB_RESULT_CM","PRESCRIBING","VITAL"];
const ehrFlag = harvestSuggestsEhr(DB_PARAM, SCHEMA_NAME); // true/false/null
const enforceEhr = (ehrFlag === true);
let tables = allCdmTables.slice();
if (only !== "ALL") {
  if (!allCdmTables.includes(only)) {
    throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or a CDM table name.`);
  }
  tables = [only];
}
const meta = tableMeta(DB_PARAM, SCHEMA_NAME, tables);
for (const t of tables) {
  const m = meta[t] || { exists: false, row_count: null };
  const missing = !m.exists;
  const isCore = coreRequired.includes(t);
  const isEhr = ehrRequired.includes(t);
  const tier = isCore ? "CORE" : (isEhr ? "EHR" : "CDM");
  // Exception if a core table is missing, or an EHR table is missing when EHR is inferred
  const missingException = missing && (isCore || (isEhr && enforceEhr));
  const details = JSON.stringify({
    table_exists: m.exists,
    row_count: m.row_count,
    required_tier: tier,
    ehr_inferred: ehrFlag
  });
  const base = [RUN_ID, reg.checkId, reg.checkName, rowNum, reg.edcTable, t];
  insertMetric(resultsTbl, base, "TABLE_EXISTS_FLAG", m.exists ? 1 : 0, m.exists ? "1" : "0", false, details);
  insertMetric(resultsTbl, base, "MISSING_REQUIRED_TABLE_FLAG", missing ? 1 : 0, missing ? "1" : "0", missingException, details);
}
return `DC 1.01 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';