CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_02"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT='DC 1.02 (Table IID): required CDM tables are populated (rowcount > 0). Uses INFORMATION_SCHEMA.TABLES.ROW_COUNT when available; falls back to COUNT(*) if needed. Required-for-all: DEMOGRAPHIC, ENROLLMENT, ENCOUNTER, DIAGNOSIS, PROCEDURES, HARVEST. EHR-only tables (LAB_RESULT_CM, PRESCRIBING, VITAL) are enforced only if EHR can be inferred from HARVEST; otherwise recorded informationally. Run: CALL CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_02(DB_PARAM, SCHEMA_NAME, RUN_ID, TARGET_TABLE|''ALL''); or driver: CALL CHARACTERIZATION.DCQ.SP_RUN_DCQ(DB_PARAM, SCHEMA_NAME, ''ROW_NUM'', ''1.02'', ''part1'', TARGET_TABLE|''ALL''). Output: <DB_PARAM>.CHARACTERIZATION_DCQ.DCQ_RESULTS where ROW_NUM=1.02. Interpret: per SOURCE_TABLE, METRIC=''EMPTY_REQUIRED_TABLE_FLAG'' with VALUE_NUM=1 / EXCEPTION_FLAG=TRUE indicates table exists but has 0 rows; METRIC=''MISSING_REQUIRED_TABLE_FLAG'' indicates table missing entirely.'
EXECUTE AS CALLER
AS '
function q(sqlText, binds) { return snowflake.execute({ sqlText, binds: binds || [] }); }
function isSafeIdentPart(s) { return /^[A-Za-z0-9_$]+$/.test((s || '''').toString()); }
function scalar(sqlText, binds) { const rs = q(sqlText, binds || []); rs.next(); return rs.getColumnValue(1); }
function tableMeta(db, schema, tables) {
  q("DROP TABLE IF EXISTS _TBL");
  q("CREATE TEMP TABLE _TBL(table_name STRING)");
  if (tables.length) {
    const selects = tables.map(() => "SELECT ?").join(" UNION ALL ");
    q(`INSERT INTO _TBL(table_name) ${selects}`, tables);
  }
  const rs = q(
    `SELECT t.table_name, i.row_count
     FROM _TBL t
     LEFT JOIN ${db}.INFORMATION_SCHEMA.TABLES i
       ON i.table_schema = ? AND i.table_name = t.table_name`,
    [schema.toUpperCase()]
  );
  const out = {};
  while (rs.next()) {
    const name = rs.getColumnValue(1);
    const rc = rs.getColumnValue(2);
    out[name] = { exists: rc !== null, row_count: rc };
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
  const candidates = ["EHR_CAPTURE", "DATA_SOURCE", "DATASOURCE", "CDM_SOURCE", "SOURCE_DATA"];
  const harvestMeta = tableMeta(db, schema, ["HARVEST"]);
  if (!harvestMeta["HARVEST"]?.exists) return null;
  for (const c of candidates) {
    const hasCol = scalar(
      `SELECT COUNT(*)
       FROM ${db}.INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ''HARVEST'' AND COLUMN_NAME = ?`,
      [schema.toUpperCase(), c]
    );
    if (hasCol > 0) {
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
const rowNum = 1.02;
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
const requiredAll = ["DEMOGRAPHIC","ENROLLMENT","ENCOUNTER","DIAGNOSIS","PROCEDURES","HARVEST"];
const requiredEhr = ["LAB_RESULT_CM","PRESCRIBING","VITAL"];
const ehrFlag = harvestSuggestsEhr(DB_PARAM, SCHEMA_NAME); // true/false/null
const enforceEhr = (ehrFlag === true);
let tables = requiredAll.slice();
if (enforceEhr) tables = tables.concat(requiredEhr);
if (only !== "ALL") {
  if (!requiredAll.includes(only) && !requiredEhr.includes(only) && only !== "HARVEST") {
    throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or a CDM table name.`);
  }
  tables = [only];
}
const meta = tableMeta(DB_PARAM, SCHEMA_NAME, tables);
for (const t of tables) {
  const m = meta[t] || { exists: false, row_count: null };
  let nobs = null;
  if (!m.exists) {
    nobs = 0;
  } else if (m.row_count !== null) {
    nobs = Number(m.row_count);
    if (!Number.isFinite(nobs)) nobs = 0;
  } else {
    // metadata missing: fallback to COUNT(*)
    nobs = Number(scalar(`SELECT COUNT(*) FROM ${DB_PARAM}.${SCHEMA_NAME}.${t}`));
  }
  const emptyFlag = (m.exists && nobs === 0) ? 1 : 0;
  const missingTableFlag = (!m.exists) ? 1 : 0;
  // For 1.02 we flag exception if required table is missing OR exists but empty
  const exception = (missingTableFlag === 1) || (emptyFlag === 1);
  const details = JSON.stringify({
    table_exists: m.exists,
    nobs: nobs,
    row_count_metadata: m.row_count,
    required_tier: requiredAll.includes(t) ? "ALL" : (requiredEhr.includes(t) ? "EHR" : "UNKNOWN"),
    ehr_inferred: ehrFlag
  });
  const base = [RUN_ID, reg.checkId, reg.checkName, rowNum, reg.edcTable, t];
  insertMetric(resultsTbl, base, "TABLE_EXISTS_FLAG", m.exists ? 1 : 0, m.exists ? "1" : "0", false, details);
  insertMetric(resultsTbl, base, "NOBS", nobs, String(nobs), false, details);
  insertMetric(resultsTbl, base, "EMPTY_REQUIRED_TABLE_FLAG", emptyFlag, String(emptyFlag), (emptyFlag === 1), details);
  insertMetric(resultsTbl, base, "MISSING_REQUIRED_TABLE_FLAG", missingTableFlag, String(missingTableFlag), (missingTableFlag === 1), details);
}
return `DC 1.02 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';