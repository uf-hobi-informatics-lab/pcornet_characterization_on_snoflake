CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_10"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
      0,
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
  ENCOUNTER: ''ADMIT_DATE''
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
const rowNum = 1.10;
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
if (!tableExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER")) {
  const base = [RUN_ID, checkId, checkName, rowNum, edcTable];
  insertMetric(resultsTbl, base, "ENCOUNTER", "STATUS", null, "ERROR", true, { message: "ENCOUNTER table missing; cannot evaluate replication errors." });
  return `DC 1.10 ERROR: ENCOUNTER missing`;
}
const targets = [
  { table: "DIAGNOSIS", encounterId: "ENCOUNTERID", encType: "ENC_TYPE", admitDate: "ADMIT_DATE" },
  { table: "PROCEDURES", encounterId: "ENCOUNTERID", encType: "ENC_TYPE", admitDate: "ADMIT_DATE" }
];
let selected = targets;
if (only !== "ALL") {
  selected = targets.filter(t => t.table === only);
  if (selected.length === 0) throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, DIAGNOSIS, or PROCEDURES.`);
}
for (const t of selected) {
  const base = [RUN_ID, checkId, checkName, rowNum, edcTable];
  if (!tableExists(DB_PARAM, SCHEMA_NAME, t.table)) {
    insertMetric(resultsTbl, base, t.table, "STATUS", null, "SKIPPED", false, { message: "Table does not exist" });
    continue;
  }
  // Validate required columns exist
  const missingCols = [];
  for (const c of [t.encounterId, t.encType, t.admitDate]) {
    if (!colExists(DB_PARAM, SCHEMA_NAME, t.table, c)) missingCols.push(c);
  }
  for (const c of ["ENCOUNTERID", "ENC_TYPE", "ADMIT_DATE"]) {
    if (!colExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER", c)) missingCols.push(`ENCOUNTER.${c}`);
  }
  if (missingCols.length > 0) {
    insertMetric(resultsTbl, base, t.table, "STATUS", null, "ERROR", true, { message: "Missing required columns", missing_columns: missingCols });
    continue;
  }
  const fullSrc = `${DB_PARAM}.${SCHEMA_NAME}.${t.table}`;
  const fullEnc = `${DB_PARAM}.${SCHEMA_NAME}.ENCOUNTER`;
  const sql = `
    WITH src AS (
      SELECT
        ${t.encounterId} AS encounterid,
        ${t.encType} AS enc_type,
        ${t.admitDate} AS admit_date
      FROM ${fullSrc}
      WHERE ${t.encounterId} IS NOT NULL ${dateFilterWhere(t.table)}
    ),
    joined AS (
      SELECT
        s.encounterid,
        s.enc_type AS src_enc_type,
        e.enc_type AS enc_enc_type,
        s.admit_date AS src_admit_date,
        e.admit_date AS enc_admit_date,
        IFF(s.enc_type IS DISTINCT FROM e.enc_type, 1, 0) AS enc_type_mismatch,
        IFF(s.admit_date IS DISTINCT FROM e.admit_date, 1, 0) AS admit_date_mismatch
      FROM src s
      JOIN ${fullEnc} e
        ON s.encounterid = e.encounterid
    ),
    agg AS (
      SELECT
        (SELECT COUNT(*) FROM src) AS total_records_with_encounterid,
        SUM(IFF(enc_type_mismatch=1 OR admit_date_mismatch=1, 1, 0)) AS replication_error_records,
        SUM(enc_type_mismatch) AS enc_type_mismatch_records,
        SUM(admit_date_mismatch) AS admit_date_mismatch_records
      FROM joined
    )
    SELECT
      total_records_with_encounterid,
      replication_error_records,
      enc_type_mismatch_records,
      admit_date_mismatch_records,
      IFF(total_records_with_encounterid > 0,
          (replication_error_records::FLOAT / total_records_with_encounterid::FLOAT) * 100,
          0
      ) AS replication_error_pct
    FROM agg
  `;
  const rs = q(sql);
  rs.next();
  const total = Number(rs.getColumnValue(1));
  const err = Number(rs.getColumnValue(2));
  const encTypeErr = Number(rs.getColumnValue(3));
  const admitErr = Number(rs.getColumnValue(4));
  const pct = Number(rs.getColumnValue(5));
  const flag = err > 0 ? 1 : 0;
  const details = {
    source_table: t.table,
    total_records_with_encounterid: total,
    replication_error_records: err,
    enc_type_mismatch_records: encTypeErr,
    admit_date_mismatch_records: admitErr,
    replication_error_pct: pct
  };
  insertMetric(resultsTbl, base, t.table, "TOTAL_RECORDS_WITH_ENCOUNTERID", total, String(total), false, details);
  insertMetric(resultsTbl, base, t.table, "REPLICATION_ERROR_RECORDS", err, String(err), false, details);
  insertMetric(resultsTbl, base, t.table, "ENC_TYPE_MISMATCH_RECORDS", encTypeErr, String(encTypeErr), false, details);
  insertMetric(resultsTbl, base, t.table, "ADMIT_DATE_MISMATCH_RECORDS", admitErr, String(admitErr), false, details);
  insertMetric(resultsTbl, base, t.table, "REPLICATION_ERROR_PCT", pct, String(pct), false, details);
  insertMetric(resultsTbl, base, t.table, "REPLICATION_ERROR_FLAG", flag, String(flag), (flag === 1), details);
}
return `DC 1.10 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';