CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_2_04"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
function insertMetric(resultsTbl, baseBinds, codeType, metric, valueNum, valueStr, thresholdNum, exceptionFlag, detailsObj) {
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
      ''ENCOUNTER'', ?, ?, ?, ?,
      ?, IFF(?=1, TRUE, FALSE), PARSE_JSON(?)
    `,
    baseBinds.concat([codeType, metric, valueNum, valueStr, thresholdNum, flagInt, detailsJson])
  );
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
const vStartDate = (START_DATE || '''').toString().trim() || null;
const vEndDate = (END_DATE || '''').toString().trim() || null;
function dateFilter(colName) {
  let clause = '''';
  if (vStartDate) clause += ` AND TRY_TO_DATE(${colName}) >= TRY_TO_DATE(''''${vStartDate}'''')`;
  if (vEndDate) clause += ` AND TRY_TO_DATE(${colName}) <= TRY_TO_DATE(''''${vEndDate}'''')`;
  return clause;
}
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 2.04;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
const threshold = 2.0;
if (!(only === "ALL" || only === "ENCOUNTER")) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or ENCOUNTER.`);
}
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
q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
if (!tableExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER")) {
  insertMetric(resultsTbl, base, "ALL", "STATUS", null, "ERROR", threshold, true, { message: "ENCOUNTER table missing" });
  return `DC 2.04 ERROR: ENCOUNTER missing`;
}
for (const c of ["PATID","ENC_TYPE","ADMIT_DATE","PROVIDERID"]) {
  if (!colExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER", c)) {
    insertMetric(resultsTbl, base, "ALL", "STATUS", null, "ERROR", threshold, true, { message: "Missing required column", missing_column: c });
    return `DC 2.04 ERROR: missing ENCOUNTER.${c}`;
  }
}
const fullEnc = `${DB_PARAM}.${SCHEMA_NAME}.ENCOUNTER`;
const rs = q(
  `
  WITH types AS (
    SELECT column1 AS enc_type
    FROM VALUES (''IP''), (''ED''), (''EI'')
  ),
  src0 AS (
    SELECT
      ENC_TYPE,
      PATID,
      TRY_TO_DATE(ADMIT_DATE) AS admit_date,
      PROVIDERID
    FROM ${fullEnc}
    WHERE ENC_TYPE IN (''IP'',''ED'',''EI'')${dateFilter(''ADMIT_DATE'')}
  ),
  totals AS (
    SELECT ENC_TYPE, COUNT(*) AS total_rows_n
    FROM src0
    GROUP BY ENC_TYPE
  ),
  eligible AS (
    SELECT *
    FROM src0
    WHERE PATID IS NOT NULL
      AND admit_date IS NOT NULL
      AND PROVIDERID IS NOT NULL
  ),
  per_type AS (
    SELECT
      ENC_TYPE,
      COUNT(*) AS encounter_records_n,
      COUNT(DISTINCT (PATID || ''|'' || ENC_TYPE || ''|'' || TO_CHAR(admit_date,''YYYY-MM-DD'') || ''|'' || PROVIDERID)) AS visit_distinct_n
    FROM eligible
    GROUP BY ENC_TYPE
  )
  SELECT
    t.enc_type,
    COALESCE(x.total_rows_n, 0) AS total_rows_n,
    COALESCE(p.encounter_records_n, 0) AS encounter_records_n,
    COALESCE(p.visit_distinct_n, 0) AS visit_distinct_n,
    IFF(COALESCE(p.visit_distinct_n,0) > 0,
        COALESCE(p.encounter_records_n,0)::FLOAT / COALESCE(p.visit_distinct_n,0)::FLOAT,
        NULL
    ) AS encounters_per_visit_avg
  FROM types t
  LEFT JOIN totals x ON x.enc_type = t.enc_type
  LEFT JOIN per_type p ON p.enc_type = t.enc_type
  ORDER BY t.enc_type
  `
);
while (rs.next()) {
  const encType = rs.getColumnValue(1);
  const totalRows = Number(rs.getColumnValue(2));
  const recordsN = Number(rs.getColumnValue(3));
  const visitsN = Number(rs.getColumnValue(4));
  const avg = rs.getColumnValue(5) === null ? null : Number(rs.getColumnValue(5));
  const flag = (avg !== null && avg > threshold) ? 1 : 0;
  const details = {
    enc_type: encType,
    total_rows_n: totalRows,
    eligible_rows_n: recordsN,
    excluded_rows_n: totalRows - recordsN,
    visit_distinct_n: visitsN,
    encounters_per_visit_avg: avg,
    threshold: threshold,
    visit_definition: "PATID+ENC_TYPE+ADMIT_DATE+PROVIDERID (ENCOUNTER table)"
  };
  insertMetric(resultsTbl, base, encType, "ENCOUNTER_RECORDS_N", recordsN, String(recordsN), threshold, false, details);
  insertMetric(resultsTbl, base, encType, "VISIT_DISTINCT_N", visitsN, String(visitsN), threshold, false, details);
  insertMetric(resultsTbl, base, encType, "ENCOUNTERS_PER_VISIT_AVG", avg, avg === null ? "NULL" : String(avg), threshold, false, details);
  insertMetric(resultsTbl, base, encType, "ENCOUNTERS_PER_VISIT_FLAG", flag, String(flag), threshold, (flag === 1), details);
}
return `DC 2.04 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';