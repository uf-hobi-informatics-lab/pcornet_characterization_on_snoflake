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
  const numStr = (valueNum === null || valueNum === undefined) ? null : String(valueNum);
  q(
    `INSERT INTO ${resultsTbl} (
      RUN_ID, CHECK_ID, CHECK_NAME, ROW_NUM, EDC_TABLE,
      SOURCE_TABLE, CODE_TYPE, METRIC, VALUE_NUM, VALUE_STR,
      THRESHOLD_NUM, EXCEPTION_FLAG, DETAILS
    )
    SELECT
      ?, ?, ?, ?, ?,
      ''ENCOUNTER'', ?, ?, TRY_TO_NUMBER(?, 38, 10), ?,
      ?, IFF(?=1, TRUE, FALSE), PARSE_JSON(?)
    `,
    baseBinds.concat([codeType, metric, numStr, valueStr, thresholdNum, flagInt, detailsJson])
  );
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
function normDateParam(x) { if (x === null || x === undefined) return null; var v = x.toString().trim(); var u = v.toUpperCase(); return (u === '''' || u === ''NONE'' || u === ''NULL'' || u === ''(NONE)'') ? null : v; }
const vStartDate = normDateParam(START_DATE);
const vEndDate = normDateParam(END_DATE);
const tableDateCol = { ENCOUNTER: ''ADMIT_DATE'' };
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
  return "DC 2.04 ERROR: ENCOUNTER missing";
}
for (const c of ["PATID","ENC_TYPE","ADMIT_DATE","PROVIDERID"]) {
  if (!colExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER", c)) {
    insertMetric(resultsTbl, base, "ALL", "STATUS", null, "ERROR", threshold, true, { message: "Missing required column", missing_column: c });
    return `DC 2.04 ERROR: missing ENCOUNTER.${c}`;
  }
}
const fullEnc = `${DB_PARAM}.${SCHEMA_NAME}.ENCOUNTER`;

// Compute all metrics per ENC_TYPE in a single query
const rs = q(
  `WITH src AS (
     SELECT
       UPPER(TRIM(ENC_TYPE)) AS enc_type,
       PATID,
       TRY_TO_DATE(ADMIT_DATE) AS admit_date,
       PROVIDERID
     FROM ${fullEnc}
     WHERE ENC_TYPE IS NOT NULL
       AND TRIM(ENC_TYPE) <> ''''${dateFilterWhere(''ENCOUNTER'')}
   ),
   per_type AS (
     SELECT
       enc_type,
       COUNT(*)                                           AS encounters_n,
       COUNT(DISTINCT PATID)                              AS patients_n,
       COUNT_IF(PROVIDERID IS NOT NULL
                AND TRIM(PROVIDERID::STRING) <> '''')     AS enc_known_provider_n,
       COUNT(DISTINCT
         CASE WHEN PATID IS NOT NULL
                   AND admit_date IS NOT NULL
                   AND PROVIDERID IS NOT NULL
                   AND TRIM(PROVIDERID::STRING) <> ''''
              THEN PATID || ''|'' || enc_type || ''|'' || TO_CHAR(admit_date,''YYYY-MM-DD'') || ''|'' || PROVIDERID
         END
       )                                                  AS visit_distinct_n
     FROM src
     GROUP BY enc_type
   )
   SELECT
     enc_type,
     encounters_n,
     patients_n,
     IFF(patients_n > 0, encounters_n::DOUBLE / patients_n::DOUBLE, NULL)                    AS enc_per_patient,
     enc_known_provider_n,
     visit_distinct_n,
     IFF(visit_distinct_n > 0, enc_known_provider_n::DOUBLE / visit_distinct_n::DOUBLE, NULL) AS enc_known_prov_per_visit
   FROM per_type
   ORDER BY enc_type`
);

let wrote = 0;
while (rs.next()) {
  const encType         = rs.getColumnValue(1);
  const encountersN     = Number(rs.getColumnValue(2));
  const patientsN       = Number(rs.getColumnValue(3));
  const encPerPatient   = rs.getColumnValue(4) === null ? null : Number(rs.getColumnValue(4));
  const encKnownProvN   = Number(rs.getColumnValue(5));
  const visitDistinctN  = Number(rs.getColumnValue(6));
  const encProvPerVisit = rs.getColumnValue(7) === null ? null : Number(rs.getColumnValue(7));

  const flag = (encProvPerVisit !== null && encProvPerVisit > threshold) ? 1 : 0;

  const details = {
    enc_type: encType,
    encounters_n: encountersN,
    patients_n: patientsN,
    enc_per_patient: encPerPatient,
    enc_known_provider_n: encKnownProvN,
    visit_distinct_n: visitDistinctN,
    enc_known_prov_per_visit: encProvPerVisit,
    threshold: threshold,
    visit_definition: "Unique combinations of PATID + ENC_TYPE + ADMIT_DATE + PROVIDERID"
  };

  insertMetric(resultsTbl, base, encType, "ENCOUNTERS_N",               encountersN,    String(encountersN),    threshold, false, details);
  insertMetric(resultsTbl, base, encType, "PATIENTS_N",                 patientsN,      String(patientsN),      threshold, false, details);
  insertMetric(resultsTbl, base, encType, "ENC_PER_PATIENT",            encPerPatient,  encPerPatient === null ? null : String(encPerPatient), threshold, false, details);
  insertMetric(resultsTbl, base, encType, "ENC_KNOWN_PROVIDER_N",       encKnownProvN,  String(encKnownProvN),  threshold, false, details);
  insertMetric(resultsTbl, base, encType, "VISIT_DISTINCT_N",           visitDistinctN, String(visitDistinctN), threshold, false, details);
  insertMetric(resultsTbl, base, encType, "ENC_KNOWN_PROV_PER_VISIT",   encProvPerVisit, encProvPerVisit === null ? null : String(encProvPerVisit), threshold, false, details);
  insertMetric(resultsTbl, base, encType, "ENC_KNOWN_PROV_PER_VISIT_FLAG", flag, String(flag), threshold, (flag === 1), details);
  wrote += 1;
}

if (wrote === 0) {
  insertMetric(resultsTbl, base, "ALL", "STATUS", null, "OK", threshold, false, { note: "No encounter types found" });
} else {
  insertMetric(resultsTbl, base, "ALL", "STATUS", null, "OK", threshold, false, { enc_types_evaluated: wrote });
}

return `DC 2.04 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} enc_types=${wrote}`;
';
