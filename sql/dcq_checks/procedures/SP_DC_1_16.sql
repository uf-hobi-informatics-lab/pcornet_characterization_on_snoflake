CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_16"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT='DC 1.16 (Table IIF): misplaced LOINC codes based on LOINC CLASSTYPE. Joins CDM codes to CHARACTERIZATION.EDC_REF.LOINC_REF_RAW and flags mismatches: LAB_RESULT_CM and LAB_HISTORY expect CLASSTYPE=1; OBS_CLIN expects CLASSTYPE=2; OBS_GEN should not use CLASSTYPE=1. TARGET_TABLE may be one of LAB_RESULT_CM, LAB_HISTORY, OBS_CLIN, OBS_GEN, or ALL. Run: CALL CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_16(DB_PARAM, SCHEMA_NAME, RUN_ID, TARGET_TABLE|''ALL''); or driver: CALL CHARACTERIZATION.DCQ.SP_RUN_DCQ(DB_PARAM, SCHEMA_NAME, ''ROW_NUM'', ''1.16'', ''part1'', TARGET_TABLE|''ALL''). Output: <DB_PARAM>.CHARACTERIZATION_DCQ.DCQ_RESULTS where ROW_NUM=1.16. Interpret: metrics RECORDS/MISPLACED_RECORDS/MISPLACED_CODES/MISPLACED_RECORD_PCT are per SOURCE_TABLE; EXCEPTION_FLAG=TRUE when MISPLACED_RECORDS>0.'
EXECUTE AS CALLER
AS '
function q(sqlText, binds) {
  return snowflake.execute({ sqlText, binds: binds || [] });
}
function isSafeIdentPart(s) {
  return /^[A-Za-z0-9_$]+$/.test((s || '''').toString());
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
function insertStatus(resultsTbl, runId, checkId, checkName, rowNum, edcTable, sourceTable, status, msg) {
  const detailsJson = JSON.stringify({ status: status, message: msg });
  q(
    `INSERT INTO ${resultsTbl} (
       RUN_ID, CHECK_ID, CHECK_NAME, ROW_NUM, EDC_TABLE,
       SOURCE_TABLE, CODE_TYPE, METRIC, VALUE_NUM, VALUE_STR,
       THRESHOLD_NUM, EXCEPTION_FLAG, DETAILS
     )
     SELECT
       ?, ?, ?, ?, ?,
       ?, ''LOINC'', ''STATUS'', NULL, ?,
       0,
       IFF(?=''ERROR'', TRUE, FALSE),
       PARSE_JSON(?)
    `,
    [runId, checkId, checkName, rowNum, edcTable, sourceTable, status, status, detailsJson]
  );
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 1.16;
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
q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
const loincRef = `CHARACTERIZATION.EDC_REF.LOINC_REF_RAW`;
function insertFor(tableName, idCol, loincCol, expectedLabel, mismatchPredicateSql) {
  const fullTable = `${DB_PARAM}.${SCHEMA_NAME}.${tableName}`;
  const sql = `
    INSERT INTO ${resultsTbl} (
      RUN_ID, CHECK_ID, CHECK_NAME, ROW_NUM, EDC_TABLE,
      SOURCE_TABLE, CODE_TYPE, METRIC, VALUE_NUM, VALUE_STR,
      THRESHOLD_NUM, EXCEPTION_FLAG, DETAILS
    )
    WITH base AS (
      SELECT ${idCol} AS record_id,
             UPPER(REGEXP_REPLACE(${loincCol}, ''[.,\\\\\\\\s]'', '''')) AS code_clean
      FROM ${fullTable}
      WHERE ${loincCol} IS NOT NULL
    ),
    joined AS (
      SELECT b.record_id, b.code_clean,
             TRIM(r.CLASSTYPE) AS classtype,
             r.LONG_COMMON_NAME AS code_desc
      FROM base b
      JOIN ${loincRef} r
        ON b.code_clean = UPPER(TRIM(r.LOINC_NUM))
      WHERE ${mismatchPredicateSql}
    ),
    denom AS (SELECT COUNT(*) AS records FROM base),
    numer AS (SELECT COUNT(*) AS misplaced_records, COUNT(DISTINCT code_clean) AS misplaced_codes FROM joined)
    SELECT
      ?,
      ?, ?, ?, ?,
      ''${tableName}'', ''LOINC'',
      metric,
      value_num::NUMBER(38,10),
      value_str,
      0::NUMBER(38,10),
      IFF(misplaced_records > 0, TRUE, FALSE),
      OBJECT_CONSTRUCT(
        ''expected_classtype'', ''${expectedLabel}'',
        ''records'', records,
        ''misplaced_records'', misplaced_records,
        ''misplaced_codes'', misplaced_codes
      )
    FROM (
      SELECT d.records, n.misplaced_records, n.misplaced_codes,
             ''RECORDS'' AS metric, d.records::NUMBER AS value_num, d.records::STRING AS value_str
      FROM denom d CROSS JOIN numer n
      UNION ALL
      SELECT d.records, n.misplaced_records, n.misplaced_codes,
             ''MISPLACED_RECORDS'', n.misplaced_records::NUMBER, n.misplaced_records::STRING
      FROM denom d CROSS JOIN numer n
      UNION ALL
      SELECT d.records, n.misplaced_records, n.misplaced_codes,
             ''MISPLACED_CODES'', n.misplaced_codes::NUMBER, n.misplaced_codes::STRING
      FROM denom d CROSS JOIN numer n
      UNION ALL
      SELECT d.records, n.misplaced_records, n.misplaced_codes,
             ''MISPLACED_RECORD_PCT'',
             IFF(d.records > 0, (n.misplaced_records::FLOAT / d.records::FLOAT) * 100, 0)::NUMBER,
             IFF(d.records > 0, ((n.misplaced_records::FLOAT / d.records::FLOAT) * 100)::STRING, ''0'')
      FROM denom d CROSS JOIN numer n
    );
  `;
  q(sql, [RUN_ID, checkId, checkName, rowNum, edcTable]);
}
const targets = [
  { t: "LAB_RESULT_CM", id: "LAB_RESULT_CM_ID", col: "LAB_LOINC",     exp: "1",     where: "TRIM(r.CLASSTYPE) <> ''1''" },
  { t: "LAB_HISTORY",   id: "LABHISTORYID",     col: "LAB_LOINC",     exp: "1",     where: "TRIM(r.CLASSTYPE) <> ''1''" },
  { t: "OBS_CLIN",      id: "OBSCLINID",        col: "OBSCLIN_CODE",  exp: "2",     where: "TRIM(r.CLASSTYPE) <> ''2''" },
  { t: "OBS_GEN",       id: "OBSGENID",         col: "OBSGEN_CODE",   exp: "2/3/4", where: "TRIM(r.CLASSTYPE) = ''1''" }
];
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
const filtered = (only === "ALL") ? targets : targets.filter(x => x.t === only);
if (filtered.length === 0) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, LAB_RESULT_CM, LAB_HISTORY, OBS_CLIN, or OBS_GEN.`);
}
for (const x of filtered) {
  try {
    if (!tableExists(DB_PARAM, SCHEMA_NAME, x.t)) {
      insertStatus(resultsTbl, RUN_ID, checkId, checkName, rowNum, edcTable, x.t, "SKIPPED", "Table does not exist");
      continue;
    }
    insertFor(x.t, x.id, x.col, x.exp, x.where);
  } catch (e) {
    const msg = (e && e.message) ? e.message.toString().slice(0, 2000) : "Unknown error";
    insertStatus(resultsTbl, RUN_ID, checkId, checkName, rowNum, edcTable, x.t, "ERROR", msg);
  }
}
return `DC 1.16 finished for RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';