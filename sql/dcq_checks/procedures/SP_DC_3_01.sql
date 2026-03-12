CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_01"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
      ''DIAGNOSIS'', ?, ?, TRY_TO_NUMBER(?, 38, 10), ?,
      ?, IFF(?=1, TRUE, FALSE), PARSE_JSON(?)
    `,
    baseBinds.concat([codeType, metric, numStr, valueStr, thresholdNum, flagInt, detailsJson])
  );
}
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 3.01;
const only = (TARGET_TABLE || "ALL").toString().trim().toUpperCase();
const threshold = 1.0;
if (!(only === "ALL" || only === "DIAGNOSIS" || only === "ENCOUNTER")) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, DIAGNOSIS, or ENCOUNTER.`);
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
if (!tableExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER", "ENCOUNTERID") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "ENCOUNTER", "ENC_TYPE")) {
  insertMetric(resultsTbl, base, "ALL", "STATUS", null, "ERROR", threshold, true, { message: "ENCOUNTER missing required columns" });
  return "DC 3.01 ERROR: ENCOUNTER missing";
}
if (!tableExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS", "ENCOUNTERID") ||
    !colExists(DB_PARAM, SCHEMA_NAME, "DIAGNOSIS", "DX_TYPE")) {
  insertMetric(resultsTbl, base, "ALL", "STATUS", null, "ERROR", threshold, true, { message: "DIAGNOSIS missing required columns" });
  return "DC 3.01 ERROR: DIAGNOSIS missing";
}
function normDateParam(x) { if (x === null || x === undefined) return null; var v = x.toString().trim(); var u = v.toUpperCase(); return (u === '''' || u === ''NONE'' || u === ''NULL'' || u === ''(NONE)'') ? null : v; }
const vStartDate = normDateParam(START_DATE);
const vEndDate = normDateParam(END_DATE);
const tableDateCol = { ENCOUNTER: ''ADMIT_DATE'', DIAGNOSIS: ''DX_DATE'' };
function dateFilterWhere(tbl, alias) {
  const dc = tableDateCol[tbl] || null;
  if (!dc) return '''';
  const col = alias ? `${alias}.${dc}` : dc;
  let clause = '''';
  if (vStartDate) clause += ` AND TRY_TO_DATE(${col}) >= TRY_TO_DATE(''${vStartDate}'')`;
  if (vEndDate) clause += ` AND TRY_TO_DATE(${col}) <= TRY_TO_DATE(''${vEndDate}'')`;
  return clause;
}
const enc = `${DB_PARAM}.${SCHEMA_NAME}.ENCOUNTER`;
const dia = `${DB_PARAM}.${SCHEMA_NAME}.DIAGNOSIS`;

// Known ENC_TYPE values per PCORnet CDM
const knownEncTypes = [''AV'',''ED'',''EI'',''IC'',''IP'',''IS'',''OA'',''OS'',''TH''];

const rs = q(
  `WITH enc_typed AS (
     SELECT
       ENCOUNTERID,
       UPPER(TRIM(ENC_TYPE)) AS enc_type,
       CASE
         WHEN UPPER(TRIM(ENC_TYPE)) IN (${knownEncTypes.map(t => "''"+t.replace(/''''/g,"")+"''").join(",")})
         THEN UPPER(TRIM(ENC_TYPE))
         ELSE ''Missing/NI/UN/OT''
       END AS enc_type_group
     FROM ${enc}
     WHERE ENCOUNTERID IS NOT NULL${dateFilterWhere(''ENCOUNTER'')}
   ),
   dia_all AS (
     SELECT
       d.ENCOUNTERID,
       e.enc_type_group,
       d.DX_TYPE,
       CASE
         WHEN d.DX_TYPE IS NOT NULL
              AND TRIM(d.DX_TYPE) <> ''''
              AND UPPER(TRIM(d.DX_TYPE)) NOT IN (''NI'',''UN'',''OT'')
         THEN 1 ELSE 0
       END AS is_known_dxtype
     FROM ${dia} d
     JOIN enc_typed e ON d.ENCOUNTERID = e.ENCOUNTERID
     WHERE d.ENCOUNTERID IS NOT NULL${dateFilterWhere(''DIAGNOSIS'', ''d'')}
   ),
   by_group AS (
     SELECT
       enc_type_group,
       COUNT(*)                        AS dx_records_n,
       SUM(is_known_dxtype)            AS dx_known_dxtype_n
     FROM dia_all
     GROUP BY enc_type_group
   ),
   enc_by_group AS (
     SELECT
       enc_type_group,
       COUNT(*)                        AS enc_records_n
     FROM enc_typed
     GROUP BY enc_type_group
   )
   SELECT
     g.enc_type_group,
     COALESCE(b.dx_records_n, 0)        AS dx_records_n,
     g.enc_records_n,
     COALESCE(b.dx_known_dxtype_n, 0)   AS dx_known_dxtype_n
   FROM enc_by_group g
   LEFT JOIN by_group b ON b.enc_type_group = g.enc_type_group
   ORDER BY g.enc_type_group`
);

let totalDxN = 0;
let totalEncN = 0;
let totalDxKnownN = 0;
let wrote = 0;

while (rs.next()) {
  const grp          = rs.getColumnValue(1);
  const dxN          = Number(rs.getColumnValue(2));
  const encN         = Number(rs.getColumnValue(3));
  const dxKnownN     = Number(rs.getColumnValue(4));

  totalDxN      += dxN;
  totalEncN     += encN;
  totalDxKnownN += dxKnownN;

  const dxPerEnc      = (encN > 0) ? dxN / encN : null;
  const dxKnownPerEnc = (encN > 0) ? dxKnownN / encN : null;
  const flag          = (dxKnownPerEnc !== null && dxKnownPerEnc < threshold) ? 1 : 0;

  const details = {
    enc_type_group: grp,
    dx_records_n: dxN,
    enc_records_n: encN,
    dx_per_encounter: dxPerEnc,
    dx_known_dxtype_n: dxKnownN,
    dx_known_dxtype_per_encounter: dxKnownPerEnc,
    threshold_avg_lt: threshold
  };

  insertMetric(resultsTbl, base, grp, "DX_RECORDS_N",                  dxN,          String(dxN),       threshold, false, details);
  insertMetric(resultsTbl, base, grp, "ENC_RECORDS_N",                 encN,         String(encN),      threshold, false, details);
  insertMetric(resultsTbl, base, grp, "DX_PER_ENCOUNTER",              dxPerEnc,     dxPerEnc === null ? null : String(dxPerEnc),           threshold, false, details);
  insertMetric(resultsTbl, base, grp, "DX_KNOWN_DXTYPE_N",             dxKnownN,     String(dxKnownN),  threshold, false, details);
  insertMetric(resultsTbl, base, grp, "DX_KNOWN_DXTYPE_PER_ENCOUNTER", dxKnownPerEnc, dxKnownPerEnc === null ? null : String(dxKnownPerEnc), threshold, false, details);
  insertMetric(resultsTbl, base, grp, "DX_KNOWN_DXTYPE_PER_ENC_FLAG",  flag,         String(flag),      threshold, (flag === 1), details);
  wrote += 1;
}

// Total row
const totalDxPerEnc      = (totalEncN > 0) ? totalDxN / totalEncN : null;
const totalDxKnownPerEnc = (totalEncN > 0) ? totalDxKnownN / totalEncN : null;
const totalFlag          = (totalDxKnownPerEnc !== null && totalDxKnownPerEnc < threshold) ? 1 : 0;
const totalDetails = {
  enc_type_group: "Total",
  dx_records_n: totalDxN,
  enc_records_n: totalEncN,
  dx_per_encounter: totalDxPerEnc,
  dx_known_dxtype_n: totalDxKnownN,
  dx_known_dxtype_per_encounter: totalDxKnownPerEnc,
  threshold_avg_lt: threshold
};

insertMetric(resultsTbl, base, "Total", "DX_RECORDS_N",                  totalDxN,          String(totalDxN),       threshold, false, totalDetails);
insertMetric(resultsTbl, base, "Total", "ENC_RECORDS_N",                 totalEncN,         String(totalEncN),      threshold, false, totalDetails);
insertMetric(resultsTbl, base, "Total", "DX_PER_ENCOUNTER",              totalDxPerEnc,     totalDxPerEnc === null ? null : String(totalDxPerEnc),           threshold, false, totalDetails);
insertMetric(resultsTbl, base, "Total", "DX_KNOWN_DXTYPE_N",             totalDxKnownN,     String(totalDxKnownN),  threshold, false, totalDetails);
insertMetric(resultsTbl, base, "Total", "DX_KNOWN_DXTYPE_PER_ENCOUNTER", totalDxKnownPerEnc, totalDxKnownPerEnc === null ? null : String(totalDxKnownPerEnc), threshold, false, totalDetails);
insertMetric(resultsTbl, base, "Total", "DX_KNOWN_DXTYPE_PER_ENC_FLAG",  totalFlag,         String(totalFlag),      threshold, (totalFlag === 1), totalDetails);

insertMetric(resultsTbl, base, "ALL", "STATUS", null, "OK", threshold, false, { enc_type_groups: wrote });

return `DC 3.01 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} groups=${wrote}`;
';
