CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_05"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
function q(sqlText, binds) { return snowflake.execute({ sqlText, binds: binds || [] }); }
function isSafeIdentPart(s) { return /^[A-Za-z0-9_$]+$/.test((s || '''').toString()); }
function insertMetric(resultsTbl, baseBinds, metric, valueNum, valueStr, exceptionFlag, detailsObj) {
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
    baseBinds.concat([metric, valueNum, valueStr, flagInt, detailsJson])
  );
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
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);
function normDateParam(x) { if (x === null || x === undefined) return null; var v = x.toString().trim(); var u = v.toUpperCase(); return (u === '''' || u === ''NONE'' || u === ''NULL'' || u === ''(NONE)'') ? null : v; }
const vStartDate = normDateParam(START_DATE);
const vEndDate = normDateParam(END_DATE);
const tableDateCol = {
  CONDITION: ''REPORT_DATE'',
  DEATH: ''DEATH_DATE'',
  DEMOGRAPHIC: null,
  DIAGNOSIS: ''DX_DATE'',
  DISPENSING: ''DISPENSE_DATE'',
  ENCOUNTER: ''ADMIT_DATE'',
  ENROLLMENT: ''ENR_START_DATE'',
  EXTERNAL_MEDS: ''EXT_RECORD_DATE'',
  HARVEST: null,
  HASH_TOKEN: null,
  IMMUNIZATION: ''VX_RECORD_DATE'',
  LAB_HISTORY: null,
  LAB_RESULT_CM: ''RESULT_DATE'',
  LDS_ADDRESS_HISTORY: null,
  MED_ADMIN: ''MEDADMIN_START_DATE'',
  OBS_CLIN: ''OBSCLIN_START_DATE'',
  OBS_GEN: ''OBSGEN_START_DATE'',
  PAT_RELATIONSHIP: null,
  PCORNET_TRIAL: null,
  PRESCRIBING: ''RX_ORDER_DATE'',
  PROCEDURES: ''PX_DATE'',
  PROVIDER: null,
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
const rowNum = 1.05;
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
// PK definitions (your provided mapping)
const defs = [
  { table: "DEMOGRAPHIC", cols: ["PATID"] },
  { table: "ENROLLMENT", cols: ["PATID","ENR_START_DATE","ENR_BASIS"] },
  { table: "ENCOUNTER", cols: ["ENCOUNTERID"] },
  { table: "DIAGNOSIS", cols: ["DIAGNOSISID"] },
  { table: "PROCEDURES", cols: ["PROCEDURESID"] },
  { table: "VITAL", cols: ["VITALID"] },
  { table: "DISPENSING", cols: ["DISPENSINGID"] },
  { table: "LAB_RESULT_CM", cols: ["LAB_RESULT_CM_ID"] },
  { table: "CONDITION", cols: ["CONDITIONID"] },
  { table: "PRO_CM", cols: ["PRO_CM_ID"] },
  { table: "PRESCRIBING", cols: ["PRESCRIBINGID"] },
  { table: "PCORNET_TRIAL", cols: ["PATID","TRIALID","PARTICIPANTID"] },
  { table: "DEATH", cols: ["PATID","DEATH_SOURCE"] },
  { table: "DEATH_CAUSE", cols: ["PATID","DEATH_CAUSE","DEATH_CAUSE_CODE","DEATH_CAUSE_TYPE","DEATH_CAUSE_SOURCE"] },
  { table: "MED_ADMIN", cols: ["MEDADMINID"] },
  { table: "PROVIDER", cols: ["PROVIDERID"] },
  { table: "OBS_CLIN", cols: ["OBSCLINID"] },
  { table: "OBS_GEN", cols: ["OBSGENID"] },
  { table: "HASH_TOKEN", cols: ["PATID","TOKEN_ENCRYPTION_KEY"] },
  { table: "LDS_ADDRESS_HISTORY", cols: ["ADDRESSID"] },
  { table: "IMMUNIZATION", cols: ["IMMUNIZATIONID"] },
  { table: "LAB_HISTORY", cols: ["LABHISTORYID"] },
  { table: "PAT_RELATIONSHIP", cols: ["PATID_1","PATID_2","RELATIONSHIP_TYPE"] },
  { table: "EXTERNAL_MEDS", cols: ["EXTMEDID"] }
];
let targets = defs;
if (only !== "ALL") {
  targets = defs.filter(d => d.table === only);
  if (targets.length === 0) {
    throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or one of the CDM table names in the PK map.`);
  }
}
for (const d of targets) {
  const t = d.table;
  const exists = tableExists(DB_PARAM, SCHEMA_NAME, t);
  const base = [RUN_ID, checkId, checkName, rowNum, edcTable, t];
  if (!exists) {
    const details = { table_exists: false, pk_columns: d.cols };
    insertMetric(resultsTbl, base, "STATUS", null, "SKIPPED", false, details);
    continue;
  }
  const fullTable = `${DB_PARAM}.${SCHEMA_NAME}.${t}`;
  const nonNullPred = d.cols.map(c => `${c} IS NOT NULL`).join(" AND ");
  const pkExpr = d.cols.map(c => `COALESCE(TO_VARCHAR(${c}), '''')`).join(` || ''|'' || `);
  const sql = `
    WITH base AS (
      SELECT
        (${nonNullPred}) AS pk_complete,
        ${pkExpr} AS pk_key
      FROM ${fullTable}
      WHERE 1=1 ${dateFilterWhere(t)}
    ),
    agg AS (
      SELECT
        COUNT(*) AS row_count,
        SUM(IFF(pk_complete, 1, 0)) AS pk_non_null_count,
        COUNT(DISTINCT IFF(pk_complete, pk_key, NULL)) AS pk_distinct_count
      FROM base
    )
    SELECT row_count, pk_non_null_count, pk_distinct_count,
           (pk_non_null_count - pk_distinct_count) AS pk_duplicate_row_count
    FROM agg
  `;
  const rs = q(sql);
  rs.next();
  const rowCount = Number(rs.getColumnValue(1));
  const pkNonNull = Number(rs.getColumnValue(2));
  const pkDistinct = Number(rs.getColumnValue(3));
  const pkDupRows = Number(rs.getColumnValue(4));
  const details = {
    table_exists: true,
    pk_columns: d.cols,
    row_count: rowCount,
    pk_non_null_count: pkNonNull,
    pk_distinct_count: pkDistinct,
    pk_duplicate_row_count: pkDupRows
  };
  insertMetric(resultsTbl, base, "ROW_COUNT", rowCount, String(rowCount), false, details);
  insertMetric(resultsTbl, base, "PK_NON_NULL_COUNT", pkNonNull, String(pkNonNull), false, details);
  insertMetric(resultsTbl, base, "PK_DISTINCT_COUNT", pkDistinct, String(pkDistinct), false, details);
  insertMetric(resultsTbl, base, "PK_DUPLICATE_ROW_COUNT", pkDupRows, String(pkDupRows), false, details);
  const dupFlag = pkDupRows > 0 ? 1 : 0;
  insertMetric(resultsTbl, base, "PK_DUPLICATE_FLAG", dupFlag, String(dupFlag), (dupFlag === 1), details);
}
return `DC 1.05 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';