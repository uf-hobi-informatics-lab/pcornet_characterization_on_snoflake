CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_07"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
function q(sqlText, binds) { return snowflake.execute({ sqlText, binds: binds || [] }); }
function scalar(sqlText, binds) { const rs = q(sqlText, binds); rs.next(); return rs.getColumnValue(1); }
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

function insertMetric(resultsTbl, bindsBase, edcTableVal, sourceTableVal, codeTypeVal, metric, valueNum, valueStr, thresholdNum, exceptionFlag, detailsObj) {
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
      ?, IFF(?=1, TRUE, FALSE), PARSE_JSON(?)`,
    bindsBase.concat([
      edcTableVal,
      sourceTableVal,
      codeTypeVal,
      metric,
      valueNum,
      valueStr,
      thresholdNum,
      flagInt,
      detailsJson
    ])
  );
}

if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);

function normDateParam(x) { if (x === null || x === undefined) return null; var v = x.toString().trim(); var u = v.toUpperCase(); return (u === '''' || u === ''NONE'' || u === ''NULL'' || u === ''(NONE)'') ? null : v; }
const vStartDate = normDateParam(START_DATE);
const vEndDate = normDateParam(END_DATE);
const tableDateCol = {
  ENCOUNTER: ''ADMIT_DATE'',
  DIAGNOSIS: ''ADMIT_DATE'',
  PROCEDURES: ''ADMIT_DATE''
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
const rowNum = 3.07;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();
if (!(only === ''ALL'' || only === ''ENCOUNTER'' || only === ''DIAGNOSIS'' || only === ''PROCEDURES'')) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, ENCOUNTER, DIAGNOSIS, or PROCEDURES.`);
}

// SAS Table IVF: check Month -2 only, flag if count=0 or <75% of benchmark average (prior 12 months, months -12..-23).
const thresholdPctLt = 75.0;
const encTypes = "(''AV'',''ED'',''EI'',''IP'',''TH'')";

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
const bindsBase = [RUN_ID, checkId, checkName, rowNum];

q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);

// Anchor date: current date at time of run (per user).
const anchorDateStr = scalar("SELECT TO_VARCHAR(CURRENT_DATE(), ''YYYY-MM-DD'')");

// Prereqs
const encOk = tableExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'') &&
  colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ENCOUNTERID'') &&
  colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ENC_TYPE'') &&
  colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ADMIT_DATE'');
if (!encOk) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ENCOUNTER'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    { message: ''ENCOUNTER missing required columns (ENCOUNTERID, ENC_TYPE, ADMIT_DATE)'' });
  return ''DC 3.07 ERROR: ENCOUNTER missing required columns'';
}

const haveDiag = tableExists(DB_PARAM, SCHEMA_NAME, ''DIAGNOSIS'') && colExists(DB_PARAM, SCHEMA_NAME, ''DIAGNOSIS'', ''ENCOUNTERID'');
const havePro = tableExists(DB_PARAM, SCHEMA_NAME, ''PROCEDURES'') && colExists(DB_PARAM, SCHEMA_NAME, ''PROCEDURES'', ''ENCOUNTERID'');

const encFq = `${DB_PARAM}.${SCHEMA_NAME}.ENCOUNTER`;
const diaFq = `${DB_PARAM}.${SCHEMA_NAME}.DIAGNOSIS`;
const proFq = `${DB_PARAM}.${SCHEMA_NAME}.PROCEDURES`;

// Prefer replicated ENCOUNTER columns in DIAGNOSIS/PROCEDURES if present (like SAS normalization does).
const diagHasRep = haveDiag && colExists(DB_PARAM, SCHEMA_NAME, ''DIAGNOSIS'', ''ADMIT_DATE'') && colExists(DB_PARAM, SCHEMA_NAME, ''DIAGNOSIS'', ''ENC_TYPE'');
const proHasRep = havePro && colExists(DB_PARAM, SCHEMA_NAME, ''PROCEDURES'', ''ADMIT_DATE'') && colExists(DB_PARAM, SCHEMA_NAME, ''PROCEDURES'', ''ENC_TYPE'');

function seriesSql(seriesName) {
  if (seriesName === ''ENCOUNTER'') {
    return `SELECT DATE_TRUNC(''MONTH'', e.ADMIT_DATE)::DATE AS month, COUNT(*)::NUMBER AS n
            FROM ${encFq} e
            WHERE e.ADMIT_DATE IS NOT NULL
              AND e.ENC_TYPE IS NOT NULL
              AND UPPER(TRIM(e.ENC_TYPE::STRING)) IN ${encTypes}
              ${dateFilterWhere(''ENCOUNTER'')}
            GROUP BY 1`;
  }
  if (seriesName === ''DIAGNOSIS'') {
    if (!haveDiag) return null;
    if (diagHasRep) {
      return `SELECT DATE_TRUNC(''MONTH'', d.ADMIT_DATE)::DATE AS month, COUNT(*)::NUMBER AS n
              FROM ${diaFq} d
              WHERE d.ADMIT_DATE IS NOT NULL
                AND d.ENC_TYPE IS NOT NULL
                AND UPPER(TRIM(d.ENC_TYPE::STRING)) IN ${encTypes}
                ${dateFilterWhere(''DIAGNOSIS'')}
              GROUP BY 1`;
    }
    return `SELECT DATE_TRUNC(''MONTH'', e.ADMIT_DATE)::DATE AS month, COUNT(*)::NUMBER AS n
            FROM ${diaFq} d
            JOIN ${encFq} e
              ON d.ENCOUNTERID = e.ENCOUNTERID
            WHERE e.ADMIT_DATE IS NOT NULL
              AND e.ENC_TYPE IS NOT NULL
              AND UPPER(TRIM(e.ENC_TYPE::STRING)) IN ${encTypes}
              ${dateFilterWhere(''ENCOUNTER'')}
            GROUP BY 1`;
  }
  if (seriesName === ''PROCEDURES'') {
    if (!havePro) return null;
    if (proHasRep) {
      return `SELECT DATE_TRUNC(''MONTH'', p.ADMIT_DATE)::DATE AS month, COUNT(*)::NUMBER AS n
              FROM ${proFq} p
              WHERE p.ADMIT_DATE IS NOT NULL
                AND p.ENC_TYPE IS NOT NULL
                AND UPPER(TRIM(p.ENC_TYPE::STRING)) IN ${encTypes}
                ${dateFilterWhere(''PROCEDURES'')}
              GROUP BY 1`;
    }
    return `SELECT DATE_TRUNC(''MONTH'', e.ADMIT_DATE)::DATE AS month, COUNT(*)::NUMBER AS n
            FROM ${proFq} p
            JOIN ${encFq} e
              ON p.ENCOUNTERID = e.ENCOUNTERID
            WHERE e.ADMIT_DATE IS NOT NULL
              AND e.ENC_TYPE IS NOT NULL
              AND UPPER(TRIM(e.ENC_TYPE::STRING)) IN ${encTypes}
              ${dateFilterWhere(''ENCOUNTER'')}
            GROUP BY 1`;
  }
  return null;
}

const series = [];
if (only === ''ALL'' || only === ''ENCOUNTER'') series.push(''ENCOUNTER'');
if ((only === ''ALL'' || only === ''DIAGNOSIS'') && haveDiag) series.push(''DIAGNOSIS'');
if ((only === ''ALL'' || only === ''PROCEDURES'') && havePro) series.push(''PROCEDURES'');

if (series.length === 0) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''OK'', thresholdPctLt, false,
    { message: ''No eligible series (missing tables) for evaluation.'', target_table: only });
  return `DC 3.07 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
}

let anyFlag = 0;

for (const s of series) {
  const sSql = seriesSql(s);
  if (!sSql) continue;

  const rs = q(
    `WITH anchor AS (
       SELECT DATE_TRUNC(''MONTH'', TO_DATE(?))::DATE AS anchor_month
     ),
     months AS (
       SELECT
         seq4() AS ord,
         DATEADD(MONTH, -seq4(), (SELECT anchor_month FROM anchor))::DATE AS month
       FROM TABLE(GENERATOR(ROWCOUNT => 24))
     ),
     counts AS (
       ${sSql}
     ),
     filled AS (
       SELECT
         m.ord,
         m.month,
         COALESCE(c.n, 0) AS n
       FROM months m
       LEFT JOIN counts c
         ON c.month = m.month
     ),
     bench AS (
       SELECT
         SUM(IFF(ord BETWEEN 12 AND 23, n, 0)) AS bench_sum,
         SUM(IFF(ord = 2, n, 0)) AS m2_n
       FROM filled
     )
     SELECT
       bench_sum,
       IFF(bench_sum IS NOT NULL, bench_sum / 12.0, NULL) AS bench_avg,
       m2_n,
       IFF(bench_sum > 0, (m2_n / (bench_sum / 12.0)) * 100.0, NULL) AS pct_of_bench
     FROM bench`,
    [anchorDateStr]
  );
  rs.next();
  const benchSum = Number(rs.getColumnValue(1));
  const benchAvg = (rs.getColumnValue(2) === null) ? null : Number(rs.getColumnValue(2));
  const m2 = Number(rs.getColumnValue(3));
  const pct = (rs.getColumnValue(4) === null) ? null : Number(rs.getColumnValue(4));

  const flag = (m2 === 0) || (pct !== null && pct < thresholdPctLt);
  if (flag) anyFlag = 1;

  const details = {
    anchor_date: anchorDateStr,
    month_minus_2: scalar("SELECT TO_VARCHAR(DATEADD(MONTH, -2, DATE_TRUNC(''MONTH'', TO_DATE(?)))::DATE, ''YYYY-MM-DD'')", [anchorDateStr]),
    benchmark_months_ord: ''12..23'',
    benchmark_sum_records: benchSum,
    benchmark_avg_records: benchAvg,
    month_minus_2_records: m2,
    pct_of_benchmark_avg: pct,
    threshold_pct_lt: thresholdPctLt,
    definition: "Month -2 flags if record count is 0 or <75% of benchmark average (benchmark = total records in months -12..-23 / 12).",
    enc_types: [''AV'',''ED'',''EI'',''IP'',''TH'']
  };

  insertMetric(resultsTbl, bindsBase, s, s, ''ALL'', ''MONTH_MINUS2_RECORD_N'', m2, String(m2), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, s, s, ''ALL'', ''BENCHMARK_AVG_RECORD_N'', benchAvg, (benchAvg === null ? null : String(benchAvg)), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, s, s, ''ALL'', ''PCT_OF_BENCHMARK_AVG'', pct, (pct === null ? null : String(pct)), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, s, s, ''ALL'', ''DECREASE_FLAG'', (flag ? 1 : 0), String(flag ? 1 : 0), thresholdPctLt, flag, details);
}

insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''ANY_DECREASE_FLAG'', anyFlag, String(anyFlag), thresholdPctLt, (anyFlag === 1),
  { anchor_date: anchorDateStr, threshold_pct_lt: thresholdPctLt, evaluated_series: series }
);

insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''OK'', thresholdPctLt, false,
  { target_table: only, anchor_date: anchorDateStr, evaluated_series: series }
);

return `DC 3.07 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';