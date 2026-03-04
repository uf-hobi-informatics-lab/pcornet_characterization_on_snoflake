CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_11"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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

const vStartDate = (START_DATE || '').toString().trim() || null;
const vEndDate = (END_DATE || '').toString().trim() || null;
function dateFilter(colName) {
  let clause = '';
  if (vStartDate) clause += ` AND TRY_TO_DATE(${colName}) >= TRY_TO_DATE(''${vStartDate}'')`;
  if (vEndDate) clause += ` AND TRY_TO_DATE(${colName}) <= TRY_TO_DATE(''${vEndDate}'')`;
  return clause;
}

const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 3.11;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();
if (only !== ''ALL'' && !isSafeIdentPart(only)) throw new Error(`Invalid TARGET_TABLE: ${TARGET_TABLE}`);

// SAS Table IVG: check Month -3 only, flag if count=0 or <75% of benchmark average (months -12..-23 avg).
const thresholdPctLt = 75.0;
const checkOrd = 3;

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

// Anchor date: current date at time of run.
const anchorDateStr = scalar("SELECT TO_VARCHAR(CURRENT_DATE(), ''YYYY-MM-DD'')");

const defs = [
  { table: ''VITAL'', dateCol: ''MEASURE_DATE'' },
  { table: ''PRESCRIBING'', dateCol: ''RX_ORDER_DATE'' },
  { table: ''LAB_RESULT_CM'', dateCol: ''RESULT_DATE'' }
];

let anyFlag = 0;
const evaluated = [];
const skipped = [];

for (const d of defs) {
  if (!(only === ''ALL'' || only === d.table)) continue;
  if (!tableExists(DB_PARAM, SCHEMA_NAME, d.table) || !colExists(DB_PARAM, SCHEMA_NAME, d.table, d.dateCol)) {
    skipped.push({ table: d.table, reason: ''missing_table_or_date_column'', date_col: d.dateCol });
    continue;
  }

  const fq = `${DB_PARAM}.${SCHEMA_NAME}.${d.table}`;
  const sSql = `SELECT DATE_TRUNC(''MONTH'', ${d.dateCol})::DATE AS month, COUNT(*)::NUMBER AS n
                FROM ${fq}
                WHERE ${d.dateCol} IS NOT NULL
                GROUP BY 1`;

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
         SUM(IFF(ord = ${checkOrd}, n, 0)) AS chk_n
       FROM filled
     )
     SELECT
       bench_sum,
       IFF(bench_sum IS NOT NULL, bench_sum / 12.0, NULL) AS bench_avg,
       chk_n,
       IFF(bench_sum > 0, (chk_n / (bench_sum / 12.0)) * 100.0, NULL) AS pct_of_bench
     FROM bench`,
    [anchorDateStr]
  );
  rs.next();

  const benchSum = Number(rs.getColumnValue(1));
  const benchAvg = (rs.getColumnValue(2) === null) ? null : Number(rs.getColumnValue(2));
  const chkN = Number(rs.getColumnValue(3));
  const pct = (rs.getColumnValue(4) === null) ? null : Number(rs.getColumnValue(4));

  const flag = (chkN === 0) || (pct !== null && pct < thresholdPctLt);
  if (flag) anyFlag = 1;

  const details = {
    anchor_date: anchorDateStr,
    month_checked_ord: checkOrd,
    month_checked: scalar(`SELECT TO_VARCHAR(DATEADD(MONTH, -${checkOrd}, DATE_TRUNC(''MONTH'', TO_DATE(?)))::DATE, ''YYYY-MM-DD'')`, [anchorDateStr]),
    benchmark_months_ord: ''12..23'',
    benchmark_sum_records: benchSum,
    benchmark_avg_records: benchAvg,
    month_checked_records: chkN,
    pct_of_benchmark_avg: pct,
    threshold_pct_lt: thresholdPctLt,
    definition: "Month -3 flags if record count is 0 or <75% of benchmark average (benchmark = total records in months -12..-23 / 12)."
  };

  insertMetric(resultsTbl, bindsBase, d.table, d.table, ''ALL'', ''MONTH_MINUS3_RECORD_N'', chkN, String(chkN), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, d.table, d.table, ''ALL'', ''BENCHMARK_AVG_RECORD_N'', benchAvg, (benchAvg === null ? null : String(benchAvg)), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, d.table, d.table, ''ALL'', ''PCT_OF_BENCHMARK_AVG'', pct, (pct === null ? null : String(pct)), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, d.table, d.table, ''ALL'', ''DECREASE_FLAG'', (flag ? 1 : 0), String(flag ? 1 : 0), thresholdPctLt, flag, details);

  evaluated.push(d.table);
}

insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''ANY_DECREASE_FLAG'', anyFlag, String(anyFlag), thresholdPctLt, (anyFlag === 1),
  { anchor_date: anchorDateStr, threshold_pct_lt: thresholdPctLt, month_checked_ord: checkOrd, evaluated_series: evaluated, skipped: skipped }
);
insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''OK'', thresholdPctLt, false,
  { target_table: only, anchor_date: anchorDateStr, evaluated_series: evaluated, skipped: skipped }
);

return `DC 3.11 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';