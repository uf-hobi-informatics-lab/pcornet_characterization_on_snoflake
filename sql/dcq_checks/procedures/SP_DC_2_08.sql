CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_2_08"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
const vStartDate = (START_DATE || '''').toString().trim() || null;
const vEndDate = (END_DATE || '''').toString().trim() || null;
function dateFilter(colName) {
  let clause = '''';
  if (vStartDate) clause += ` AND TRY_TO_DATE(${colName}) >= TRY_TO_DATE(''${vStartDate}'')`;
  if (vEndDate) clause += ` AND TRY_TO_DATE(${colName}) <= TRY_TO_DATE(''${vEndDate}'')`;
  return clause;
}

const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 2.08;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();
if (only !== ''ALL'' && !isSafeIdentPart(only)) throw new Error(`Invalid TARGET_TABLE: ${TARGET_TABLE}`);

// SAS Table IIIG parameters
const nback = 12;
const nsd = -7;
const minLagAvg = 500;

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

// Determine response_date. SAS uses XTBL_L3_METADATA.RESPONSE_DATE.
// We approximate using CURRENT_DATE and bind as a YYYY-MM-DD string (avoid JS Date bind issues).
const responseDateStr = scalar("SELECT TO_VARCHAR(CURRENT_DATE(), ''YYYY-MM-DD'')");

// Series definitions for monthly counts.
// For ENCOUNTER/DIAGNOSIS/PROCEDURES we stratify by ENC_TYPE and month of ADMIT_DATE.
// For other tables we use the primary date field and category=''n/a''.
const defs = [
  { table: ''ENCOUNTER'', kind: ''enc'', dateCol: ''ADMIT_DATE'', catCol: ''ENC_TYPE'' },
  { table: ''DIAGNOSIS'', kind: ''enc_join'', dateCol: ''ADMIT_DATE'', catCol: ''ENC_TYPE'' },
  { table: ''PROCEDURES'', kind: ''enc_join'', dateCol: ''ADMIT_DATE'', catCol: ''ENC_TYPE'' },
  { table: ''VITAL'', kind: ''date_only'', dateCol: ''MEASURE_DATE'' },
  { table: ''PRESCRIBING'', kind: ''date_only'', dateCol: ''RX_ORDER_DATE'' },
  { table: ''LAB_RESULT_CM'', kind: ''date_only'', dateCol: ''RESULT_DATE'' },
  { table: ''MED_ADMIN'', kind: ''date_only'', dateCol: ''MEDADMIN_START_DATE'' },
  { table: ''EXTERNAL_MEDS'', kind: ''date_only'', dateCol: ''EXT_RECORD_DATE'' },
  { table: ''PRO_CM'', kind: ''date_only'', dateCol: ''PRO_DATE'' }
];

const encTableOk = tableExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'') &&
  colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ENCOUNTERID'') &&
  colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ENC_TYPE'') &&
  colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ADMIT_DATE'');

const seriesParts = [];
const p5Parts = [];
const includedTables = [];

function p5DateExpr(dateExpr) {
  // Avoid type instability of PERCENTILE_CONT on DATE by taking percentile over an integer day index.
  // Returns DATE or NULL.
  // Snowflake does not support DATE_PART(''EPOCH_DAY'', ...), so use DATEDIFF(day, ''1970-01-01'', date).
  return `DATEADD(DAY,
                  FLOOR(PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY DATEDIFF(DAY, DATE ''1970-01-01'', (${dateExpr})::DATE))),
                  DATE ''1970-01-01'')::DATE`;
}

function addP5Select(selectSql) {
  p5Parts.push(selectSql);
}

for (const d of defs) {
  const t = d.table;
  if (!(only === ''ALL'' || only === t)) continue;
  if (!tableExists(DB_PARAM, SCHEMA_NAME, t)) continue;

  const fq = `${DB_PARAM}.${SCHEMA_NAME}.${t}`;

  if (d.kind === ''enc'') {
    if (!colExists(DB_PARAM, SCHEMA_NAME, t, d.dateCol) || !colExists(DB_PARAM, SCHEMA_NAME, t, d.catCol)) continue;
    // P5 based on ADMIT_DATE
    addP5Select(
      `SELECT ''${t}'' AS TABLE_NAME,
              ${p5DateExpr(d.dateCol)} AS P5_DATE
       FROM ${fq}
       WHERE ${d.dateCol} IS NOT NULL`
    );

    seriesParts.push(
      `SELECT ''${t}'' AS TABLE_NAME,
              UPPER(TRIM(${d.catCol}::STRING)) AS CATEGORY,
              DATE_TRUNC(''MONTH'', ${d.dateCol})::DATE AS MONTH,
              COUNT(*)::NUMBER AS RESULTN
       FROM ${fq}
       WHERE ${d.dateCol} IS NOT NULL
         AND ${d.catCol} IS NOT NULL
         AND UPPER(TRIM(${d.catCol}::STRING)) IN (''AV'',''ED'',''IP'',''EI'',''TH'')
       GROUP BY 1,2,3`
    );
    includedTables.push(t);
  } else if (d.kind === ''enc_join'') {
    // Prefer replicated columns if present, else join ENCOUNTER.
    const hasRepDate = colExists(DB_PARAM, SCHEMA_NAME, t, d.dateCol);
    const hasRepCat = colExists(DB_PARAM, SCHEMA_NAME, t, d.catCol);
    const hasEncId = colExists(DB_PARAM, SCHEMA_NAME, t, ''ENCOUNTERID'');

    if (hasRepDate && hasRepCat) {
      addP5Select(
        `SELECT ''${t}'' AS TABLE_NAME,
                ${p5DateExpr(d.dateCol)} AS P5_DATE
         FROM ${fq}
         WHERE ${d.dateCol} IS NOT NULL`
      );
      seriesParts.push(
        `SELECT ''${t}'' AS TABLE_NAME,
                UPPER(TRIM(${d.catCol}::STRING)) AS CATEGORY,
                DATE_TRUNC(''MONTH'', ${d.dateCol})::DATE AS MONTH,
                COUNT(*)::NUMBER AS RESULTN
         FROM ${fq}
         WHERE ${d.dateCol} IS NOT NULL
           AND ${d.catCol} IS NOT NULL
           AND UPPER(TRIM(${d.catCol}::STRING)) IN (''AV'',''ED'',''IP'',''EI'',''TH'')
         GROUP BY 1,2,3`
      );
      includedTables.push(t);
    } else if (encTableOk && hasEncId) {
      const encFq = `${DB_PARAM}.${SCHEMA_NAME}.ENCOUNTER`;
      addP5Select(
        `SELECT ''${t}'' AS TABLE_NAME,
                ${p5DateExpr(''e.ADMIT_DATE'')} AS P5_DATE
         FROM ${fq} x
         JOIN ${encFq} e
           ON x.ENCOUNTERID = e.ENCOUNTERID
         WHERE e.ADMIT_DATE IS NOT NULL`
      );
      seriesParts.push(
        `SELECT ''${t}'' AS TABLE_NAME,
                UPPER(TRIM(e.ENC_TYPE::STRING)) AS CATEGORY,
                DATE_TRUNC(''MONTH'', e.ADMIT_DATE)::DATE AS MONTH,
                COUNT(*)::NUMBER AS RESULTN
         FROM ${fq} x
         JOIN ${encFq} e
           ON x.ENCOUNTERID = e.ENCOUNTERID
         WHERE e.ADMIT_DATE IS NOT NULL
           AND e.ENC_TYPE IS NOT NULL
           AND UPPER(TRIM(e.ENC_TYPE::STRING)) IN (''AV'',''ED'',''IP'',''EI'',''TH'')
         GROUP BY 1,2,3`
      );
      includedTables.push(t);
    }
  } else if (d.kind === ''date_only'') {
    if (!colExists(DB_PARAM, SCHEMA_NAME, t, d.dateCol)) continue;
    addP5Select(
      `SELECT ''${t}'' AS TABLE_NAME,
              ${p5DateExpr(d.dateCol)} AS P5_DATE
       FROM ${fq}
       WHERE ${d.dateCol} IS NOT NULL`
    );
    seriesParts.push(
      `SELECT ''${t}'' AS TABLE_NAME,
              ''n/a'' AS CATEGORY,
              DATE_TRUNC(''MONTH'', ${d.dateCol})::DATE AS MONTH,
              COUNT(*)::NUMBER AS RESULTN
       FROM ${fq}
       WHERE ${d.dateCol} IS NOT NULL
       GROUP BY 1,2,3`
    );
    includedTables.push(t);
  }
}

if (seriesParts.length === 0) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''OK'', nsd, false,
    { message: ''No eligible tables/columns found for monthly outlier evaluation.'', target_table: only });
  return `DC 2.08 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
}

// Build outliers into a temp table.
q(''DROP TABLE IF EXISTS _OUTLIERS'');
q(
  `CREATE TEMP TABLE _OUTLIERS AS
   WITH
     PARAMS AS (
       SELECT
         TO_DATE(?) AS RESPONSE_DATE,
         DATE_TRUNC(''MONTH'', DATEADD(MONTH, -11, TO_DATE(?)))::DATE AS MONTH11_DATE,
         ${nback}::NUMBER AS NBACK,
         ${nsd}::NUMBER AS NSD,
         ${minLagAvg}::NUMBER AS MIN_LAG_AVG
     ),
     P5_BY_TABLE AS (
       ${p5Parts.join(''\\nUNION ALL\\n'')}
     ),
     MONTHLY AS (
       ${seriesParts.join(''\\nUNION ALL\\n'')}
     ),
     STATS AS (
       SELECT
         m.TABLE_NAME,
         m.CATEGORY,
         m.MONTH,
         m.RESULTN,
         p.P5_DATE,
         (SELECT MONTH11_DATE FROM PARAMS) AS MONTH11_DATE,
         AVG(m.RESULTN) OVER (
           PARTITION BY m.TABLE_NAME, m.CATEGORY
           ORDER BY m.MONTH
           ROWS BETWEEN ${nback} PRECEDING AND 1 PRECEDING
         ) AS LAG_AVG,
         STDDEV_SAMP(m.RESULTN) OVER (
           PARTITION BY m.TABLE_NAME, m.CATEGORY
           ORDER BY m.MONTH
           ROWS BETWEEN ${nback} PRECEDING AND 1 PRECEDING
         ) AS LAG_STD
       FROM MONTHLY m
       JOIN P5_BY_TABLE p
         ON p.TABLE_NAME = m.TABLE_NAME
     ),
     ELIG AS (
       SELECT
         s.*,
         IFF(s.LAG_STD IS NOT NULL AND s.LAG_STD > 0,
             (s.RESULTN - s.LAG_AVG) / s.LAG_STD,
             NULL
         ) AS DSD
       FROM STATS s
       WHERE s.LAG_AVG IS NOT NULL
         AND s.LAG_STD IS NOT NULL
         AND s.LAG_AVG >= (SELECT MIN_LAG_AVG FROM PARAMS)
         AND s.LAG_STD > 0
         AND s.MONTH IS NOT NULL
         AND s.P5_DATE IS NOT NULL
         AND s.MONTH > s.P5_DATE
         AND s.MONTH < s.MONTH11_DATE
         AND s.MONTH NOT IN (TO_DATE(''2020-03-01''), TO_DATE(''2020-04-01''))
     )
   SELECT
     TABLE_NAME,
     CATEGORY,
     MONTH,
     RESULTN,
     LAG_AVG,
     LAG_STD,
     DSD,
     P5_DATE,
     MONTH11_DATE,
     IFF(RESULTN = 0, ''ZERO'', IFF(DSD < (SELECT NSD FROM PARAMS), ''Z_LT_NSD'', ''OTHER'')) AS OUTLIER_REASON
   FROM ELIG
   WHERE RESULTN = 0 OR DSD < (SELECT NSD FROM PARAMS)`,
  [responseDateStr, responseDateStr]
);

const outlierN = Number(scalar(''SELECT COUNT(*) FROM _OUTLIERS''));

// Insert outlier rows
q(
  `INSERT INTO ${resultsTbl} (
     RUN_ID, CHECK_ID, CHECK_NAME, ROW_NUM, EDC_TABLE,
     SOURCE_TABLE, CODE_TYPE, METRIC, VALUE_NUM, VALUE_STR,
     THRESHOLD_NUM, EXCEPTION_FLAG, DETAILS
   )
   SELECT
     ?, ?, ?, ?, o.TABLE_NAME,
     o.TABLE_NAME, o.CATEGORY,
     ''MONTHLY_OUTLIER'',
     o.RESULTN::NUMBER(38,10),
     TO_VARCHAR(o.MONTH, ''YYYY-MM'') AS VALUE_STR,
     ${nsd}::NUMBER(38,10) AS THRESHOLD_NUM,
     TRUE AS EXCEPTION_FLAG,
     OBJECT_CONSTRUCT(
       ''table'', o.TABLE_NAME,
       ''category'', o.CATEGORY,
       ''month'', TO_VARCHAR(o.MONTH, ''YYYY-MM-DD''),
       ''records'', o.RESULTN,
       ''lag_avg_prev_12'', o.LAG_AVG,
       ''lag_std_prev_12'', o.LAG_STD,
       ''dsd'', o.DSD,
       ''nback'', ${nback},
       ''nsd'', ${nsd},
       ''min_lag_avg'', ${minLagAvg},
       ''p5_date'', TO_VARCHAR(o.P5_DATE, ''YYYY-MM-DD''),
       ''month11_date'', TO_VARCHAR(o.MONTH11_DATE, ''YYYY-MM-DD''),
       ''excluded_months'', ARRAY_CONSTRUCT(''2020-03-01'',''2020-04-01''),
       ''outlier_reason'', o.OUTLIER_REASON,
       ''definition'', ''Monthly outlier if records=0 or (records-lag_avg)/lag_std < nsd, with lagged 12-month rolling mean/std.''
     )
   FROM _OUTLIERS o`,
  [RUN_ID, checkId, checkName, rowNum]
);

insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''OUTLIER_ROW_N'', outlierN, String(outlierN), nsd, false,
  { target_table: only, response_date: responseDateStr, included_tables: Array.from(new Set(includedTables)).sort() }
);
insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''OUTLIER_FOUND_FLAG'', (outlierN > 0 ? 1 : 0), String(outlierN > 0 ? 1 : 0), nsd, (outlierN > 0),
  { target_table: only, outlier_row_n: outlierN }
);
insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''OK'', nsd, false,
  { target_table: only, outlier_row_n: outlierN, note: (outlierN > 0 ? ''Outliers found (see MONTHLY_OUTLIER rows).'' : ''No monthly outliers found.'') }
);

return `DC 2.08 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} OUTLIERS=${outlierN}`;
';