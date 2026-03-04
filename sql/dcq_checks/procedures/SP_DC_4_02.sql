CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_4_02("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
function q(sqlText, binds) { return snowflake.execute({ sqlText: sqlText, binds: binds || [] }); }
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
if (!isSafeIdentPart(PREV_DB_PARAM)) throw new Error(`Unsafe PREV_DB_PARAM: ${PREV_DB_PARAM}`);
if (!isSafeIdentPart(PREV_SCHEMA_NAME)) throw new Error(`Unsafe PREV_SCHEMA_NAME: ${PREV_SCHEMA_NAME}`);

const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 4.02;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();
if (!(only === ''ALL'' || only === ''DIAGNOSIS'' || only === ''PROCEDURES'' || only === ''LAB_RESULT_CM'' || only === ''PRESCRIBING'')) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, DIAGNOSIS, PROCEDURES, LAB_RESULT_CM, or PRESCRIBING.`);
}

// SAS flags decreases >5% (i.e., percent change < -5.00) or current=0 when previous>0.
const thresholdPctLt = -5.0;
const encTypes = [''AV'',''ED'',''IP'',''OA'',''TH''];

q(`CREATE SCHEMA IF NOT EXISTS ${outSchema}`);
q(`CREATE TABLE IF NOT EXISTS ${resultsTbl} (
  RUN_ID STRING, CHECK_ID STRING, CHECK_NAME STRING, ROW_NUM NUMBER(10,2), EDC_TABLE STRING,
  SOURCE_TABLE STRING, CODE_TYPE STRING, METRIC STRING, VALUE_NUM NUMBER(38,10), VALUE_STR STRING,
  THRESHOLD_NUM NUMBER(38,10), EXCEPTION_FLAG BOOLEAN, DETAILS VARIANT,
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)`);

// registry constants
const regRs = q(
  `SELECT CHECK_ID, CHECK_NAME
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

// Domains (table + join to ENCOUNTER for ENC_TYPE)
const defs = [
  { domain: ''DIAGNOSIS'', table: ''DIAGNOSIS'', patCol: ''PATID'' },
  { domain: ''PROCEDURES'', table: ''PROCEDURES'', patCol: ''PATID'' },
  { domain: ''LAB_RESULT_CM'', table: ''LAB_RESULT_CM'', patCol: ''PATID'' },
  { domain: ''PRESCRIBING'', table: ''PRESCRIBING'', patCol: ''PATID'' }
];

const curEncounter = {
  db: DB_PARAM,
  schema: SCHEMA_NAME,
  encounter_table_exists: tableExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER''),
  encounterid_col_exists: colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ENCOUNTERID''),
  enc_type_col_exists: colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ENC_TYPE'')
};
const prevEncounter = {
  db: PREV_DB_PARAM,
  schema: PREV_SCHEMA_NAME,
  encounter_table_exists: tableExists(PREV_DB_PARAM, PREV_SCHEMA_NAME, ''ENCOUNTER''),
  encounterid_col_exists: colExists(PREV_DB_PARAM, PREV_SCHEMA_NAME, ''ENCOUNTER'', ''ENCOUNTERID''),
  enc_type_col_exists: colExists(PREV_DB_PARAM, PREV_SCHEMA_NAME, ''ENCOUNTER'', ''ENC_TYPE'')
};

function prereqOk(db, schema, domainTable, patCol) {
  if (!tableExists(db, schema, ''ENCOUNTER'')) return { ok: false, reason: ''missing ENCOUNTER table'' };
  if (!colExists(db, schema, ''ENCOUNTER'', ''ENCOUNTERID'') || !colExists(db, schema, ''ENCOUNTER'', ''ENC_TYPE'')) {
    return { ok: false, reason: ''ENCOUNTER missing ENCOUNTERID or ENC_TYPE'' };
  }
  if (!tableExists(db, schema, domainTable)) return { ok: false, reason: `missing ${domainTable} table` };
  if (!colExists(db, schema, domainTable, ''ENCOUNTERID'')) return { ok: false, reason: `${domainTable} missing ENCOUNTERID` };
  if (!colExists(db, schema, domainTable, patCol)) return { ok: false, reason: `${domainTable} missing ${patCol}` };
  return { ok: true, reason: null };
}

function buildCountsSql(db, schema, domainsToInclude) {
  const parts = [];
  const encFq = `${db}.${schema}.ENCOUNTER`;
  const encTypeList = encTypes.map(v => `''${v}''`).join('','');

  for (const d of domainsToInclude) {
    const fq = `${db}.${schema}.${d.table}`;
    parts.push(
      `SELECT ''${d.domain}'' AS domain,
              UPPER(TRIM(e.ENC_TYPE::STRING)) AS enc_type,
              COUNT(*)::NUMBER AS record_n,
              COUNT(DISTINCT t.${d.patCol})::NUMBER AS distinct_patid_n
       FROM ${fq} t
       JOIN ${encFq} e
         ON e.ENCOUNTERID = t.ENCOUNTERID
       WHERE e.ENC_TYPE IS NOT NULL
         AND UPPER(TRIM(e.ENC_TYPE::STRING)) IN (${encTypeList})
       GROUP BY 1,2`
    );
  }
  if (parts.length === 0) return null;
  return parts.join(''\\nUNION ALL\\n'');
}

const domainsToRun = defs.filter(d => (only === ''ALL'' || only === d.table));
const skipped = [];
const runnable = [];
for (const d of domainsToRun) {
  const curOk = prereqOk(DB_PARAM, SCHEMA_NAME, d.table, d.patCol);
  const prevOk = prereqOk(PREV_DB_PARAM, PREV_SCHEMA_NAME, d.table, d.patCol);
  if (!curOk.ok || !prevOk.ok) {
    skipped.push({ domain: d.domain, table: d.table, current_ok: curOk.ok, prev_ok: prevOk.ok, current_reason: curOk.reason, prev_reason: prevOk.reason });
    continue;
  }
  runnable.push(d);
}

if (runnable.length === 0) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    {
      message: ''No runnable domains (missing tables/columns in one or both snapshots).'',
      current_db: DB_PARAM,
      current_schema: SCHEMA_NAME,
      prev_db: PREV_DB_PARAM,
      prev_schema: PREV_SCHEMA_NAME,
      target_table: only,
      encounter_prereq_current: curEncounter,
      encounter_prereq_prev: prevEncounter,
      skipped: skipped
    }
  );
  return `DC 4.02 ERROR: no runnable domains`; 
}

const curSql = buildCountsSql(DB_PARAM, SCHEMA_NAME, runnable);
const prevSql = buildCountsSql(PREV_DB_PARAM, PREV_SCHEMA_NAME, runnable);

const rs = q(
  `WITH cur AS (
     ${curSql}
   ),
   prev AS (
     ${prevSql}
   ),
   keys AS (
     SELECT domain, enc_type FROM cur
     UNION
     SELECT domain, enc_type FROM prev
   )
   SELECT
     k.domain,
     k.enc_type,
     COALESCE(p.record_n, 0) AS prev_record_n,
     COALESCE(c.record_n, 0) AS cur_record_n,
     COALESCE(p.distinct_patid_n, 0) AS prev_distinct_patid_n,
     COALESCE(c.distinct_patid_n, 0) AS cur_distinct_patid_n
   FROM keys k
   LEFT JOIN prev p
     ON p.domain = k.domain AND p.enc_type = k.enc_type
   LEFT JOIN cur c
     ON c.domain = k.domain AND c.enc_type = k.enc_type
   ORDER BY k.enc_type, k.domain`
);

let anyFlag = 0;
const evaluated = [];
while (rs.next()) {
  const domain = rs.getColumnValue(1);
  const encType = rs.getColumnValue(2);
  const prevRec = Number(rs.getColumnValue(3));
  const curRec = Number(rs.getColumnValue(4));
  const prevPat = Number(rs.getColumnValue(5));
  const curPat = Number(rs.getColumnValue(6));

  const recPct = (prevRec > 0) ? ((curRec - prevRec) / prevRec) * 100.0 : null;
  const patPct = (prevPat > 0) ? ((curPat - prevPat) / prevPat) * 100.0 : null;

  const recFlag = ((curRec === 0 && prevRec > 0) || (recPct !== null && recPct < thresholdPctLt));
  const patFlag = ((curPat === 0 && prevPat > 0) || (patPct !== null && patPct < thresholdPctLt));
  const flag = recFlag || patFlag;
  if (flag) anyFlag = 1;

  const details = {
    domain: domain,
    encounter_type: encType,
    current: { db: DB_PARAM, schema: SCHEMA_NAME, record_n: curRec, distinct_patid_n: curPat },
    previous: { db: PREV_DB_PARAM, schema: PREV_SCHEMA_NAME, record_n: prevRec, distinct_patid_n: prevPat },
    record_pct_change: recPct,
    patient_pct_change: patPct,
    threshold_pct_lt: thresholdPctLt,
    flag_rules: ''Flag if current=0 and previous>0 OR percent_change < -5.0''
  };

  // Use EDC_TABLE for encounter type, SOURCE_TABLE for domain/table.
  insertMetric(resultsTbl, bindsBase, encType, domain, ''RECORDS'', ''PREV_N'', prevRec, String(prevRec), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, encType, domain, ''RECORDS'', ''CUR_N'', curRec, String(curRec), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, encType, domain, ''RECORDS'', ''PCT_CHANGE'', recPct, (recPct === null ? null : String(recPct)), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, encType, domain, ''RECORDS'', ''DECREASE_FLAG'', (recFlag ? 1 : 0), String(recFlag ? 1 : 0), thresholdPctLt, recFlag, details);

  insertMetric(resultsTbl, bindsBase, encType, domain, ''PATIENTS'', ''PREV_DISTINCT_N'', prevPat, String(prevPat), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, encType, domain, ''PATIENTS'', ''CUR_DISTINCT_N'', curPat, String(curPat), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, encType, domain, ''PATIENTS'', ''PCT_CHANGE'', patPct, (patPct === null ? null : String(patPct)), thresholdPctLt, false, details);
  insertMetric(resultsTbl, bindsBase, encType, domain, ''PATIENTS'', ''DECREASE_FLAG'', (patFlag ? 1 : 0), String(patFlag ? 1 : 0), thresholdPctLt, patFlag, details);

  insertMetric(resultsTbl, bindsBase, encType, domain, ''ALL'', ''ANY_DECREASE_FLAG'', (flag ? 1 : 0), String(flag ? 1 : 0), thresholdPctLt, flag, details);
  evaluated.push({ enc_type: encType, domain: domain });
}

insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''ANY_DECREASE_FLAG'', anyFlag, String(anyFlag), thresholdPctLt, (anyFlag === 1),
  { target_table: only, prev_db: PREV_DB_PARAM, prev_schema: PREV_SCHEMA_NAME, evaluated: evaluated, skipped: skipped }
);
insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''OK'', thresholdPctLt, false,
  { target_table: only, prev_db: PREV_DB_PARAM, prev_schema: PREV_SCHEMA_NAME, evaluated: evaluated, skipped: skipped }
);

return `DC 4.02 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} PREV=${PREV_DB_PARAM}.${PREV_SCHEMA_NAME}`;
';
