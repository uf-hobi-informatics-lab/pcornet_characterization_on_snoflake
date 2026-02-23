CREATE OR REPLACE PROCEDURE "SP_DC_4_03"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR)
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
const rowNum = 4.03;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();
if (!(only === ''ALL'' ||
      only === ''DIAGNOSIS'' || only === ''PROCEDURES'' || only === ''DISPENSING'' || only === ''IMMUNIZATION'' ||
      only === ''MED_ADMIN'' || only === ''PRESCRIBING'' || only === ''OBS_CLIN'' || only === ''PRO_CM'' ||
      only === ''EXTERNAL_MEDS'')) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or one of DIAGNOSIS, PROCEDURES, DISPENSING, IMMUNIZATION, MED_ADMIN, PRESCRIBING, OBS_CLIN, PRO_CM, EXTERNAL_MEDS.`);
}

// SAS flags decreases >5% (i.e., percent change < -5.00) or current=0 when previous>0.
// SAS Table VC excludes category=''09'' from exceptions.
const thresholdPctLt = -5.0;

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

// Definitions based on SAS Table VC.
// categoryCol and codeCol are the CDM columns used for grouping and distinct code counting.
const defs = [
  { edcTable: ''DIAGNOSIS'', dataset: ''DIA_L3_DX_DXTYPE'', table: ''DIAGNOSIS'', categoryCol: ''DX_TYPE'', codeCol: ''DX'', categories: [''09'',''10''] },
  { edcTable: ''PROCEDURES'', dataset: ''PRO_L3_PX_PXTYPE'', table: ''PROCEDURES'', categoryCol: ''PX_TYPE'', codeCol: ''PX'', categories: [''09'',''10'',''CH'',''ND''] },
  { edcTable: ''DISPENSING'', dataset: ''DISP_L3_NDC'', table: ''DISPENSING'', categoryCol: null, codeCol: ''NDC'', categories: [''ND''] },
  { edcTable: ''IMMUNIZATION'', dataset: ''IMMUNE_L3_CODE_CODETYPE'', table: ''IMMUNIZATION'', categoryCol: ''VX_CODE_TYPE'', codeCol: ''VX_CODE'', categories: [''CH'',''ND'',''RX'',''CX''] },
  { edcTable: ''MED_ADMIN'', dataset: ''MEDADM_L3_CODE_TYPE'', table: ''MED_ADMIN'', categoryCol: ''MEDADMIN_TYPE'', codeCol: ''MEDADMIN_CODE'', categories: [''ND'',''RX''] },
  { edcTable: ''PRESCRIBING'', dataset: ''PRES_L3_RXCUI'', table: ''PRESCRIBING'', categoryCol: null, codeCol: ''RXNORM_CUI'', categories: [''RX''] },
  { edcTable: ''OBS_CLIN'', dataset: ''OBSCLIN_L3_CODE_TYPE'', table: ''OBS_CLIN'', categoryCol: ''OBSCLIN_TYPE'', codeCol: ''OBSCLIN_CODE'', categories: [''LC''] },
  { edcTable: ''PRO_CM'', dataset: ''PROCM_L3_CODE_TYPE'', table: ''PRO_CM'', categoryCol: ''PRO_TYPE'', codeCol: ''PRO_CODE'', categories: [''LC''] },
  { edcTable: ''EXTERNAL_MEDS'', dataset: ''EXTMED_L3_RXCUI'', table: ''EXTERNAL_MEDS'', categoryCol: null, codeCol: ''RXNORM_CUI'', categories: [''RX''] }
];

function prereqOk(db, schema, d) {
  if (!tableExists(db, schema, d.table)) return { ok: false, reason: `missing ${d.table} table` };
  if (!colExists(db, schema, d.table, d.codeCol)) return { ok: false, reason: `${d.table} missing ${d.codeCol}` };
  if (d.categoryCol && !colExists(db, schema, d.table, d.categoryCol)) return { ok: false, reason: `${d.table} missing ${d.categoryCol}` };
  return { ok: true, reason: null };
}

function queryCounts(db, schema, d) {
  const fq = `${db}.${schema}.${d.table}`;
  const cats = d.categories.map(v => `''${v}''`).join('','');

  let sql;
  if (d.categoryCol) {
    sql = `SELECT
             UPPER(TRIM(${d.categoryCol}::STRING)) AS category,
             COUNT(*)::NUMBER AS records_n,
             COUNT(DISTINCT UPPER(TRIM(${d.codeCol}::STRING)))::NUMBER AS distinct_code_n
           FROM ${fq}
           WHERE ${d.categoryCol} IS NOT NULL
             AND TRIM(${d.categoryCol}::STRING) <> ''''
             AND UPPER(TRIM(${d.categoryCol}::STRING)) IN (${cats})
             AND ${d.codeCol} IS NOT NULL
             AND TRIM(${d.codeCol}::STRING) <> ''''
           GROUP BY 1`;
  } else {
    // Single category domains (e.g., DISPENSING NDC or PRESCRIBING RXNORM_CUI).
    const fixedCat = d.categories[0];
    sql = `SELECT
             ''${fixedCat}'' AS category,
             COUNT(*)::NUMBER AS records_n,
             COUNT(DISTINCT UPPER(TRIM(${d.codeCol}::STRING)))::NUMBER AS distinct_code_n
           FROM ${fq}
           WHERE ${d.codeCol} IS NOT NULL
             AND TRIM(${d.codeCol}::STRING) <> ''''`;
  }

  // For categoryCol-based datasets we already constrain to categories via IN (cats).

  const rs = q(sql);
  const out = {};
  while (rs.next()) {
    const cat = rs.getColumnValue(1);
    out[cat] = {
      records_n: Number(rs.getColumnValue(2)),
      distinct_code_n: Number(rs.getColumnValue(3))
    };
  }
  return out;
}

let anyFlag = 0;
const evaluated = [];
const skipped = [];

for (const d of defs) {
  if (!(only === ''ALL'' || only === d.edcTable)) continue;

  const curOk = prereqOk(DB_PARAM, SCHEMA_NAME, d);
  const prevOk = prereqOk(PREV_DB_PARAM, PREV_SCHEMA_NAME, d);
  if (!curOk.ok || !prevOk.ok) {
    skipped.push({ edc_table: d.edcTable, dataset: d.dataset, current_ok: curOk.ok, prev_ok: prevOk.ok, current_reason: curOk.reason, prev_reason: prevOk.reason });
    continue;
  }

  const cur = queryCounts(DB_PARAM, SCHEMA_NAME, d);
  const prev = queryCounts(PREV_DB_PARAM, PREV_SCHEMA_NAME, d);

  for (const cat of d.categories) {
    const curRec = (cur[cat] ? cur[cat].records_n : 0);
    const prevRec = (prev[cat] ? prev[cat].records_n : 0);
    const curCode = (cur[cat] ? cur[cat].distinct_code_n : 0);
    const prevCode = (prev[cat] ? prev[cat].distinct_code_n : 0);

    const recPct = (prevRec > 0) ? ((curRec - prevRec) / prevRec) * 100.0 : null;
    const codePct = (prevCode > 0) ? ((curCode - prevCode) / prevCode) * 100.0 : null;

    const eligible = (cat !== ''09'');
    const recFlag = eligible && ((curRec === 0 && prevRec > 0) || (recPct !== null && recPct < thresholdPctLt));
    const codeFlag = eligible && ((curCode === 0 && prevCode > 0) || (codePct !== null && codePct < thresholdPctLt));
    const flag = recFlag || codeFlag;
    if (flag) anyFlag = 1;

    const details = {
      edc_table: d.edcTable,
      dataset: d.dataset,
      table: d.table,
      category: cat,
      category_col: d.categoryCol,
      code_col: d.codeCol,
      current: { db: DB_PARAM, schema: SCHEMA_NAME, records_n: curRec, distinct_code_n: curCode },
      previous: { db: PREV_DB_PARAM, schema: PREV_SCHEMA_NAME, records_n: prevRec, distinct_code_n: prevCode },
      record_pct_change: recPct,
      distinct_code_pct_change: codePct,
      threshold_pct_lt: thresholdPctLt,
      exclude_from_exceptions_if_category: ''09''
    };

    insertMetric(resultsTbl, bindsBase, d.edcTable, d.dataset, cat, ''RECORDS_PREV_N'', prevRec, String(prevRec), thresholdPctLt, false, details);
    insertMetric(resultsTbl, bindsBase, d.edcTable, d.dataset, cat, ''RECORDS_CUR_N'', curRec, String(curRec), thresholdPctLt, false, details);
    insertMetric(resultsTbl, bindsBase, d.edcTable, d.dataset, cat, ''RECORDS_PCT_CHANGE'', recPct, (recPct === null ? null : String(recPct)), thresholdPctLt, false, details);
    insertMetric(resultsTbl, bindsBase, d.edcTable, d.dataset, cat, ''RECORDS_DECREASE_FLAG'', (recFlag ? 1 : 0), String(recFlag ? 1 : 0), thresholdPctLt, recFlag, details);

    insertMetric(resultsTbl, bindsBase, d.edcTable, d.dataset, cat, ''DISTINCT_CODES_PREV_N'', prevCode, String(prevCode), thresholdPctLt, false, details);
    insertMetric(resultsTbl, bindsBase, d.edcTable, d.dataset, cat, ''DISTINCT_CODES_CUR_N'', curCode, String(curCode), thresholdPctLt, false, details);
    insertMetric(resultsTbl, bindsBase, d.edcTable, d.dataset, cat, ''DISTINCT_CODES_PCT_CHANGE'', codePct, (codePct === null ? null : String(codePct)), thresholdPctLt, false, details);
    insertMetric(resultsTbl, bindsBase, d.edcTable, d.dataset, cat, ''DISTINCT_CODES_DECREASE_FLAG'', (codeFlag ? 1 : 0), String(codeFlag ? 1 : 0), thresholdPctLt, codeFlag, details);

    insertMetric(resultsTbl, bindsBase, d.edcTable, d.dataset, cat, ''ANY_DECREASE_FLAG'', (flag ? 1 : 0), String(flag ? 1 : 0), thresholdPctLt, flag, details);
  }

  evaluated.push({ edc_table: d.edcTable, dataset: d.dataset });
}

insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''ANY_DECREASE_FLAG'', anyFlag, String(anyFlag), thresholdPctLt, (anyFlag === 1),
  { target_table: only, prev_db: PREV_DB_PARAM, prev_schema: PREV_SCHEMA_NAME, evaluated: evaluated, skipped: skipped }
);
insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''OK'', thresholdPctLt, false,
  { target_table: only, prev_db: PREV_DB_PARAM, prev_schema: PREV_SCHEMA_NAME, evaluated: evaluated, skipped: skipped }
);

return `DC 4.03 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} PREV=${PREV_DB_PARAM}.${PREV_SCHEMA_NAME}`;
';