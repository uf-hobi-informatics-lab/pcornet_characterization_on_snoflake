CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_2_09("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
  if (vStartDate) clause += ` AND TRY_TO_DATE(${colName}) >= TRY_TO_DATE(''''${vStartDate}'''')`;
  if (vEndDate) clause += ` AND TRY_TO_DATE(${colName}) <= TRY_TO_DATE(''''${vEndDate}'''')`;
  return clause;
}

const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 2.09;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();
if (!(only === ''ALL'' || only === ''DEMOGRAPHIC'' || only === ''ENCOUNTER'')) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, DEMOGRAPHIC, or ENCOUNTER.`);
}

const thresholdPctLt = 80.0;
const thresholdPctDxProc = 50.0;

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
const bindsBase = [RUN_ID, checkId, checkName, rowNum];

q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);

// prerequisites
if (!tableExists(DB_PARAM, SCHEMA_NAME, ''DEMOGRAPHIC'') || !colExists(DB_PARAM, SCHEMA_NAME, ''DEMOGRAPHIC'', ''PATID'')) {
  insertMetric(resultsTbl, bindsBase, edcTable, ''DEMOGRAPHIC'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    { message: ''DEMOGRAPHIC missing required table/column PATID'' });
  return ''DC 2.09 ERROR: DEMOGRAPHIC missing'';
}
if (!tableExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'') || !colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''PATID'') || !colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ADMIT_DATE'')) {
  insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    { message: ''ENCOUNTER missing required columns PATID, ADMIT_DATE'' });
  return ''DC 2.09 ERROR: ENCOUNTER missing required columns'';
}

const demo = `${DB_PARAM}.${SCHEMA_NAME}.DEMOGRAPHIC`;
const enc = `${DB_PARAM}.${SCHEMA_NAME}.ENCOUNTER`;

// Use todays date (session CURRENT_DATE) as the anchor for the windows.
const anchorDateStr = scalar("SELECT TO_VARCHAR(CURRENT_DATE(), ''YYYY-MM-DD'')");

// Face-to-face encounters per PCORnet: ED, EI, IP, OS, AV
if (!colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ENC_TYPE'')) {
  insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    { message: ''ENCOUNTER missing required column ENC_TYPE'' });
  return ''DC 2.09 ERROR: ENCOUNTER missing ENC_TYPE'';
}
if (!tableExists(DB_PARAM, SCHEMA_NAME, ''DIAGNOSIS'') || !colExists(DB_PARAM, SCHEMA_NAME, ''DIAGNOSIS'', ''PATID'') || !colExists(DB_PARAM, SCHEMA_NAME, ''DIAGNOSIS'', ''DX_DATE'') || !colExists(DB_PARAM, SCHEMA_NAME, ''DIAGNOSIS'', ''ENC_TYPE'')) {
  insertMetric(resultsTbl, bindsBase, edcTable, ''DIAGNOSIS'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    { message: ''DIAGNOSIS missing required columns PATID, DX_DATE, ENC_TYPE'' });
  return ''DC 2.09 ERROR: DIAGNOSIS missing required columns'';
}
if (!tableExists(DB_PARAM, SCHEMA_NAME, ''VITAL'') || !colExists(DB_PARAM, SCHEMA_NAME, ''VITAL'', ''PATID'') || !colExists(DB_PARAM, SCHEMA_NAME, ''VITAL'', ''MEASURE_DATE'')) {
  insertMetric(resultsTbl, bindsBase, edcTable, ''VITAL'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    { message: ''VITAL missing required columns PATID, MEASURE_DATE'' });
  return ''DC 2.09 ERROR: VITAL missing required columns'';
}

const dia = `${DB_PARAM}.${SCHEMA_NAME}.DIAGNOSIS`;
const vit = `${DB_PARAM}.${SCHEMA_NAME}.VITAL`;

const hasPrescribing = tableExists(DB_PARAM, SCHEMA_NAME, ''PRESCRIBING'')
  && colExists(DB_PARAM, SCHEMA_NAME, ''PRESCRIBING'', ''PATID'')
  && colExists(DB_PARAM, SCHEMA_NAME, ''PRESCRIBING'', ''RX_ORDER_DATE'');
const hasMedAdmin = tableExists(DB_PARAM, SCHEMA_NAME, ''MED_ADMIN'')
  && colExists(DB_PARAM, SCHEMA_NAME, ''MED_ADMIN'', ''PATID'')
  && colExists(DB_PARAM, SCHEMA_NAME, ''MED_ADMIN'', ''MEDADMIN_START_DATE'');
const hasLab = tableExists(DB_PARAM, SCHEMA_NAME, ''LAB_RESULT_CM'')
  && colExists(DB_PARAM, SCHEMA_NAME, ''LAB_RESULT_CM'', ''PATID'')
  && colExists(DB_PARAM, SCHEMA_NAME, ''LAB_RESULT_CM'', ''RESULT_DATE'');
const hasProc = tableExists(DB_PARAM, SCHEMA_NAME, ''PROCEDURES'')
  && colExists(DB_PARAM, SCHEMA_NAME, ''PROCEDURES'', ''PATID'');

const pres = `${DB_PARAM}.${SCHEMA_NAME}.PRESCRIBING`;
const meda = `${DB_PARAM}.${SCHEMA_NAME}.MED_ADMIN`;
const lab = `${DB_PARAM}.${SCHEMA_NAME}.LAB_RESULT_CM`;
const pro = `${DB_PARAM}.${SCHEMA_NAME}.PROCEDURES`;

const medUnionSql = (hasPrescribing || hasMedAdmin)
  ? `med AS (
       ${hasPrescribing ? `SELECT DISTINCT PATID FROM ${pres} p JOIN anchor a ON 1=1 WHERE p.PATID IS NOT NULL AND TRIM(p.PATID::STRING) <> '''' AND TRY_TO_DATE(p.RX_ORDER_DATE) BETWEEN a.start_5yr AND a.anchor_date` : `SELECT NULL AS PATID WHERE 1=0`}
       UNION
       ${hasMedAdmin ? `SELECT DISTINCT PATID FROM ${meda} m JOIN anchor a ON 1=1 WHERE m.PATID IS NOT NULL AND TRIM(m.PATID::STRING) <> '''' AND TRY_TO_DATE(m.MEDADMIN_START_DATE) BETWEEN a.start_5yr AND a.anchor_date` : `SELECT NULL AS PATID WHERE 1=0`}
     )`
  : `med AS (SELECT NULL AS PATID WHERE 1=0)`;

const labSql = hasLab
  ? `lab AS (
       SELECT DISTINCT PATID
       FROM ${lab} l
       JOIN anchor a ON 1=1
       WHERE l.PATID IS NOT NULL
         AND TRIM(l.PATID::STRING) <> ''''
         AND TRY_TO_DATE(l.RESULT_DATE) BETWEEN a.start_5yr AND a.anchor_date
     )`
  : `lab AS (SELECT NULL AS PATID WHERE 1=0)`;

const procAnySql = hasProc
  ? `proc_any AS (
       SELECT DISTINCT PATID
       FROM ${pro}
       WHERE PATID IS NOT NULL AND TRIM(PATID::STRING) <> ''''
     )`
  : `proc_any AS (SELECT NULL AS PATID WHERE 1=0)`;

const rs = q(
  `WITH anchor AS (
     SELECT CURRENT_DATE() AS anchor_date,
            DATEADD(YEAR, -5, CURRENT_DATE()) AS start_5yr,
            DATEADD(YEAR, -1, CURRENT_DATE()) AS start_1yr
   ),
   demo AS (
     SELECT DISTINCT PATID
     FROM ${demo}
     WHERE PATID IS NOT NULL AND TRIM(PATID::STRING) <> ''''
   ),
   enc_5yr AS (
     SELECT DISTINCT e.PATID
     FROM ${enc} e
     JOIN anchor a ON 1=1
     WHERE e.PATID IS NOT NULL
       AND TRIM(e.PATID::STRING) <> ''''
       AND TRY_TO_DATE(e.ADMIT_DATE) BETWEEN a.start_5yr AND a.anchor_date
       AND UPPER(e.ENC_TYPE::STRING) IN (''ED'',''EI'',''IP'',''OS'',''AV'')
   ),
   enc_1yr AS (
     SELECT DISTINCT e.PATID
     FROM ${enc} e
     JOIN anchor a ON 1=1
     WHERE e.PATID IS NOT NULL
       AND TRIM(e.PATID::STRING) <> ''''
       AND TRY_TO_DATE(e.ADMIT_DATE) BETWEEN a.start_1yr AND a.anchor_date
       AND UPPER(e.ENC_TYPE::STRING) IN (''ED'',''EI'',''IP'',''OS'',''AV'')
   ),
   enc_any AS (
     SELECT DISTINCT PATID
     FROM ${enc}
     WHERE PATID IS NOT NULL AND TRIM(PATID::STRING) <> ''''
   ),
   dx_5yr_ftf AS (
     SELECT DISTINCT d.PATID
     FROM ${dia} d
     JOIN anchor a ON 1=1
     WHERE d.PATID IS NOT NULL
       AND TRIM(d.PATID::STRING) <> ''''
       AND TRY_TO_DATE(d.DX_DATE) BETWEEN a.start_5yr AND a.anchor_date
       AND UPPER(d.ENC_TYPE::STRING) IN (''ED'',''EI'',''IP'',''OS'',''AV'')
   ),
   dx_any AS (
     SELECT DISTINCT PATID
     FROM ${dia}
     WHERE PATID IS NOT NULL AND TRIM(PATID::STRING) <> ''''
   ),
   vital_5yr AS (
     SELECT DISTINCT v.PATID
     FROM ${vit} v
     JOIN anchor a ON 1=1
     WHERE v.PATID IS NOT NULL
       AND TRIM(v.PATID::STRING) <> ''''
       AND TRY_TO_DATE(v.MEASURE_DATE) BETWEEN a.start_5yr AND a.anchor_date
   ),
   ${medUnionSql},
   ${labSql},
   ${procAnySql},
   dx_vital_5yr AS (
     SELECT DISTINCT d.PATID
     FROM dx_5yr_ftf d
     JOIN vital_5yr v
       ON v.PATID = d.PATID
   ),
   dx_vital_med_lab_5yr AS (
     SELECT DISTINCT d.PATID
     FROM dx_5yr_ftf d
     JOIN vital_5yr v
       ON v.PATID = d.PATID
     JOIN med m
       ON m.PATID = d.PATID
     JOIN lab l
       ON l.PATID = d.PATID
   )
   SELECT
     (SELECT COUNT(*) FROM demo) AS dem_n,
     (SELECT COUNT(*) FROM enc_5yr) AS enc_5yr_n,
     (SELECT COUNT(*) FROM enc_1yr) AS enc_1yr_n,
     (SELECT COUNT(*) FROM dx_vital_5yr) AS dx_vital_5yr_n,
     (SELECT COUNT(*) FROM dx_vital_med_lab_5yr) AS dx_vital_med_lab_5yr_n,
     (SELECT COUNT(*) FROM enc_any) AS enc_any_n,
     (SELECT COUNT(*) FROM dx_any) AS dx_any_n,
     (SELECT COUNT(*) FROM proc_any) AS proc_any_n
  `
);
rs.next();

const demN = Number(rs.getColumnValue(1));
const enc5N = Number(rs.getColumnValue(2));
const enc1N = Number(rs.getColumnValue(3));
const dxVital5N = Number(rs.getColumnValue(4));
const dxVitalMedLab5N = Number(rs.getColumnValue(5));
const encAnyN = Number(rs.getColumnValue(6));
const dxAnyN = Number(rs.getColumnValue(7));
const procAnyN = Number(rs.getColumnValue(8));

const pctDxVital5 = (enc5N > 0) ? (dxVital5N / enc5N) * 100.0 : null;
const flag209 = (pctDxVital5 !== null && pctDxVital5 < thresholdPctLt) ? 1 : 0;

const pctDx = (encAnyN > 0) ? (dxAnyN / encAnyN) * 100.0 : null;
const flag304 = (pctDx !== null && pctDx < thresholdPctDxProc) ? 1 : 0;

const pctProc = (encAnyN > 0) ? (procAnyN / encAnyN) * 100.0 : null;
const flag305 = (pctProc !== null && pctProc < thresholdPctDxProc) ? 1 : 0;

const details = {
  anchor_date: anchorDateStr,
  window_years: 5,
  ftf_enc_types: ["ED","EI","IP","OS","AV"],
  dem_distinct_patid_n: demN,
  enc_ftf_distinct_patid_5yr_n: enc5N,
  enc_ftf_distinct_patid_1yr_n: enc1N,
  dx_vital_ftf_distinct_patid_5yr_n: dxVital5N,
  dx_vital_med_lab_ftf_distinct_patid_5yr_n: dxVitalMedLab5N,
  pct_enc_ftf_5yr_with_dx_and_vital: pctDxVital5,
  threshold_pct_lt_209: thresholdPctLt,
  enc_any_distinct_patid_n: encAnyN,
  dx_any_distinct_patid_n: dxAnyN,
  proc_any_distinct_patid_n: procAnyN,
  pct_enc_any_with_dx: pctDx,
  pct_enc_any_with_proc: pctProc,
  threshold_pct_lt_304_305: thresholdPctDxProc,
  definition: ''Potential pools of patients (Table IB-style metrics) using CURRENT_DATE as anchor.''
};

insertMetric(resultsTbl, bindsBase, edcTable, ''DEMOGRAPHIC'', ''ALL'', ''DEM_L3_N'', demN, String(demN), 0, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER'', ''ALL'', ''ENC_L3_DASH2_5YR_N'', enc5N, String(enc5N), 0, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER'', ''ALL'', ''ENC_L3_DASH2_1YR_N'', enc1N, String(enc1N), 0, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''ALL'', ''ALL'', ''XTBL_L3_DASH1_N'', dxVital5N, String(dxVital5N), 0, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''ALL'', ''ALL'', ''PCT_ENC_FTF_5YR_WITH_DX_AND_VITAL'', pctDxVital5, (pctDxVital5 === null ? null : String(pctDxVital5)), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''ALL'', ''ALL'', ''PCT_ENC_FTF_5YR_WITH_DX_AND_VITAL_FLAG'', flag209, String(flag209), thresholdPctLt, (flag209 === 1), details);
insertMetric(resultsTbl, bindsBase, edcTable, ''ALL'', ''ALL'', ''XTBL_L3_DASH3_N'', dxVitalMedLab5N, String(dxVitalMedLab5N), 0, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER'', ''ALL'', ''ENC_L3_N'', encAnyN, String(encAnyN), 0, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''DIAGNOSIS'', ''ALL'', ''DIA_L3_N'', dxAnyN, String(dxAnyN), 0, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''PROCEDURES'', ''ALL'', ''PRO_L3_N'', procAnyN, String(procAnyN), 0, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER;DIAGNOSIS'', ''ALL'', ''PCT_ENC_ANY_WITH_DX'', pctDx, (pctDx === null ? null : String(pctDx)), thresholdPctDxProc, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER;DIAGNOSIS'', ''ALL'', ''PCT_ENC_ANY_WITH_DX_FLAG'', flag304, String(flag304), thresholdPctDxProc, (flag304 === 1), details);
insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER;PROCEDURES'', ''ALL'', ''PCT_ENC_ANY_WITH_PROC'', pctProc, (pctProc === null ? null : String(pctProc)), thresholdPctDxProc, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''ENCOUNTER;PROCEDURES'', ''ALL'', ''PCT_ENC_ANY_WITH_PROC_FLAG'', flag305, String(flag305), thresholdPctDxProc, (flag305 === 1), details);

insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''OK'', thresholdPctLt, false,
  { target_table: only }
);

return `DC 2.09 finished RUN_ID=${RUN_ID} anchor_date=${anchorDateStr}`;
';
