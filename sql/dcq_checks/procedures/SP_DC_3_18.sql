CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_18"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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

const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 3.18;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();

// TARGET_TABLE is honored for compatibility with the orchestrator but this
// is an XTBL (cross-table) check anchored on ENCOUNTER + LDS_ADDRESS_HISTORY.
// Accepted values: ALL, LDS_ADDRESS_HISTORY, ENCOUNTER.
if (!(only === ''ALL'' || only === ''LDS_ADDRESS_HISTORY'' || only === ''ENCOUNTER'')) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL, LDS_ADDRESS_HISTORY, or ENCOUNTER.`);
}

const thresholdPctLt = 80.0;
// SAS-faithful cohort window: F2F ENCOUNTER admits between (today - 63 months, today - 3 months).
// SAS %dash macro derives cutoff_dash = today - 3 months, cutoff_dashyr5 = cutoff_dash - 5 years,
// so the 5-year window is admit_date in [today - 63mo, today - 3mo].
const cohortEndMonths = -3;
const cohortStartMonths = -63;
const f2fEncTypes = [''ED'',''EI'',''IP'',''OS'',''AV''];

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

// Pre-flight: verify the tables and columns we need exist.
const missing = [];
if (!tableExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'')) {
  missing.push(''ENCOUNTER (table)'');
} else {
  if (!colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''PATID'')) missing.push(''ENCOUNTER.PATID'');
  if (!colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ADMIT_DATE'')) missing.push(''ENCOUNTER.ADMIT_DATE'');
  if (!colExists(DB_PARAM, SCHEMA_NAME, ''ENCOUNTER'', ''ENC_TYPE'')) missing.push(''ENCOUNTER.ENC_TYPE'');
}
if (!tableExists(DB_PARAM, SCHEMA_NAME, ''LDS_ADDRESS_HISTORY'')) {
  missing.push(''LDS_ADDRESS_HISTORY (table)'');
} else {
  const need = [''PATID'',''ADDRESS_ZIP5'',''COUNTY_FIPS'',''STATE_FIPS'',''CURRENT_ADDRESS_FLAG''];
  for (const c of need) {
    if (!colExists(DB_PARAM, SCHEMA_NAME, ''LDS_ADDRESS_HISTORY'', c)) {
      missing.push(`LDS_ADDRESS_HISTORY.${c}`);
    }
  }
}
if (missing.length > 0) {
  insertMetric(resultsTbl, bindsBase, edcTable, ''LDS_ADDRESS_HISTORY'', ''XTBL_L3_CFIPSZIP_5Y'', ''STATUS'',
    null, ''ERROR'', thresholdPctLt, true,
    { message: `Missing required tables/columns: ${missing.join('', '')}`, missing: missing });
  return ''DC 3.18 ERROR: missing required tables/columns'';
}

const encFq = `${DB_PARAM}.${SCHEMA_NAME}.ENCOUNTER`;
const ldsFq = `${DB_PARAM}.${SCHEMA_NAME}.LDS_ADDRESS_HISTORY`;
const encTypeList = f2fEncTypes.map(t => `''${t}''`).join('','');

// Build the SAS-equivalent CFIPSZIP_5Y cohort and derived flags in one query.
//
// SAS logic (data_curation_tables.sas ~1140-1200, ~4664-4726):
//   valid_address_zip5 = notdigit(strip(address_zip5))=0 AND length(address_zip5)=5
//   valid_county_fips  = notdigit(strip(county_fips))=0 AND length(county_fips)=5
//                        AND substr(strip(county_fips),1,2)=strip(state_fips)
//   LDS_ADDRESS_HISTORY_F2F_CURRENT = rows with current_address_flag=''Y''
//     AND (valid_address_zip5=''Y'' OR missing OR valid_county_fips=''Y'' OR missing OR ...)
//   ENCOUNTER_PERIOD 5 yrs cohort = distinct PATIDs with enc_type in F2F list
//     AND admit_date between cutoff_dashyr5 and cutoff_dash
//   Output metrics per SAS %tag: distinct_patid_n counts for patients, address_zip5,
//     county_fips, and both.
const sql = `
WITH lds AS (
  SELECT
    PATID,
    ADDRESS_ZIP5,
    COUNTY_FIPS,
    STATE_FIPS,
    CURRENT_ADDRESS_FLAG,
    CASE
      WHEN ADDRESS_ZIP5 IS NOT NULL
       AND LENGTH(TRIM(ADDRESS_ZIP5::STRING)) = 5
       AND REGEXP_LIKE(TRIM(ADDRESS_ZIP5::STRING), ''^[0-9]{5}$'')
      THEN 1 ELSE 0
    END AS valid_address_zip5,
    CASE
      WHEN COUNTY_FIPS IS NOT NULL
       AND LENGTH(TRIM(COUNTY_FIPS::STRING)) = 5
       AND REGEXP_LIKE(TRIM(COUNTY_FIPS::STRING), ''^[0-9]{5}$'')
       AND SUBSTR(TRIM(COUNTY_FIPS::STRING), 1, 2) = TRIM(COALESCE(STATE_FIPS::STRING,''''))
      THEN 1 ELSE 0
    END AS valid_county_fips
  FROM ${ldsFq}
  WHERE CURRENT_ADDRESS_FLAG = ''Y''
),
lds_agg AS (
  -- Roll up to one row per PATID (SAS retains flags across rows for same PATID).
  SELECT
    PATID,
    MAX(valid_address_zip5) AS valid_address_zip5,
    MAX(valid_county_fips) AS valid_county_fips
  FROM lds
  GROUP BY PATID
),
enc_5yr AS (
  -- SAS ENCOUNTER_PERIOD dataset (data_curation_tables.sas ~1435-1449) then
  -- ENC_L3_DASH2 (~3967-4015) with distinct=y and period=''5 yrs''. This is the
  -- SAS denom_fipszip used at edc_report.sas line 883-884.
  SELECT DISTINCT PATID
  FROM ${encFq}
  WHERE ENC_TYPE IN (${encTypeList})
    AND ADMIT_DATE IS NOT NULL
    AND TRY_TO_DATE(ADMIT_DATE::STRING) BETWEEN
        DATEADD(month, ${cohortStartMonths}, CURRENT_DATE())
    AND DATEADD(month, ${cohortEndMonths}, CURRENT_DATE())
),
cohort AS (
  -- SAS cfipszip_5y dataset (data_curation_tables.sas 4676-4703).
  -- Merge: lds_address_history_f2f_current (current=''Y'' AND (valid_z=Y OR valid_f=Y))
  --        with encounter_period ''5 yrs'' -- keep only where inl AND ine.
  SELECT
    l.PATID,
    l.valid_address_zip5,
    l.valid_county_fips
  FROM lds_agg l
  JOIN enc_5yr e USING (PATID)
  WHERE l.valid_address_zip5 = 1 OR l.valid_county_fips = 1
)
SELECT
  (SELECT COUNT(*) FROM enc_5yr) AS f2f_denom_n,
  COUNT(DISTINCT PATID) AS intersect_n,
  COUNT(DISTINCT CASE WHEN valid_address_zip5 = 1 THEN PATID END) AS address_zip5_n,
  COUNT(DISTINCT CASE WHEN valid_county_fips = 1 THEN PATID END) AS county_fips_n,
  COUNT(DISTINCT CASE WHEN valid_address_zip5 = 1 AND valid_county_fips = 1 THEN PATID END) AS both_n
FROM cohort
`;

const rs = q(sql);
if (!rs.next()) {
  insertMetric(resultsTbl, bindsBase, edcTable, ''LDS_ADDRESS_HISTORY'', ''XTBL_L3_CFIPSZIP_5Y'', ''STATUS'',
    null, ''ERROR'', thresholdPctLt, true, { message: ''Cohort query returned no rows'' });
  return ''DC 3.18 ERROR: cohort query returned no rows'';
}

// SAS Table IB row 3.18 semantics (edc_report.sas lines 883-905):
//   denom  = ENC_L3_DASH2 period=''5 yrs'' distinct_patid_n
//          = F2F 5yr cohort size BEFORE joining LDS_ADDRESS_HISTORY.
//   numer  = XTBL_L3_CFIPSZIP_5Y variable=''ADDRESS_ZIP5 and COUNTY_FIPS'' distinct_patid_n
//          = intersection (F2F 5yr) AND (LDS current AND valid_zip5 AND valid_fips).
//   pct    = numer/denom * 100 ; threshold fires when pct<80 OR numer=0 (with denom>0).
const patients = Number(rs.getColumnValue(1)) || 0;   // denominator = F2F 5yr cohort (SAS denom_fipszip)
const intersectN = Number(rs.getColumnValue(2)) || 0; // XTBL_L3_CFIPSZIP_5Y PATIENTS distinct_patid_n
const zip5 = Number(rs.getColumnValue(3)) || 0;
const fips = Number(rs.getColumnValue(4)) || 0;
const both = Number(rs.getColumnValue(5)) || 0;
const pctBoth = (patients > 0) ? (both / patients) * 100.0 : null;
// Exception when denom>0 and (pct < 80 OR numerator = 0). Mirrors edc_report.sas Table IB row 3.18.
const flag = (patients > 0 && ((pctBoth !== null && pctBoth < thresholdPctLt) || both === 0)) ? 1 : 0;

const details = {
  table: ''XTBL_L3_CFIPSZIP_5Y'',
  cohort_period: ''5 yrs (F2F encounters)'',
  cohort_window: {
    admit_date_from: `DATEADD(month, ${cohortStartMonths}, CURRENT_DATE)`,
    admit_date_to: `DATEADD(month, ${cohortEndMonths}, CURRENT_DATE)`,
    f2f_enc_types: f2fEncTypes
  },
  lds_filter: {
    current_address_flag: ''Y'',
    valid_address_zip5: ''digits-only AND length=5'',
    valid_county_fips: ''digits-only AND length=5 AND first 2 chars = STATE_FIPS''
  },
  counts: {
    patients_n: patients,
    intersect_lds_valid_n: intersectN,
    address_zip5_n: zip5,
    county_fips_n: fips,
    both_n: both
  },
  pct_both: pctBoth,
  threshold_pct_lt: thresholdPctLt,
  definition: ''SAS Table IB row 3.18: numerator = distinct PATIDs whose CURRENT LDS_ADDRESS_HISTORY row has BOTH a valid ADDRESS_ZIP5 AND a valid COUNTY_FIPS, among the denominator; denominator = distinct PATIDs with an F2F encounter (ED/EI/IP/OS/AV) in the past 5 years (SAS denom_fipszip = ENC_L3_DASH2 period 5yrs distinct_patid_n).'',
  reference: ''PCORnet CDM DC 3.18 / Table IB / XTBL_L3_CFIPSZIP_5Y (SAS data_curation_tables.sas v16 lines 4664-4726; edc_report.sas v16 lines 878-908).''
};

const codeType = ''XTBL_L3_CFIPSZIP_5Y'';
// PATIENTS_N is the SAS Table IB row 3.18 denominator = distinct F2F 5yr PATIDs.
insertMetric(resultsTbl, bindsBase, edcTable, ''LDS_ADDRESS_HISTORY'', codeType, ''PATIENTS_N'', patients, String(patients), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''LDS_ADDRESS_HISTORY'', codeType, ''ADDRESS_ZIP5_N'', zip5, String(zip5), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''LDS_ADDRESS_HISTORY'', codeType, ''COUNTY_FIPS_N'', fips, String(fips), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''LDS_ADDRESS_HISTORY'', codeType, ''BOTH_N'', both, String(both), thresholdPctLt, false, details);
// INTERSECT_LDS_VALID_N = SAS XTBL_L3_CFIPSZIP_5Y PATIENTS distinct_patid_n
// (kept for traceability - this is the count of PATIDs in the merged cfipszip_5y dataset).
insertMetric(resultsTbl, bindsBase, edcTable, ''LDS_ADDRESS_HISTORY'', codeType, ''INTERSECT_LDS_VALID_N'', intersectN, String(intersectN), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''LDS_ADDRESS_HISTORY'', codeType, ''PCT_BOTH'', pctBoth,
  (pctBoth === null ? null : String(pctBoth)), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, edcTable, ''LDS_ADDRESS_HISTORY'', codeType, ''BOTH_FIPSZIP_FLAG'',
  flag, String(flag), thresholdPctLt, (flag === 1), details);

insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'',
  null, ''OK'', thresholdPctLt, false,
  { target_table: only, patients_n: patients, intersect_lds_valid_n: intersectN, both_n: both, pct_both: pctBoth });

return `DC 3.18 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} patients_n=${patients} both_n=${both} pct_both=${pctBoth}`;
';
