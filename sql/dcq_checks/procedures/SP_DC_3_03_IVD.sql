CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_03_IVD"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
// -----------------------------------------------------------------------
// DC 3.03 Table IVD: Missing or Unknown Values
// -----------------------------------------------------------------------
// Ports SAS edc_report.sas Table IVD block. For each field:
//   - "category" kind:        count NULL/blank + values in (NI, UN, OT)
//   - "null_n" kind:          count NULL/blank only (ID fields)
//   - "nmiss" kind:           count NULL only (date/time/numeric fields)
//   - "null_or_missing" kind: count NULL/blank only (SAS "NULL or missing" category-only)
//   - "cross_null_or_missing":same as null_or_missing (LAB_L3_PX_PXTYPE special)
//   - "stat_null_or_missing": count NULL/blank only (numeric distribution stat)
// Denominator:
//   - "none" : COUNT(*) of parent table
//   - "ip_ei": COUNT_IF(UPPER(TRIM(ENC_TYPE)) IN (IP, EI)) on ENCOUNTER
// A field is flagged (EXCEPTION_FLAG=TRUE) when missing_pct >= 10 AND field is in the
// SAS-specified flag list for IVD (see catalog "flag" property below).
// -----------------------------------------------------------------------

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
      ?, IFF(?=1, TRUE, FALSE), PARSE_JSON(?)
    `,
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

const outSchema  = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum     = 3.03;
const edcTable   = ''Table IVD'';
const variant    = ''IVD''; // for logging
const thresholdPct = 10; // SAS flags when perc >= 10.00
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();

q(`CREATE SCHEMA IF NOT EXISTS ${outSchema}`);
q(`CREATE TABLE IF NOT EXISTS ${resultsTbl} (
  RUN_ID STRING, CHECK_ID STRING, CHECK_NAME STRING, ROW_NUM NUMBER(10,2), EDC_TABLE STRING,
  SOURCE_TABLE STRING, CODE_TYPE STRING, METRIC STRING, VALUE_NUM NUMBER(38,10), VALUE_STR STRING,
  THRESHOLD_NUM NUMBER(38,10), EXCEPTION_FLAG BOOLEAN, DETAILS VARIANT,
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)`);

// Fetch registry row for this specific EDC_TABLE variant. Both DC_3_03_IVC and DC_3_03_IVD
// have distinct CHECK_ID / CHECK_NAME in the registry so we filter by CHECK_NAME.
const checkNameFilter = ''DC_3_03_IVD'';
const regRs = q(
  `SELECT CHECK_ID, CHECK_NAME
   FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY
   WHERE CHECK_NAME = ?
   QUALIFY ROW_NUMBER() OVER (ORDER BY CHECK_ID) = 1`,
  [checkNameFilter]
);
if (!regRs.next()) throw new Error(`No registry row found for CHECK_NAME=${checkNameFilter}`);
const checkId   = regRs.getColumnValue(1);
const checkName = regRs.getColumnValue(2);
const bindsBase = [RUN_ID, checkId, checkName, rowNum];

// Idempotency: delete only THIS variant''s rows for this RUN_ID (scoped by EDC_TABLE)
if (only === ''ALL'') {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ? AND EDC_TABLE = ?`, [RUN_ID, rowNum, edcTable]);
} else {
  if (!isSafeIdentPart(only)) throw new Error(`Invalid TARGET_TABLE: ${TARGET_TABLE}`);
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ? AND EDC_TABLE = ? AND UPPER(SOURCE_TABLE) = ?`, [RUN_ID, rowNum, edcTable, only]);
}

// -----------------------------------------------------------------------
// Field catalog (auto-generated from SAS edc_report.sas newest via
// sas_code/parse_dc_3_03_catalog.py)
// -----------------------------------------------------------------------
const catalog = [
  { tbl: ''VITAL'', col: ''ENCOUNTERID'', sas_var: ''VITAL_ENCOUNTERID'', dc_name: ''VIT_L3_N'', kind: ''null_n'', denom: ''none'', flag: true },
  { tbl: ''VITAL'', col: ''VITAL_SOURCE'', sas_var: ''VITAL_SOURCE'', dc_name: ''VIT_L3_VITAL_SOURCE'', kind: ''category'', denom: ''none'', flag: true },
  { tbl: ''VITAL'', col: ''MEASURE_TIME'', sas_var: ''MEASURE_TIME'', dc_name: ''XTBL_L3_TIMES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''DEATH'', col: ''DEATH_DATE_IMPUTE'', sas_var: ''DEATH_DATE_IMPUTE'', dc_name: ''DEATH_L3_IMPUTE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEATH'', col: ''DEATH_MATCH_CONFIDENCE'', sas_var: ''DEATH_MATCH_CONFIDENCE'', dc_name: ''DEATH_L3_MATCH'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEATH'', col: ''DEATH_DATE'', sas_var: ''DEATH_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''DEATH'', col: ''DEATH_SOURCE'', sas_var: ''DEATH_SOURCE'', dc_name: ''DEATH_L3_SOURCE'', kind: ''category'', denom: ''none'', flag: true },
  { tbl: ''LAB_RESULT_CM'', col: ''ENCOUNTERID'', sas_var: ''ENCOUNTERID'', dc_name: ''LAB_L3_N'', kind: ''null_n'', denom: ''none'', flag: true },
  { tbl: ''LAB_RESULT_CM'', col: ''SPECIMEN_SOURCE'', sas_var: ''SPECIMEN_SOURCE'', dc_name: ''LAB_L3_SOURCE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''LAB_LOINC'', sas_var: ''LAB_LOINC'', dc_name: ''LAB_L3_LOINC'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''PRIORITY'', sas_var: ''PRIORITY'', dc_name: ''LAB_L3_PRIORITY'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''RESULT_LOC'', sas_var: ''RESULT_LOC'', dc_name: ''LAB_L3_LOC'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''LAB_PX_TYPE'', sas_var: ''LAB_PX_TYPE'', dc_name: ''LAB_L3_PX_TYPE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''LAB_PX'', sas_var: ''LAB_PX'', dc_name: ''LAB_L3_PX_PXTYPE'', kind: ''cross_null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''LAB_ORDER_DATE'', sas_var: ''LAB_ORDER_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''SPECIMEN_DATE'', sas_var: ''SPECIMEN_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''SPECIMEN_TIME'', sas_var: ''SPECIMEN_TIME'', dc_name: ''XTBL_L3_TIMES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''RESULT_TIME'', sas_var: ''RESULT_TIME'', dc_name: ''XTBL_L3_TIMES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''RESULT_QUAL'', sas_var: ''RESULT_QUAL'', dc_name: ''LAB_L3_QUAL'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''RESULT_NUM'', sas_var: ''RESULT_NUM'', dc_name: ''LAB_L3_LOINC_RESULT_NUM'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''RESULT_MODIFIER'', sas_var: ''RESULT_MODIFIER'', dc_name: ''LAB_L3_MOD'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''RESULT_UNIT'', sas_var: ''RESULT_UNIT'', dc_name: ''LAB_L3_UNIT'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''NORM_MODIFIER_LOW'', sas_var: ''NORM_MODIFIER_LOW'', dc_name: ''LAB_L3_LOW'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''NORM_MODIFIER_HIGH'', sas_var: ''NORM_MODIFIER_HIGH'', dc_name: ''LAB_L3_HIGH'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''ABN_IND'', sas_var: ''ABN_IND'', dc_name: ''LAB_L3_ABN'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''LAB_LOINC_SOURCE'', sas_var: ''LAB_LOINC_SOURCE'', dc_name: ''LAB_L3_LSOURCE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_RESULT_CM'', col: ''LAB_RESULT_SOURCE'', sas_var: ''LAB_RESULT_SOURCE'', dc_name: ''LAB_L3_RSOURCE'', kind: ''category'', denom: ''none'', flag: true },
  { tbl: ''LAB_RESULT_CM'', col: ''RESULT_SNOMED'', sas_var: ''RESULT_SNOMED'', dc_name: ''LAB_L3_SNOMED'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''ENCOUNTERID'', sas_var: ''RX_ENCOUNTERID'', dc_name: ''PRES_L3_N'', kind: ''null_n'', denom: ''none'', flag: true },
  { tbl: ''PRESCRIBING'', col: ''RX_PROVIDERID'', sas_var: ''RX_PROVIDERID'', dc_name: ''PRES_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RX_ORDER_DATE'', sas_var: ''RX_ORDER_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: true },
  { tbl: ''PRESCRIBING'', col: ''RX_ORDER_TIME'', sas_var: ''RX_ORDER_TIME'', dc_name: ''XTBL_L3_TIMES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RX_START_DATE'', sas_var: ''RX_START_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RX_END_DATE'', sas_var: ''RX_END_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RX_DAYS_SUPPLY_GROUP'', sas_var: ''RX_DAYS_SUPPLY_GROUP'', dc_name: ''PRES_L3_SUPDIST2'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RX_FREQUENCY'', sas_var: ''RX_FREQUENCY'', dc_name: ''PRES_L3_FREQ'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RX_BASIS'', sas_var: ''RX_BASIS'', dc_name: ''PRES_L3_BASIS'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RXNORM_CUI'', sas_var: ''RXNORM_CUI'', dc_name: ''PRES_L3_RXCUI'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RX_QUANTITY'', sas_var: ''RX_QUANTITY'', dc_name: ''PRES_L3_RXQTY_DIST'', kind: ''stat_null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RX_REFILLS'', sas_var: ''RX_REFILLS'', dc_name: ''PRES_L3_RXREFILL_DIST'', kind: ''stat_null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RX_DOSE_ORDERED'', sas_var: ''RX_DOSE_ORDERED'', dc_name: ''PRES_L3_RXDOSEODR_DIST'', kind: ''stat_null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RX_DOSE_ORDERED_UNIT'', sas_var: ''RX_DOSE_ORDERED_UNIT'', dc_name: ''PRES_L3_RXDOSEODRUNIT'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RX_DOSE_FORM'', sas_var: ''RX_DOSE_FORM'', dc_name: ''PRES_L3_RXDOSEFORM'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RX_PRN_FLAG'', sas_var: ''RX_PRN_FLAG'', dc_name: ''PRES_L3_PRNFLAG'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RX_ROUTE'', sas_var: ''RX_ROUTE'', dc_name: ''PRES_L3_ROUTE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''PRESCRIBING'', col: ''RX_SOURCE'', sas_var: ''RX_SOURCE'', dc_name: ''PRES_L3_SOURCE'', kind: ''category'', denom: ''none'', flag: true },
  { tbl: ''PRESCRIBING'', col: ''RX_DISPENSE_AS_WRITTEN'', sas_var: ''RX_DISPENSE_AS_WRITTEN'', dc_name: ''PRES_L3_DISPASWRTN'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DISPENSING'', col: ''PRESCRIBINGID'', sas_var: ''PRESCRIBINGID'', dc_name: ''DISP_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''DISPENSING'', col: ''DISPENSE_SUP_GROUP'', sas_var: ''DISPENSE_SUP_GROUP'', dc_name: ''DISP_L3_SUPDIST2'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''DISPENSING'', col: ''DISPENSE_DOSE_DISP'', sas_var: ''DISPENSE_DOSE_DISP'', dc_name: ''DISP_L3_DOSE_DIST'', kind: ''stat_null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''DISPENSING'', col: ''DISPENSE_ROUTE'', sas_var: ''DISPENSE_ROUTE'', dc_name: ''DISP_L3_ROUTE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DISPENSING'', col: ''DISPENSE_SOURCE'', sas_var: ''DISPENSE_SOURCE'', dc_name: ''DISP_L3_SOURCE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DISPENSING'', col: ''DISPENSE_AMT'', sas_var: ''DISPENSE_AMT'', dc_name: ''DISP_L3_DISPAMT_DIST'', kind: ''stat_null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''CONDITION'', col: ''REPORT_DATE'', sas_var: ''REPORT_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''CONDITION'', col: ''RESOLVE_DATE'', sas_var: ''RESOLVE_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''CONDITION'', col: ''ONSET_DATE'', sas_var: ''ONSET_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''CONDITION'', col: ''CONDITION_STATUS'', sas_var: ''CONDITION_STATUS'', dc_name: ''COND_L3_STATUS'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''CONDITION'', col: ''CONDITION_SOURCE'', sas_var: ''CONDITION_SOURCE'', dc_name: ''COND_L3_SOURCE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''CONDITION'', col: ''CONDITION_TYPE'', sas_var: ''CONDITION_TYPE'', dc_name: ''COND_L3_TYPE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''PROVIDER'', col: ''PROVIDER_NPI'', sas_var: ''PROVIDER_NPI'', dc_name: ''PROV_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''PROVIDER'', col: ''PROVIDER_NPI_FLAG'', sas_var: ''PROVIDER_NPI_FLAG'', dc_name: ''PROV_L3_NPIFLAG'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''PROVIDER'', col: ''PROVIDER_SPECIALTY_PRIMARY'', sas_var: ''PROVIDER_SPECIALTY_PRIMARY'', dc_name: ''PROV_L3_SPECIALTY'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''PROVIDER'', col: ''PROVIDER_SEX'', sas_var: ''PROVIDER_SEX'', dc_name: ''PROV_L3_SEX'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''MED_ADMIN'', col: ''ENCOUNTERID'', sas_var: ''MEDADMIN_ENCOUNTERID'', dc_name: ''MEDADM_L3_N'', kind: ''null_n'', denom: ''none'', flag: true },
  { tbl: ''MED_ADMIN'', col: ''PRESCRIBINGID'', sas_var: ''MEDADMIN_PRESCRIBINGID'', dc_name: ''MEDADM_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''MED_ADMIN'', col: ''MEDADMIN_PROVIDERID'', sas_var: ''MEDADMIN_PROVIDERID'', dc_name: ''MEDADM_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''MED_ADMIN'', col: ''MEDADMIN_DOSE_ADMIN'', sas_var: ''MEDADMIN_DOSE_ADMIN'', dc_name: ''MEDADM_L3_DOSEADM'', kind: ''stat_null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''MED_ADMIN'', col: ''MEDADMIN_DOSE_ADMIN_UNIT'', sas_var: ''MEDADMIN_DOSE_ADMIN_UNIT'', dc_name: ''MEDADM_L3_DOSEADMUNIT'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''MED_ADMIN'', col: ''MEDADMIN_ROUTE'', sas_var: ''MEDADMIN_ROUTE'', dc_name: ''MEDADM_L3_ROUTE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''MED_ADMIN'', col: ''MEDADMIN_SOURCE'', sas_var: ''MEDADMIN_SOURCE'', dc_name: ''MEDADM_L3_SOURCE'', kind: ''category'', denom: ''none'', flag: true },
  { tbl: ''MED_ADMIN'', col: ''MEDADMIN_CODE'', sas_var: ''MEDADMIN_CODE'', dc_name: ''MEDADM_L3_CODE_TYPE'', kind: ''null_or_missing'', denom: ''none'', flag: true },
  { tbl: ''MED_ADMIN'', col: ''MEDADMIN_START_TIME'', sas_var: ''MEDADMIN_START_TIME'', dc_name: ''XTBL_L3_TIMES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''MED_ADMIN'', col: ''MEDADMIN_STOP_DATE'', sas_var: ''MEDADMIN_STOP_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''MED_ADMIN'', col: ''MEDADMIN_STOP_TIME'', sas_var: ''MEDADMIN_STOP_TIME'', dc_name: ''XTBL_L3_TIMES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''MED_ADMIN'', col: ''MEDADMIN_TYPE'', sas_var: ''MEDADMIN_TYPE'', dc_name: ''MEDADM_L3_TYPE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''OBS_CLIN'', col: ''ENCOUNTERID'', sas_var: ''OBSCLIN_ENCOUNTERID'', dc_name: ''OBSCLIN_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''OBS_CLIN'', col: ''OBSCLIN_PROVIDERID'', sas_var: ''OBSCLIN_PROVIDERID'', dc_name: ''OBSCLIN_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''OBS_CLIN'', col: ''OBSCLIN_SOURCE'', sas_var: ''OBSCLIN_SOURCE'', dc_name: ''OBSCLIN_L3_SOURCE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''OBS_CLIN'', col: ''OBSCLIN_START_TIME'', sas_var: ''OBSCLIN_START_TIME'', dc_name: ''XTBL_L3_TIMES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''OBS_CLIN'', col: ''OBSCLIN_STOP_DATE'', sas_var: ''OBSCLIN_STOP_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''OBS_CLIN'', col: ''OBSCLIN_STOP_TIME'', sas_var: ''OBSCLIN_STOP_TIME'', dc_name: ''XTBL_L3_TIMES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''OBS_CLIN'', col: ''OBSCLIN_CODE'', sas_var: ''OBSCLIN_CODE'', dc_name: ''OBSCLIN_L3_CODE_TYPE'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''OBS_CLIN'', col: ''OBSCLIN_TYPE'', sas_var: ''OBSCLIN_TYPE'', dc_name: ''OBSCLIN_L3_TYPE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''OBS_CLIN'', col: ''OBSCLIN_RESULT_QUAL'', sas_var: ''OBSCLIN_RESULT_QUAL'', dc_name: ''OBSCLIN_L3_QUAL'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''OBS_CLIN'', col: ''OBSCLIN_RESULT_MODIFIER'', sas_var: ''OBSCLIN_RESULT_MODIFIER'', dc_name: ''OBSCLIN_L3_MOD'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''OBS_CLIN'', col: ''OBSCLIN_ABN_IND'', sas_var: ''OBSCLIN_ABN_IND'', dc_name: ''OBSCLIN_L3_ABN'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''OBS_GEN'', col: ''ENCOUNTERID'', sas_var: ''OBSGEN_ENCOUNTERID'', dc_name: ''OBSGEN_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''OBS_GEN'', col: ''OBSGEN_PROVIDERID'', sas_var: ''OBSGEN_PROVIDERID'', dc_name: ''OBSGEN_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''OBS_GEN'', col: ''OBSGEN_SOURCE'', sas_var: ''OBSGEN_SOURCE'', dc_name: ''OBSGEN_L3_SOURCE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''OBS_GEN'', col: ''OBSGEN_START_TIME'', sas_var: ''OBSGEN_START_TIME'', dc_name: ''XTBL_L3_TIMES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''OBS_GEN'', col: ''OBSGEN_STOP_DATE'', sas_var: ''OBSGEN_STOP_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''OBS_GEN'', col: ''OBSGEN_STOP_TIME'', sas_var: ''OBSGEN_STOP_TIME'', dc_name: ''XTBL_L3_TIMES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''OBS_GEN'', col: ''OBSGEN_CODE'', sas_var: ''OBSGEN_CODE'', dc_name: ''OBSGEN_L3_CODE_TYPE'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''OBS_GEN'', col: ''OBSGEN_TYPE'', sas_var: ''OBSGEN_TYPE'', dc_name: ''OBSGEN_L3_TYPE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''OBS_GEN'', col: ''OBSGEN_RESULT_QUAL'', sas_var: ''OBSGEN_RESULT_QUAL'', dc_name: ''OBSGEN_L3_QUAL'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''OBS_GEN'', col: ''OBSGEN_RESULT_MODIFIER'', sas_var: ''OBSGEN_RESULT_MODIFIER'', dc_name: ''OBSGEN_L3_MOD'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''OBS_GEN'', col: ''OBSGEN_ABN_IND'', sas_var: ''OBSGEN_ABN_IND'', dc_name: ''OBSGEN_L3_ABN'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LDS_ADDRESS_HISTORY'', col: ''ADDRESS_USE'', sas_var: ''ADDRESS_USE'', dc_name: ''LDSADRS_L3_ADRSUSE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LDS_ADDRESS_HISTORY'', col: ''ADDRESS_TYPE'', sas_var: ''ADDRESS_TYPE'', dc_name: ''LDSADRS_L3_ADRSTYPE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LDS_ADDRESS_HISTORY'', col: ''COUNTY_FIPS'', sas_var: ''COUNTY_FIPS'', dc_name: ''LDSADRS_L3_COUNTYFIPS'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''LDS_ADDRESS_HISTORY'', col: ''ADDRESS_CITY'', sas_var: ''ADDRESS_CITY'', dc_name: ''LDSADRS_L3_ADRSCITY'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''LDS_ADDRESS_HISTORY'', col: ''ADDRESS_STATE'', sas_var: ''ADDRESS_STATE'', dc_name: ''LDSADRS_L3_ADRSSTATE'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''LDS_ADDRESS_HISTORY'', col: ''ADDRESS_ZIP5'', sas_var: ''ADDRESS_ZIP5'', dc_name: ''LDSADRS_L3_ADRSZIP5'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''LDS_ADDRESS_HISTORY'', col: ''ADDRESS_ZIP9'', sas_var: ''ADDRESS_ZIP9'', dc_name: ''LDSADRS_L3_ADRSZIP9'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''LDS_ADDRESS_HISTORY'', col: ''CURRENT_ADDRESS_FLAG'', sas_var: ''CURRENT_ADDRESS_FLAG'', dc_name: ''LDSADRS_L3_CURRENT'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''LDS_ADDRESS_HISTORY'', col: ''RUCA_ZIP'', sas_var: ''RUCA_ZIP'', dc_name: ''LDSADRS_L3_RUCAZIP'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''LDS_ADDRESS_HISTORY'', col: ''STATE_FIPS'', sas_var: ''STATE_FIPS'', dc_name: ''LDSADRS_L3_STATEFIPS'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''LDS_ADDRESS_HISTORY'', col: ''ADDRESS_COUNTY'', sas_var: ''ADDRESS_COUNTY'', dc_name: ''LDSADRS_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''LDS_ADDRESS_HISTORY'', col: ''ADDRESS_PERIOD_START'', sas_var: ''ADDRESS_PERIOD_START'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''LDS_ADDRESS_HISTORY'', col: ''ADDRESS_PERIOD_END'', sas_var: ''ADDRESS_PERIOD_END'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''ENCOUNTERID'', sas_var: ''IMMUNE_ENCOUNTERID'', dc_name: ''IMMUNE_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''PROCEDURESID'', sas_var: ''PROCEDURESID'', dc_name: ''IMMUNE_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''VX_PROVIDERID'', sas_var: ''VX_PROVIDERID'', dc_name: ''IMMUNE_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''VX_RECORD_DATE'', sas_var: ''VX_RECORD_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''VX_ADMIN_DATE'', sas_var: ''VX_ADMIN_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''VX_CODE_TYPE'', sas_var: ''VX_CODE_TYPE'', dc_name: ''IMMUNE_L3_CODETYPE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''VX_STATUS'', sas_var: ''VX_STATUS'', dc_name: ''IMMUNE_L3_STATUS'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''VX_STATUS_REASON'', sas_var: ''VX_STATUS_REASON'', dc_name: ''IMMUNE_L3_STATUSREASON'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''VX_SOURCE'', sas_var: ''VX_SOURCE'', dc_name: ''IMMUNE_L3_SOURCE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''VX_DOSE'', sas_var: ''VX_DOSE'', dc_name: ''IMMUNE_L3_DOSE_DIST'', kind: ''stat_null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''VX_DOSE_UNIT'', sas_var: ''VX_DOSE_UNIT'', dc_name: ''IMMUNE_L3_DOSEUNIT'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''VX_ROUTE'', sas_var: ''VX_ROUTE'', dc_name: ''IMMUNE_L3_ROUTE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''VX_BODY_SITE'', sas_var: ''VX_BODY_SITE'', dc_name: ''IMMUNE_L3_BODYSITE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''VX_MANUFACTURER'', sas_var: ''VX_MANUFACTURER'', dc_name: ''IMMUNE_L3_MANUFACTURER'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''VX_LOT_NUM'', sas_var: ''VX_LOT_NUM'', dc_name: ''IMMUNE_L3_LOTNUM'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''IMMUNIZATION'', col: ''VX_EXP_DATE'', sas_var: ''VX_EXP_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''LAB_HISTORY'', col: ''LAB_FACILITYID'', sas_var: ''LAB_FACILITYID'', dc_name: ''LABHIST_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''LAB_HISTORY'', col: ''SEX'', sas_var: ''SEX'', dc_name: ''LABHIST_L3_SEXDIST'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''LAB_HISTORY'', col: ''RACE'', sas_var: ''RACE'', dc_name: ''LABHIST_L3_RACEDIST'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_HISTORY'', col: ''AGE_MIN_WKS'', sas_var: ''AGE_MIN_WKS'', dc_name: ''LABHIST_L3_MIN_WKS'', kind: ''stat_null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''LAB_HISTORY'', col: ''AGE_MAX_WKS'', sas_var: ''AGE_MAX_WKS'', dc_name: ''LABHIST_L3_MAX_WKS'', kind: ''stat_null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''LAB_HISTORY'', col: ''RESULT_UNIT'', sas_var: ''LH_RESULT_UNIT'', dc_name: ''LABHIST_L3_UNIT'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_HISTORY'', col: ''NORM_MODIFIER_LOW'', sas_var: ''LH_NORM_MODIFIER_LOW'', dc_name: ''LABHIST_L3_LOW'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_HISTORY'', col: ''NORM_MODIFIER_HIGH'', sas_var: ''LH_NORM_MODIFIER_HIGH'', dc_name: ''LABHIST_L3_HIGH'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''LAB_HISTORY'', col: ''PERIOD_START'', sas_var: ''PERIOD_START'', dc_name: ''LABHIST_L3_PDSTART_Y'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''LAB_HISTORY'', col: ''PERIOD_END'', sas_var: ''PERIOD_END'', dc_name: ''LABHIST_L3_PDEND_Y'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''EXTERNAL_MEDS'', col: ''EXT_DOSE_FORM'', sas_var: ''EXT_DOSE_FORM'', dc_name: ''EXTMED_L3_RXDOSEFORM'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''EXTERNAL_MEDS'', col: ''EXT_DOSE_ORDERED_UNIT'', sas_var: ''EXT_DOSE_ORDERED_UNIT'', dc_name: ''EXTMED_L3_RXDOSE_UNIT'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''EXTERNAL_MEDS'', col: ''EXT_ROUTE'', sas_var: ''EXT_ROUTE'', dc_name: ''EXTMED_L3_ROUTE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''EXTERNAL_MEDS'', col: ''RXNORM_CUI'', sas_var: ''EXTMED_RXNORM_CUI'', dc_name: ''EXTMED_L3_RXCUI'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''EXTERNAL_MEDS'', col: ''EXTMED_SOURCE'', sas_var: ''EXTMED_SOURCE'', dc_name: ''EXTMED_L3_SOURCE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''EXTERNAL_MEDS'', col: ''EXT_BASIS'', sas_var: ''EXT_BASIS'', dc_name: ''EXTMED_L3_BASIS'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''EXTERNAL_MEDS'', col: ''EXT_PAT_START_DATE'', sas_var: ''EXT_PAT_START_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''EXTERNAL_MEDS'', col: ''EXT_END_DATE'', sas_var: ''EXT_END_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''EXTERNAL_MEDS'', col: ''EXT_PAT_END_DATE'', sas_var: ''EXT_PAT_END_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''PAT_RELATIONSHIP'', col: ''RELATIONSHIP_START'', sas_var: ''RELATIONSHIP_START'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''PAT_RELATIONSHIP'', col: ''RELATIONSHIP_END'', sas_var: ''RELATIONSHIP_END'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''PRO_CM'', col: ''PRO_CAT'', sas_var: ''PRO_CAT'', dc_name: ''PROCM_L3_CAT'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''PRO_CM'', col: ''PRO_CODE'', sas_var: ''PRO_CODE'', dc_name: ''PROCM_L3_CODE_TYPE'', kind: ''null_or_missing'', denom: ''none'', flag: true },
  { tbl: ''PRO_CM'', col: ''PRO_FULLNAME'', sas_var: ''PRO_FULLNAME'', dc_name: ''PROCM_L3_FULLNAME'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''PRO_CM'', col: ''PRO_METHOD'', sas_var: ''PRO_METHOD'', dc_name: ''PROCM_L3_METHOD'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''PRO_CM'', col: ''PRO_MODE'', sas_var: ''PRO_MODE'', dc_name: ''PROCM_L3_MODE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''PRO_CM'', col: ''PRO_NAME'', sas_var: ''PRO_NAME'', dc_name: ''PROCM_L3_NAME'', kind: ''null_or_missing'', denom: ''none'', flag: false },
  { tbl: ''PRO_CM'', col: ''PRO_SOURCE'', sas_var: ''PRO_SOURCE'', dc_name: ''PROCM_L3_SOURCE'', kind: ''category'', denom: ''none'', flag: true },
  { tbl: ''PRO_CM'', col: ''PRO_TYPE'', sas_var: ''PRO_TYPE'', dc_name: ''PROCM_L3_TYPE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''PRO_CM'', col: ''ENCOUNTERID'', sas_var: ''PROCM_ENCOUNTERID'', dc_name: ''PROCM_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''PRO_CM'', col: ''PRO_TIME'', sas_var: ''PRO_TIME'', dc_name: ''XTBL_L3_TIMES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''DEATH_CAUSE'', col: ''DEATH_CAUSE_CODE'', sas_var: ''DEATH_CAUSE_CODE'', dc_name: ''DEATHC_L3_CODE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEATH_CAUSE'', col: ''DEATH_CAUSE_CONFIDENCE'', sas_var: ''DEATH_CAUSE_CONFIDENCE'', dc_name: ''DEATHC_L3_CONF'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEATH_CAUSE'', col: ''DEATH_CAUSE_SOURCE'', sas_var: ''DEATH_CAUSE_SOURCE'', dc_name: ''DEATHC_L3_SOURCE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEATH_CAUSE'', col: ''DEATH_CAUSE_TYPE'', sas_var: ''DEATH_CAUSE_TYPE'', dc_name: ''DEATHC_L3_TYPE'', kind: ''category'', denom: ''none'', flag: false },
];

// Build a "missing predicate" SQL fragment given the kind and column name.
function missingPredicate(col, kind) {
  const cs = `${col}::STRING`;
  switch (kind) {
    case ''category'':
      // NULL, empty, OR in (NI,UN,OT)
      return `(${col} IS NULL OR TRIM(${cs}) = '''' OR UPPER(TRIM(${cs})) IN (''NI'',''UN'',''OT''))`;
    case ''null_n'':
    case ''null_or_missing'':
    case ''cross_null_or_missing'':
    case ''stat_null_or_missing'':
      // NULL or blank only
      return `(${col} IS NULL OR TRIM(${cs}) = '''')`;
    case ''nmiss'':
      // NULL only (SAS NMISS on numerics/dates ignores blanks)
      return `(${col} IS NULL)`;
    default:
      throw new Error(`Unknown kind: ${kind}`);
  }
}

// Denominator SQL fragment
function denomWhere(denomKind) {
  if (denomKind === ''ip_ei'') {
    return `WHERE UPPER(TRIM(ENC_TYPE)) IN (''IP'',''EI'')`;
  }
  return ``; // no filter -> COUNT(*)
}

// Group catalog by table so we can query each table with one aggregate SELECT
const byTable = {};
for (const r of catalog) {
  if (!byTable[r.tbl]) byTable[r.tbl] = [];
  byTable[r.tbl].push(r);
}

const evaluated = [];
const skipped = [];

for (const tbl of Object.keys(byTable)) {
  if (!isSafeIdentPart(tbl)) {
    skipped.push({ table: tbl, reason: ''unsafe_table_name'' });
    continue;
  }
  if (!(only === ''ALL'' || only === tbl.toUpperCase())) {
    continue; // caller wants a different specific table
  }
  if (!tableExists(DB_PARAM, SCHEMA_NAME, tbl)) {
    skipped.push({ table: tbl, reason: ''missing_table'' });
    continue;
  }
  const fq = `${DB_PARAM}.${SCHEMA_NAME}.${tbl}`;
  const fields = byTable[tbl];

  // Filter out fields whose column doesn''t exist in this table (safe skip)
  const usable = [];
  for (const f of fields) {
    if (!isSafeIdentPart(f.col)) {
      skipped.push({ table: tbl, column: f.col, reason: ''unsafe_column_name'' });
      continue;
    }
    if (!colExists(DB_PARAM, SCHEMA_NAME, tbl, f.col)) {
      skipped.push({ table: tbl, column: f.col, reason: ''missing_column'' });
      continue;
    }
    usable.push(f);
  }
  if (usable.length === 0) continue;

  // We may have multiple denom filters within the same table (e.g. ENCOUNTER has
  // some fields with denom=none and others with denom=ip_ei). Split by denom.
  const byDenom = {};
  for (const f of usable) {
    if (!byDenom[f.denom]) byDenom[f.denom] = [];
    byDenom[f.denom].push(f);
  }

  for (const denomKind of Object.keys(byDenom)) {
    const flist = byDenom[denomKind];
    // Build one SELECT: COUNT(*) OR COUNT_IF(denom_filter), then per-field COUNT_IF(missing)
    // NOTE: for denom=ip_ei we need the ENC_TYPE column to exist on this table.
    if (denomKind === ''ip_ei'' && !colExists(DB_PARAM, SCHEMA_NAME, tbl, ''ENC_TYPE'')) {
      // Should never happen for IVC ENCOUNTER, but guard.
      for (const f of flist) skipped.push({ table: tbl, column: f.col, reason: ''enc_type_missing_for_ip_ei_denom'' });
      continue;
    }
    const denomExpr = (denomKind === ''ip_ei'')
      ? `COUNT_IF(UPPER(TRIM(ENC_TYPE)) IN (''IP'',''EI''))`
      : `COUNT(*)`;
    const numExprs = flist.map((f, i) => {
      const pred = missingPredicate(f.col, f.kind);
      // For ip_ei denom, numerator is also restricted to IP/EI rows.
      const p = (denomKind === ''ip_ei'')
        ? `(UPPER(TRIM(ENC_TYPE)) IN (''IP'',''EI'') AND ${pred})`
        : pred;
      return `COUNT_IF(${p}) AS n${i}`;
    });
    const sql = `SELECT ${denomExpr} AS denom, ${numExprs.join('', '')} FROM ${fq}`;
    const rs = q(sql);
    rs.next();
    const denom = Number(rs.getColumnValue(1));
    for (let i = 0; i < flist.length; i++) {
      const f = flist[i];
      const missingN = Number(rs.getColumnValue(2 + i));
      const pct = (denom > 0) ? (missingN / denom) * 100.0 : null;
      const flag = (f.flag && denom > 0 && pct !== null && pct >= thresholdPct) ? 1 : 0;

      const details = {
        table: tbl,
        column: f.col,
        sas_var: f.sas_var,
        sas_dc_name: f.dc_name,
        missing_kind: f.kind,
        denom_filter: f.denom,
        denom_n: denom,
        missing_n: missingN,
        missing_pct: pct,
        threshold_pct_gte: thresholdPct,
        is_flag_field: f.flag,
        edc_table: edcTable,
        sas_source: ''edc_report.sas newest (SAS PCE v16)''
      };

      // CODE_TYPE column carries the CDM column name; SOURCE_TABLE is the CDM table.
      insertMetric(resultsTbl, bindsBase, edcTable, tbl, f.col, ''DENOM_N'',   denom,    String(denom),    thresholdPct, false, details);
      insertMetric(resultsTbl, bindsBase, edcTable, tbl, f.col, ''MISSING_N'', missingN, String(missingN), thresholdPct, false, details);
      insertMetric(resultsTbl, bindsBase, edcTable, tbl, f.col, ''MISSING_PCT'', pct, (pct === null ? null : String(pct)), thresholdPct, false, details);
      insertMetric(resultsTbl, bindsBase, edcTable, tbl, f.col, ''MISSING_FLAG'', flag, String(flag), thresholdPct, (flag === 1), details);

      evaluated.push({ table: tbl, column: f.col });
    }
  }
}

const status = (evaluated.length === 0) ? ''ERROR'' : ''OK'';
insertMetric(
  resultsTbl,
  bindsBase,
  edcTable,
  (only === ''ALL'' ? ''ALL'' : only),
  ''ALL'',
  ''STATUS'',
  null,
  status,
  thresholdPct,
  (status === ''ERROR''),
  { target_table: only, evaluated_fields_n: evaluated.length, skipped_fields_n: skipped.length, skipped: skipped }
);

return `DC 3.03 IVD finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} evaluated=${evaluated.length} skipped=${skipped.length}`;
';
