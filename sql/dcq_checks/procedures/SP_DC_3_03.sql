CREATE OR REPLACE PROCEDURE "SP_DC_3_03"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR)
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

function colInfo(db, schema, table, col) {
  const rs = q(
    `SELECT DATA_TYPE
     FROM ${db}.INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
     QUALIFY ROW_NUMBER() OVER (ORDER BY ORDINAL_POSITION) = 1`,
    [schema.toUpperCase(), table.toUpperCase(), col.toUpperCase()]
  );
  if (!rs.next()) return null;
  return { dataType: (rs.getColumnValue(1) || '''').toString() };
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

const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const resultsTbl = `${outSchema}.DCQ_RESULTS`;
const rowNum = 3.03;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();

// CDM conformance: for fields marked "required, not null" => missing threshold is 0%.
// For string fields, treat blank as missing.
const thresholdPct = 0;
const cdmDoc = ''2025_01_23_PCORnet_Common_Data_Model_v7dot0_parseable.xlsx'';

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

if (only === ''ALL'') {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
} else {
  if (!isSafeIdentPart(only)) throw new Error(`Invalid TARGET_TABLE: ${TARGET_TABLE}`);
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ? AND UPPER(EDC_TABLE) = ?`, [RUN_ID, rowNum, only]);
}

// Key fields from CDM v7.0 parseable workbook: CONSTRAINTS sheet "required, not null".
const specs = [
  { table: ''CONDITION'', cols: [''CONDITION'', ''CONDITIONID'', ''CONDITION_SOURCE'', ''CONDITION_TYPE'', ''PATID''] },
  { table: ''DEATH'', cols: [''DEATH_SOURCE'', ''PATID''] },
  { table: ''DEATH_CAUSE'', cols: [''DEATH_CAUSE'', ''DEATH_CAUSE_CODE'', ''DEATH_CAUSE_SOURCE'', ''DEATH_CAUSE_TYPE'', ''PATID''] },
  { table: ''DEMOGRAPHIC'', cols: [''PATID''] },
  { table: ''DIAGNOSIS'', cols: [''DIAGNOSISID'', ''DX'', ''DX_SOURCE'', ''DX_TYPE'', ''PATID''] },
  { table: ''DISPENSING'', cols: [''DISPENSE_DATE'', ''DISPENSINGID'', ''NDC'', ''PATID''] },
  { table: ''ENCOUNTER'', cols: [''ADMIT_DATE'', ''ENCOUNTERID'', ''ENC_TYPE'', ''PATID''] },
  { table: ''ENROLLMENT'', cols: [''ENR_BASIS'', ''ENR_START_DATE'', ''PATID''] },
  { table: ''EXTERNAL_MEDS'', cols: [''EXTMEDID'', ''PATID''] },
  { table: ''HARVEST'', cols: [''DATAMARTID'', ''NETWORKID''] },
  { table: ''HASH_TOKEN'', cols: [''PATID'', ''TOKEN_ENCRYPTION_KEY''] },
  { table: ''IMMUNIZATION'', cols: [''IMMUNIZATIONID'', ''PATID'', ''VX_CODE'', ''VX_CODE_TYPE'', ''VX_STATUS''] },
  { table: ''LAB_HISTORY'', cols: [''LABHISTORYID'', ''LAB_LOINC''] },
  { table: ''LAB_RESULT_CM'', cols: [''LAB_RESULT_CM_ID'', ''PATID'', ''RESULT_DATE''] },
  { table: ''LDS_ADDRESS_HISTORY'', cols: [''ADDRESSID'', ''ADDRESS_PREFERRED'', ''ADDRESS_TYPE'', ''ADDRESS_USE'', ''PATID''] },
  { table: ''MED_ADMIN'', cols: [''MEDADMINID'', ''MEDADMIN_START_DATE'', ''PATID''] },
  { table: ''OBS_CLIN'', cols: [''OBSCLINID'', ''OBSCLIN_START_DATE'', ''PATID''] },
  { table: ''OBS_GEN'', cols: [''OBSGENID'', ''OBSGEN_START_DATE'', ''PATID''] },
  { table: ''PAT_RELATIONSHIP'', cols: [''PATID_1'', ''PATID_2'', ''RELATIONSHIP_TYPE''] },
  { table: ''PCORNET_TRIAL'', cols: [''PARTICIPANTID'', ''PATID'', ''TRIALID''] },
  { table: ''PRESCRIBING'', cols: [''PATID'', ''PRESCRIBINGID''] },
  { table: ''PROCEDURES'', cols: [''PATID'', ''PROCEDURESID'', ''PX'', ''PX_TYPE''] },
  { table: ''PROVIDER'', cols: [''PROVIDERID''] },
  { table: ''PRO_CM'', cols: [''PATID'', ''PRO_CM_ID'', ''PRO_DATE''] },
  { table: ''VITAL'', cols: [''MEASURE_DATE'', ''PATID'', ''VITALID'', ''VITAL_SOURCE''] }
];

const evaluated = [];
const skipped = [];

for (const spec of specs) {
  const tbl = spec.table;
  if (!isSafeIdentPart(tbl)) continue;
  if (!(only === ''ALL'' || only === tbl)) continue;
  if (!tableExists(DB_PARAM, SCHEMA_NAME, tbl)) {
    skipped.push({ table: tbl, reason: ''missing_table'' });
    continue;
  }

  const fq = `${DB_PARAM}.${SCHEMA_NAME}.${tbl}`;
  for (const col of spec.cols) {
    if (!isSafeIdentPart(col)) {
      skipped.push({ table: tbl, column: col, reason: ''unsafe_column_name'' });
      continue;
    }
    const info = colInfo(DB_PARAM, SCHEMA_NAME, tbl, col);
    if (!info) {
      skipped.push({ table: tbl, column: col, reason: ''missing_column'' });
      continue;
    }

    const dt = (info.dataType || '''').toUpperCase();
    const isString = (dt.includes(''CHAR'') || dt.includes(''TEXT'') || dt.includes(''STRING''));
    const missingPred = isString
      ? `(${col} IS NULL OR TRIM(${col}::STRING) = '''')`
      : `(${col} IS NULL)`;

    const rs = q(
      `SELECT
         COUNT(*) AS denom_n,
         COUNT_IF(${missingPred}) AS missing_n
       FROM ${fq}`
    );
    rs.next();
    const denom = Number(rs.getColumnValue(1));
    const missingN = Number(rs.getColumnValue(2));
    const pct = (denom > 0) ? (missingN / denom) * 100.0 : null;
    const flag = (denom > 0 && pct !== null && pct > thresholdPct) ? 1 : 0;

    const details = {
      table: tbl,
      column: col,
      data_type: dt,
      denom_n: denom,
      missing_n: missingN,
      missing_pct: pct,
      threshold_pct_gt: thresholdPct,
      key_field_source: ''CDM_CONSTRAINTS_REQUIRED_NOT_NULL'',
      cdm_doc: cdmDoc,
      definition: ''Percent of rows where a CDM required (not null) field is NULL (and for string fields: also blank).''
    };

    insertMetric(resultsTbl, bindsBase, tbl, tbl, col, ''DENOM_N'', denom, String(denom), thresholdPct, false, details);
    insertMetric(resultsTbl, bindsBase, tbl, tbl, col, ''MISSING_N'', missingN, String(missingN), thresholdPct, false, details);
    insertMetric(resultsTbl, bindsBase, tbl, tbl, col, ''MISSING_PCT'', pct, (pct === null ? null : String(pct)), thresholdPct, false, details);
    insertMetric(resultsTbl, bindsBase, tbl, tbl, col, ''MISSING_FLAG'', flag, String(flag), thresholdPct, (flag === 1), details);

    evaluated.push({ table: tbl, column: col });
  }
}

const status = (evaluated.length === 0) ? ''ERROR'' : ''OK'';
insertMetric(
  resultsTbl,
  bindsBase,
  (only === ''ALL'' ? ''ALL'' : only),
  ''ALL'',
  ''ALL'',
  ''STATUS'',
  null,
  status,
  thresholdPct,
  (status === ''ERROR''),
  {
    target_table: only,
    evaluated_fields_n: evaluated.length,
    skipped_fields_n: skipped.length,
    skipped: skipped
  }
);

return `DC 3.03 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} evaluated_fields=${evaluated.length}`;
';