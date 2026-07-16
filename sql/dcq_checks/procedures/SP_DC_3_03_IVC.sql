CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_03_IVC"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
// -----------------------------------------------------------------------
// DC 3.03 Table IVC: Missing or Unknown Values
// -----------------------------------------------------------------------
// Ports SAS edc_report.sas Table IVC block. For each field:
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
// SAS-specified flag list for IVC (see catalog "flag" property below).
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
const edcTable   = ''Table IVC'';
const variant    = ''IVC''; // for logging
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
const checkNameFilter = ''DC_3_03_IVC'';
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
  { tbl: ''DEMOGRAPHIC'', col: ''BIRTH_DATE'', sas_var: ''BIRTH_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: true },
  { tbl: ''DEMOGRAPHIC'', col: ''BIRTH_TIME'', sas_var: ''BIRTH_TIME'', dc_name: ''XTBL_L3_TIMES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''DEMOGRAPHIC'', col: ''SEX'', sas_var: ''SEX'', dc_name: ''DEM_L3_SEXDIST'', kind: ''category'', denom: ''none'', flag: true },
  { tbl: ''DEMOGRAPHIC'', col: ''HISPANIC'', sas_var: ''HISPANIC'', dc_name: ''DEM_L3_HISPDIST'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEMOGRAPHIC'', col: ''RACE'', sas_var: ''RACE'', dc_name: ''DEM_L3_RACE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEMOGRAPHIC'', col: ''GENDER_IDENTITY'', sas_var: ''GENDER_IDENTITY'', dc_name: ''DEM_L3_GENDERDIST'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEMOGRAPHIC'', col: ''SEXUAL_ORIENTATION'', sas_var: ''SEXUAL_ORIENTATION'', dc_name: ''DEM_L3_ORIENTDIST'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEMOGRAPHIC'', col: ''PAT_PREF_LANGUAGE_SPOKEN'', sas_var: ''PAT_PREF_LANGUAGE_SPOKEN'', dc_name: ''DEM_L3_PATPREFLANG'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEMOGRAPHIC'', col: ''BIOBANK_FLAG'', sas_var: ''BIOBANK_FLAG'', dc_name: ''DEM_L3_BIOBANK'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEMOGRAPHIC'', col: ''RACE_ETH_MISSING'', sas_var: ''RACE_ETH_MISSING'', dc_name: ''DEM_L3_RACEETHMISS'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEMOGRAPHIC'', col: ''RACE_ETH_AI_AN'', sas_var: ''RACE_ETH_AI_AN'', dc_name: ''DEM_L3_RACEETHAIAN'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEMOGRAPHIC'', col: ''RACE_ETH_ASIAN'', sas_var: ''RACE_ETH_ASIAN'', dc_name: ''DEM_L3_RACEETHASIAN'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEMOGRAPHIC'', col: ''RACE_ETH_BLACK'', sas_var: ''RACE_ETH_BLACK'', dc_name: ''DEM_L3_RACEETHBLACK'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEMOGRAPHIC'', col: ''RACE_ETH_HISPANIC'', sas_var: ''RACE_ETH_HISPANIC'', dc_name: ''DEM_L3_RACEETHHISPANIC'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEMOGRAPHIC'', col: ''RACE_ETH_ME_NA'', sas_var: ''RACE_ETH_ME_NA'', dc_name: ''DEM_L3_RACEETHMENA'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEMOGRAPHIC'', col: ''RACE_ETH_NH_PI'', sas_var: ''RACE_ETH_NH_PI'', dc_name: ''DEM_L3_RACEETHNHPI'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DEMOGRAPHIC'', col: ''RACE_ETH_WHITE'', sas_var: ''RACE_ETH_WHITE'', dc_name: ''DEM_L3_RACEETHWHITE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''ENROLLMENT'', col: ''ENR_END_DATE'', sas_var: ''ENR_END_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''ENROLLMENT'', col: ''CHART'', sas_var: ''CHART'', dc_name: ''ENR_L3_CHART'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''ENCOUNTER'', col: ''ADMIT_TIME'', sas_var: ''ADMIT_TIME'', dc_name: ''XTBL_L3_TIMES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''ENCOUNTER'', col: ''DISCHARGE_DATE'', sas_var: ''DISCHARGE_DATE'', dc_name: ''ENC_L3_ENCTYPE_DDATE_YM'', kind: ''category'', denom: ''ip_ei'', flag: true },
  { tbl: ''ENCOUNTER'', col: ''DISCHARGE_TIME'', sas_var: ''DISCHARGE_TIME'', dc_name: ''XTBL_L3_TIMES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''ENCOUNTER'', col: ''ENC_TYPE'', sas_var: ''ENC_TYPE'', dc_name: ''ENC_L3_ENCTYPE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''ENCOUNTER'', col: ''PROVIDERID'', sas_var: ''PROVIDERID'', dc_name: ''ENC_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''ENCOUNTER'', col: ''FACILITYID'', sas_var: ''FACILITYID'', dc_name: ''ENC_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''ENCOUNTER'', col: ''DISCHARGE_DISPOSITION'', sas_var: ''DISCHARGE_DISPOSITION'', dc_name: ''ENC_L3_ENCTYPE_DISDISP'', kind: ''category'', denom: ''ip_ei'', flag: true },
  { tbl: ''ENCOUNTER'', col: ''DISCHARGE_STATUS'', sas_var: ''DISCHARGE_STATUS'', dc_name: ''ENC_L3_ENCTYPE_DISSTAT'', kind: ''category'', denom: ''ip_ei'', flag: false },
  { tbl: ''ENCOUNTER'', col: ''DRG'', sas_var: ''DRG'', dc_name: ''ENC_L3_ENCTYPE_DRG'', kind: ''category'', denom: ''ip_ei'', flag: false },
  { tbl: ''ENCOUNTER'', col: ''DRG_TYPE'', sas_var: ''DRG_TYPE'', dc_name: ''ENC_L3_DRG_TYPE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''ENCOUNTER'', col: ''ADMITTING_SOURCE'', sas_var: ''ADMITTING_SOURCE'', dc_name: ''ENC_L3_ENCTYPE_ADMSRC'', kind: ''category'', denom: ''ip_ei'', flag: false },
  { tbl: ''ENCOUNTER'', col: ''PAYER_TYPE_PRIMARY'', sas_var: ''PAYER_TYPE_PRIMARY'', dc_name: ''ENC_L3_PAYERTYPE1'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''ENCOUNTER'', col: ''PAYER_TYPE_SECONDARY'', sas_var: ''PAYER_TYPE_SECONDARY'', dc_name: ''ENC_L3_PAYERTYPE2'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''ENCOUNTER'', col: ''FACILITY_TYPE'', sas_var: ''FACILITY_TYPE'', dc_name: ''ENC_L3_FACILITYTYPE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''ENCOUNTER'', col: ''FACILITY_LOCATION'', sas_var: ''FACILITY_LOCATION'', dc_name: ''ENC_L3_FACILITYLOC'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DIAGNOSIS'', col: ''DX_DATE'', sas_var: ''DX_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''DIAGNOSIS'', col: ''DX_TYPE'', sas_var: ''DX_TYPE'', dc_name: ''DIA_L3_DXTYPE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DIAGNOSIS'', col: ''DX_SOURCE'', sas_var: ''DX_SOURCE'', dc_name: ''DIA_L3_DXSOURCE'', kind: ''category'', denom: ''none'', flag: true },
  { tbl: ''DIAGNOSIS'', col: ''DX_ORIGIN'', sas_var: ''DX_ORIGIN'', dc_name: ''DIA_L3_ORIGIN'', kind: ''category'', denom: ''none'', flag: true },
  { tbl: ''DIAGNOSIS'', col: ''PDX'', sas_var: ''PDX'', dc_name: ''DIA_L3_PDX_ENCTYPE'', kind: ''category'', denom: ''ip_ei'', flag: false },
  { tbl: ''DIAGNOSIS'', col: ''ENCOUNTERID'', sas_var: ''DIA_ENCOUNTERID'', dc_name: ''DIA_L3_N'', kind: ''null_n'', denom: ''none'', flag: true },
  { tbl: ''DIAGNOSIS'', col: ''ADMIT_DATE'', sas_var: ''DIA_ADMIT_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''DIAGNOSIS'', col: ''ENC_TYPE'', sas_var: ''DIA_ENC_TYPE'', dc_name: ''DIA_L3_ENCTYPE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DIAGNOSIS'', col: ''DX_POA'', sas_var: ''DX_POA'', dc_name: ''DIA_L3_DXPOA'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''DIAGNOSIS'', col: ''PROVIDERID'', sas_var: ''DIA_PROVIDERID'', dc_name: ''DIA_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''PROCEDURES'', col: ''PX_DATE'', sas_var: ''PX_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: true },
  { tbl: ''PROCEDURES'', col: ''PX_TYPE'', sas_var: ''PX_TYPE'', dc_name: ''PRO_L3_PXTYPE'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''PROCEDURES'', col: ''PX_SOURCE'', sas_var: ''PX_SOURCE'', dc_name: ''PRO_L3_PXSOURCE'', kind: ''category'', denom: ''none'', flag: true },
  { tbl: ''PROCEDURES'', col: ''ENCOUNTERID'', sas_var: ''PRO_ENCOUNTERID'', dc_name: ''PRO_L3_N'', kind: ''null_n'', denom: ''none'', flag: true },
  { tbl: ''PROCEDURES'', col: ''PPX'', sas_var: ''PPX'', dc_name: ''PRO_L3_PPX'', kind: ''category'', denom: ''none'', flag: false },
  { tbl: ''PROCEDURES'', col: ''PROVIDERID'', sas_var: ''PRO_PROVIDERID'', dc_name: ''PRO_L3_N'', kind: ''null_n'', denom: ''none'', flag: false },
  { tbl: ''PROCEDURES'', col: ''ADMIT_DATE'', sas_var: ''PRO_ADMIT_DATE'', dc_name: ''XTBL_L3_DATES'', kind: ''nmiss'', denom: ''none'', flag: false },
  { tbl: ''PROCEDURES'', col: ''ENC_TYPE'', sas_var: ''PRO_ENC_TYPE'', dc_name: ''PRO_L3_ENCTYPE'', kind: ''category'', denom: ''none'', flag: false },
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

return `DC 3.03 IVC finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} evaluated=${evaluated.length} skipped=${skipped.length}`;
';
