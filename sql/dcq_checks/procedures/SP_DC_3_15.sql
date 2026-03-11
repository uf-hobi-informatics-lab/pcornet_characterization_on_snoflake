CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_15"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR)
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
  MED_ADMIN: ''MEDADMIN_START_DATE''
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
const rowNum = 3.15;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();

if (!(only === ''ALL'' || only === ''MED_ADMIN'')) {
  throw new Error(`Invalid TARGET_TABLE=''${TARGET_TABLE}''. Use ALL or MED_ADMIN.`);
}

// SAS Table IVH (MED_ADMIN portion): flag if Tier 1 percent < 80 or numerator 0.
const thresholdPctLt = 80.0;

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

// Reference table
const refDb = ''CHARACTERIZATION'';
const refSchema = ''EDC_REF'';
const refTable = ''RXNORM_CUI_REF_RAW'';
if (!tableExists(refDb, refSchema, refTable) ||
    !colExists(refDb, refSchema, refTable, ''RXNORM_CUI'') ||
    !colExists(refDb, refSchema, refTable, ''RXNORM_CUI_TTY'')) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), `${refDb}.${refSchema}.${refTable}`, ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    { message: `Missing ${refDb}.${refSchema}.${refTable} with RXNORM_CUI and RXNORM_CUI_TTY.` });
  return ''DC 3.15 ERROR: missing rxnorm reference'';
}

// MED_ADMIN input
if (!tableExists(DB_PARAM, SCHEMA_NAME, ''MED_ADMIN'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''MED_ADMIN'', ''MEDADMIN_TYPE'') ||
    !colExists(DB_PARAM, SCHEMA_NAME, ''MED_ADMIN'', ''MEDADMIN_CODE'')) {
  insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''MED_ADMIN'', ''ALL'', ''STATUS'', null, ''ERROR'', thresholdPctLt, true,
    { message: ''Missing MED_ADMIN or required columns MEDADMIN_TYPE, MEDADMIN_CODE'' });
  return ''DC 3.15 ERROR: missing MED_ADMIN required columns'';
}

const med = `${DB_PARAM}.${SCHEMA_NAME}.MED_ADMIN`;
const ref = `${refDb}.${refSchema}.${refTable}`;

// Tier definitions per SAS formats (Table IVH)
const tier1Ttys = "(''SCD'',''SBD'',''BPCK'',''GPCK'')";
const tier2Ttys = "(''SBDF'',''SBDFP'',''SCDF'',''SCDFP'',''SBDG'',''SCDG'',''SCDGP'',''SBDC'',''BN'',''MIN'')";
const tier3Ttys = "(''SCDC'',''PIN'',''IN'')";
const tier4Ttys = "(''DF'',''DFG'')";

// Compute tier distribution for MED_ADMIN.
// Only rows with MEDADMIN_TYPE=''RX'' contribute a RXCUI (from MEDADMIN_CODE); all other rows are treated as unmapped (Tier 5).
const rs = q(
   `WITH base AS (
     SELECT
       UPPER(TRIM(MEDADMIN_TYPE::STRING)) AS medadmin_type,
       CASE
         WHEN UPPER(TRIM(MEDADMIN_TYPE::STRING)) = ''RX'' THEN UPPER(TRIM(MEDADMIN_CODE::STRING))
         ELSE NULL
       END AS rxnorm_cui
     FROM ${med}
     WHERE 1=1 ${dateFilterWhere(''MED_ADMIN'')}
   ),
   mapped AS (
     SELECT
       b.medadmin_type,
       b.rxnorm_cui,
       UPPER(TRIM(r.RXNORM_CUI_TTY::STRING)) AS tty
     FROM base b
     LEFT JOIN ${ref} r
       ON UPPER(TRIM(r.RXNORM_CUI::STRING)) = b.rxnorm_cui
   ),
   tiered AS (
     SELECT
       medadmin_type,
       rxnorm_cui,
       tty,
       CASE
         WHEN rxnorm_cui IS NULL OR rxnorm_cui = '''' THEN ''TIER_5''
         WHEN tty IN ${tier1Ttys} THEN ''TIER_1''
         WHEN tty IN ${tier2Ttys} THEN ''TIER_2''
         WHEN tty IN ${tier3Ttys} THEN ''TIER_3''
         WHEN tty IN ${tier4Ttys} THEN ''TIER_4''
         ELSE ''TIER_5''
       END AS tier
     FROM mapped
   )
   SELECT
     COUNT(*) AS total_records,
     COUNT_IF(medadmin_type = ''RX'') AS rx_type_records,
     COUNT_IF(rxnorm_cui IS NOT NULL AND rxnorm_cui <> '''') AS nonmissing_cui_records,
     COUNT_IF(tier = ''TIER_1'') AS tier1_records,
     COUNT_IF(tier = ''TIER_2'') AS tier2_records,
     COUNT_IF(tier = ''TIER_3'') AS tier3_records,
     COUNT_IF(tier = ''TIER_4'') AS tier4_records,
     COUNT_IF(tier = ''TIER_5'') AS tier5_records
   FROM tiered`
);
rs.next();

const denomAll = Number(rs.getColumnValue(1));
const rxTypeN = Number(rs.getColumnValue(2));
const denomNonMissing = Number(rs.getColumnValue(3));
const tier1 = Number(rs.getColumnValue(4));
const tier2 = Number(rs.getColumnValue(5));
const tier3 = Number(rs.getColumnValue(6));
const tier4 = Number(rs.getColumnValue(7));
const tier5 = Number(rs.getColumnValue(8));

const pctTier1 = (denomAll > 0) ? (tier1 / denomAll) * 100.0 : null;
const pctTier2 = (denomAll > 0) ? (tier2 / denomAll) * 100.0 : null;
const pctTier3 = (denomAll > 0) ? (tier3 / denomAll) * 100.0 : null;
const pctTier4 = (denomAll > 0) ? (tier4 / denomAll) * 100.0 : null;
const pctTier5 = (denomAll > 0) ? (tier5 / denomAll) * 100.0 : null;

const notApplicable = (denomAll === 0);
const flag = (!notApplicable) && ((pctTier1 !== null && pctTier1 < thresholdPctLt) || tier1 === 0);

const details = {
  table: ''MED_ADMIN'',
  filter: "MEDADMIN_TYPE = ''RX'' contributes RXCUI; other types treated as unmapped",
  denom_records_n: denomAll,
  denom_records_with_medadmin_type_rx_n: rxTypeN,
  denom_records_with_nonmissing_rxcui_n: denomNonMissing,
  tier1_records_n: tier1,
  tier2_records_n: tier2,
  tier3_records_n: tier3,
  tier4_records_n: tier4,
  tier5_records_n: tier5,
  tier1_pct: pctTier1,
  tier2_pct: pctTier2,
  tier3_pct: pctTier3,
  tier4_pct: pctTier4,
  tier5_pct: pctTier5,
  threshold_pct_lt: thresholdPctLt,
  tier1_ttys: [''SCD'',''SBD'',''BPCK'',''GPCK''],
  tier2_ttys: [''SBDF'',''SBDFP'',''SCDF'',''SCDFP'',''SBDG'',''SCDG'',''SCDGP'',''SBDC'',''BN'',''MIN''],
  tier3_ttys: [''SCDC'',''PIN'',''IN''],
  tier4_ttys: [''DF'',''DFG''],
  reference: `${refDb}.${refSchema}.${refTable}`,
  not_applicable: notApplicable,
  definition: "Tier percent = % of all MED_ADMIN records, where MEDADMIN_TYPE=''RX'' rows use MEDADMIN_CODE as RxCUI and other types are treated as unmapped (Tier 5)."
};

insertMetric(resultsTbl, bindsBase, ''MED_ADMIN'', ''MED_ADMIN'', ''ALL'', ''DENOM_RECORDS_N'', denomAll, String(denomAll), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''MED_ADMIN'', ''MED_ADMIN'', ''ALL'', ''DENOM_RECORDS_WITH_MEDADMIN_TYPE_RX_N'', rxTypeN, String(rxTypeN), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''MED_ADMIN'', ''MED_ADMIN'', ''ALL'', ''DENOM_RECORDS_WITH_NONMISSING_RXCUI_N'', denomNonMissing, String(denomNonMissing), thresholdPctLt, false, details);

insertMetric(resultsTbl, bindsBase, ''MED_ADMIN'', ''MED_ADMIN'', ''TIER_1'', ''RECORDS_N'', tier1, String(tier1), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''MED_ADMIN'', ''MED_ADMIN'', ''TIER_1'', ''RECORD_PCT'', pctTier1, (pctTier1 === null ? null : String(pctTier1)), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''MED_ADMIN'', ''MED_ADMIN'', ''TIER_2'', ''RECORDS_N'', tier2, String(tier2), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''MED_ADMIN'', ''MED_ADMIN'', ''TIER_2'', ''RECORD_PCT'', pctTier2, (pctTier2 === null ? null : String(pctTier2)), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''MED_ADMIN'', ''MED_ADMIN'', ''TIER_3'', ''RECORDS_N'', tier3, String(tier3), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''MED_ADMIN'', ''MED_ADMIN'', ''TIER_3'', ''RECORD_PCT'', pctTier3, (pctTier3 === null ? null : String(pctTier3)), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''MED_ADMIN'', ''MED_ADMIN'', ''TIER_4'', ''RECORDS_N'', tier4, String(tier4), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''MED_ADMIN'', ''MED_ADMIN'', ''TIER_4'', ''RECORD_PCT'', pctTier4, (pctTier4 === null ? null : String(pctTier4)), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''MED_ADMIN'', ''MED_ADMIN'', ''TIER_5'', ''RECORDS_N'', tier5, String(tier5), thresholdPctLt, false, details);
insertMetric(resultsTbl, bindsBase, ''MED_ADMIN'', ''MED_ADMIN'', ''TIER_5'', ''RECORD_PCT'', pctTier5, (pctTier5 === null ? null : String(pctTier5)), thresholdPctLt, false, details);

insertMetric(resultsTbl, bindsBase, ''MED_ADMIN'', ''MED_ADMIN'', ''TIER_1'', ''TIER1_FLAG'', (flag ? 1 : 0), String(flag ? 1 : 0), thresholdPctLt, flag, details);

insertMetric(resultsTbl, bindsBase, (only === ''ALL'' ? ''ALL'' : only), ''ALL'', ''ALL'', ''STATUS'', null, ''OK'', thresholdPctLt, false,
  { target_table: only, not_applicable: notApplicable, denom_records_n: denomAll, denom_records_with_medadmin_type_rx_n: rxTypeN }
);

return `DC 3.15 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only}`;
';