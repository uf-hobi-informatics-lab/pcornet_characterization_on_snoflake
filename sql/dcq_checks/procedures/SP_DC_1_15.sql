CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ_CHECKS.SP_DC_1_15"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "RUN_ID" VARCHAR, "TARGET_TABLE" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
function q(sqlText, binds) { return snowflake.execute({ sqlText, binds: binds || [] }); }
function isSafeIdentPart(s) { return /^[A-Za-z0-9_$]+$/.test((s || '''').toString()); }

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
const rowNum = 1.15;
const only = (TARGET_TABLE || ''ALL'').toString().trim().toUpperCase();
if (only !== ''ALL'' && !isSafeIdentPart(only)) throw new Error(`Invalid TARGET_TABLE: ${TARGET_TABLE}`);

// CDM diagnostic conformance: undefined-length ID fields should be harmonized.
// Flag when a group has more than 1 distinct character_maximum_length.
const thresholdDistinctLenGt = 1;

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

if (only === ''ALL'') {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ?`, [RUN_ID, rowNum]);
} else {
  q(`DELETE FROM ${resultsTbl} WHERE RUN_ID = ? AND ROW_NUM = ? AND UPPER(EDC_TABLE) = ?`, [RUN_ID, rowNum, only]);
}

// Columns to evaluate (from the SAS Table IID / DC 1.15 message).
const idCols = [
  ''PATID'',
  ''PATID_1'',
  ''PATID_2'',
  ''ENCOUNTERID'',
  ''PRESCRIBINGID'',
  ''PROCEDURESID'',
  ''PROVIDERID'',
  ''MEDADMIN_PROVIDERID'',
  ''OBSGEN_PROVIDERID'',
  ''OBSCLIN_PROVIDERID'',
  ''RX_PROVIDERID'',
  ''VX_PROVIDERID''
];

// Grouping rules: harmonize related identifiers even when column names differ.
function groupKey(col) {
  const c = (col || '''').toUpperCase();
  if (c === ''PATID'' || c === ''PATID_1'' || c === ''PATID_2'') return ''PATID_FAMILY'';
  if (c === ''PROVIDERID'' || c.endsWith(''_PROVIDERID'')) return ''PROVIDERID_FAMILY'';
  return c;
}

const inList = idCols.map(() => ''?'').join('','');
const colsRs = q(
  `SELECT
     UPPER(TABLE_NAME) AS TABLE_NAME,
     UPPER(COLUMN_NAME) AS COLUMN_NAME,
     UPPER(DATA_TYPE) AS DATA_TYPE,
     CHARACTER_MAXIMUM_LENGTH AS CHAR_MAX_LEN
   FROM ${DB_PARAM}.INFORMATION_SCHEMA.COLUMNS
   WHERE UPPER(TABLE_SCHEMA) = ?
     AND UPPER(COLUMN_NAME) IN (${inList})
   ORDER BY 1,2`,
  [SCHEMA_NAME.toUpperCase()].concat(idCols)
);

const occurrences = [];
while (colsRs.next()) {
  const t = colsRs.getColumnValue(1);
  const c = colsRs.getColumnValue(2);
  const dt = colsRs.getColumnValue(3);
  const len = colsRs.getColumnValue(4);
  // Only evaluate character columns.
  if (!dt || !dt.toString().includes(''CHAR'') && !dt.toString().includes(''TEXT'') && !dt.toString().includes(''STRING'') && !dt.toString().includes(''VARCHAR'')) {
    continue;
  }
  occurrences.push({ table: t, column: c, data_type: dt, char_max_len: (len === null ? null : Number(len)), group: groupKey(c) });
}

const groups = {};
for (const o of occurrences) {
  if (!groups[o.group]) groups[o.group] = [];
  groups[o.group].push(o);
}

const skipped = [];
const evaluated = [];

for (const g of Object.keys(groups).sort()) {
  const list = groups[g];
  if (only !== ''ALL'') {
    const touches = list.some(x => x.table === only);
    if (!touches) continue;
  }

  const lens = list
    .map(x => x.char_max_len)
    .filter(x => x !== null && x !== undefined && !Number.isNaN(x));

  if (lens.length === 0) {
    skipped.push({ group: g, reason: ''no_character_max_length_values'' });
    continue;
  }

  const distinctLens = Array.from(new Set(lens)).sort((a, b) => a - b);
  const minLen = distinctLens[0];
  const maxLen = distinctLens[distinctLens.length - 1];
  const flag = (distinctLens.length > thresholdDistinctLenGt) ? 1 : 0;

  // Summarize per length for quick diagnosis.
  const byLen = {};
  for (const o of list) {
    const k = (o.char_max_len === null || o.char_max_len === undefined) ? ''NULL'' : String(o.char_max_len);
    if (!byLen[k]) byLen[k] = [];
    byLen[k].push({ table: o.table, column: o.column });
  }

  const details = {
    group: g,
    distinct_lengths: distinctLens,
    occurrences_n: list.length,
    by_length: byLen,
    threshold_distinct_lengths_gt: thresholdDistinctLenGt,
    definition: ''For undefined-length identifier fields present in multiple tables/columns, check that declared max length is harmonized.''
  };

  insertMetric(resultsTbl, bindsBase, edcTable, ''INFORMATION_SCHEMA.COLUMNS'', g, ''DISTINCT_LENGTH_N'', distinctLens.length, String(distinctLens.length), thresholdDistinctLenGt, false, details);
  insertMetric(resultsTbl, bindsBase, edcTable, ''INFORMATION_SCHEMA.COLUMNS'', g, ''MIN_LENGTH'', minLen, String(minLen), thresholdDistinctLenGt, false, details);
  insertMetric(resultsTbl, bindsBase, edcTable, ''INFORMATION_SCHEMA.COLUMNS'', g, ''MAX_LENGTH'', maxLen, String(maxLen), thresholdDistinctLenGt, false, details);
  insertMetric(resultsTbl, bindsBase, edcTable, ''INFORMATION_SCHEMA.COLUMNS'', g, ''HARMONIZED_LENGTH_FLAG'', flag, String(flag), thresholdDistinctLenGt, (flag === 1), details);

  evaluated.push({ group: g, distinct_length_n: distinctLens.length });
}

const status = (evaluated.length === 0) ? ''ERROR'' : ''OK'';
insertMetric(
  resultsTbl,
  bindsBase,
  (only === ''ALL'' ? ''ALL'' : only),
  ''INFORMATION_SCHEMA.COLUMNS'',
  ''ALL'',
  ''STATUS'',
  null,
  status,
  thresholdDistinctLenGt,
  (status === ''ERROR''),
  {
    target_table: only,
    evaluated_groups_n: evaluated.length,
    skipped_groups_n: skipped.length,
    skipped: skipped,
    columns_evaluated: idCols,
    note: ''This check inspects column metadata, not row-level data.''
  }
);

return `DC 1.15 finished RUN_ID=${RUN_ID} TARGET_TABLE=${only} evaluated_groups=${evaluated.length}`;
';