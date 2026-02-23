CREATE OR REPLACE PROCEDURE "SP_RUN_DCQ"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "MODE" VARCHAR, "SELECTOR" VARCHAR, "PART" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
function q(sqlText, binds) {
  return snowflake.execute({ sqlText: sqlText, binds: binds || [] });
}
function scalar(sqlText, binds) {
  const rs = q(sqlText, binds);
  rs.next();
  return rs.getColumnValue(1);
}
function isSafeIdentPart(s) {
  return /^[A-Za-z0-9_$]+$/.test((s || '''').toString());
}
function isSafeProcName(name) {
  return /^[A-Za-z0-9_$]+(\\.[A-Za-z0-9_$]+){0,2}$/.test((name || '''').toString());
}
function normMode(x) { return (x || ''ALL'').toString().trim().toUpperCase(); }
function normPart(x) { return (x || ''all'').toString().trim().toLowerCase(); }
function normTargetTable(x) {
  const v = (x || "").toString().trim();
  if (!v.length) return null;
  const u = v.toUpperCase();
  if (u === "NONE" || u === "NULL" || u === "(NONE)") return null;
  return u;
}
function normOptIdent(x) {
  if (x === null || x === undefined) return null;
  const v = x.toString().trim();
  return v.length ? v.toUpperCase() : null;
}
function parseSelector(sel) {
  if (!sel) return [];
  return sel.toString().split('','').map(s => s.trim()).filter(s => s.length > 0);
}
function parseProc(procName) {
  const parts = procName.split(''.'');
  if (parts.length === 3) return { db: parts[0], schema: parts[1], name: parts[2] };
  if (parts.length === 2) return { db: scalar(''SELECT CURRENT_DATABASE()''), schema: parts[0], name: parts[1] };
  return { db: scalar(''SELECT CURRENT_DATABASE()''), schema: scalar(''SELECT CURRENT_SCHEMA()''), name: parts[0] };
}

const procArgCache = {};
function getProcMaxArgs(procName) {
  if (procArgCache[procName] !== undefined) return procArgCache[procName];
  const p = parseProc(procName);
  if (!isSafeIdentPart(p.db) || !isSafeIdentPart(p.schema) || !isSafeIdentPart(p.name)) {
    procArgCache[procName] = 0;
    return 0;
  }
  const rs = q(`SHOW PROCEDURES LIKE ''${p.name}'' IN SCHEMA ${p.db}.${p.schema}`);
  let maxArgs = 0;
  while (rs.next()) {
    const name = rs.getColumnValue(''name'');
    if ((name || '''').toString().toUpperCase() === p.name.toUpperCase()) {
      const thisMax = rs.getColumnValue(''max_num_arguments'');
      if (thisMax !== null && thisMax > maxArgs) maxArgs = thisMax;
    }
  }
  procArgCache[procName] = maxArgs;
  return maxArgs;
}

if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);

const vPrevDb = normOptIdent(PREV_DB_PARAM);
const vPrevSchema = normOptIdent(PREV_SCHEMA_NAME);
if (vPrevDb !== null && !isSafeIdentPart(vPrevDb)) throw new Error(`Unsafe PREV_DB_PARAM: ${PREV_DB_PARAM}`);
if (vPrevSchema !== null && !isSafeIdentPart(vPrevSchema)) throw new Error(`Unsafe PREV_SCHEMA_NAME: ${PREV_SCHEMA_NAME}`);

const runId = scalar(''SELECT UUID_STRING()'');
const vMode = normMode(MODE);
const vPart = normPart(PART);
const vTargetTable = normTargetTable(TARGET_TABLE);
const sel = parseSelector(SELECTOR);

const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const runsTbl = `${outSchema}.DCQ_RUNS`;
const logTbl = `${outSchema}.DCQ_CHECK_LOG`;

q(`CREATE SCHEMA IF NOT EXISTS ${outSchema}`);



q(`CREATE TABLE IF NOT EXISTS ${runsTbl} (
  RUN_ID STRING, DB_PARAM STRING, SCHEMA_NAME STRING, MODE STRING, SELECTOR STRING, PART STRING, TARGET_TABLE STRING,
  PREV_DB_PARAM STRING, PREV_SCHEMA_NAME STRING,
  STARTED_AT TIMESTAMP_NTZ, ENDED_AT TIMESTAMP_NTZ, STATUS STRING, ERROR_MESSAGE STRING
)`);
q(`ALTER TABLE ${runsTbl} ADD COLUMN IF NOT EXISTS TARGET_TABLE STRING`);
q(`ALTER TABLE ${runsTbl} ADD COLUMN IF NOT EXISTS PREV_DB_PARAM STRING`);
q(`ALTER TABLE ${runsTbl} ADD COLUMN IF NOT EXISTS PREV_SCHEMA_NAME STRING`);
q(`CREATE TABLE IF NOT EXISTS ${logTbl} (
  RUN_ID STRING, CHECK_ID STRING, CHECK_NAME STRING, ROW_NUM NUMBER(10,2), PROC_NAME STRING,
  STATUS STRING, STARTED_AT TIMESTAMP_NTZ, ENDED_AT TIMESTAMP_NTZ, ERROR_MESSAGE STRING
)`);
q(`CREATE TABLE IF NOT EXISTS ${outSchema}.DCQ_RESULTS (
  RUN_ID STRING, CHECK_ID STRING, CHECK_NAME STRING, ROW_NUM NUMBER(10,2), EDC_TABLE STRING,
  SOURCE_TABLE STRING, CODE_TYPE STRING, METRIC STRING, VALUE_NUM NUMBER(38,10), VALUE_STR STRING,
  THRESHOLD_NUM NUMBER(38,10), EXCEPTION_FLAG BOOLEAN, DETAILS VARIANT,
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)`);

// In case DCQ_RESULTS already exists with an older schema, add expected columns.
q(`ALTER TABLE ${outSchema}.DCQ_RESULTS ADD COLUMN IF NOT EXISTS RUN_ID STRING`);
q(`ALTER TABLE ${outSchema}.DCQ_RESULTS ADD COLUMN IF NOT EXISTS CHECK_ID STRING`);
q(`ALTER TABLE ${outSchema}.DCQ_RESULTS ADD COLUMN IF NOT EXISTS CHECK_NAME STRING`);
q(`ALTER TABLE ${outSchema}.DCQ_RESULTS ADD COLUMN IF NOT EXISTS ROW_NUM NUMBER(10,2)`);
q(`ALTER TABLE ${outSchema}.DCQ_RESULTS ADD COLUMN IF NOT EXISTS EDC_TABLE STRING`);
q(`ALTER TABLE ${outSchema}.DCQ_RESULTS ADD COLUMN IF NOT EXISTS SOURCE_TABLE STRING`);
q(`ALTER TABLE ${outSchema}.DCQ_RESULTS ADD COLUMN IF NOT EXISTS CODE_TYPE STRING`);
q(`ALTER TABLE ${outSchema}.DCQ_RESULTS ADD COLUMN IF NOT EXISTS METRIC STRING`);
q(`ALTER TABLE ${outSchema}.DCQ_RESULTS ADD COLUMN IF NOT EXISTS VALUE_NUM NUMBER(38,10)`);
q(`ALTER TABLE ${outSchema}.DCQ_RESULTS ADD COLUMN IF NOT EXISTS VALUE_STR STRING`);
q(`ALTER TABLE ${outSchema}.DCQ_RESULTS ADD COLUMN IF NOT EXISTS THRESHOLD_NUM NUMBER(38,10)`);
q(`ALTER TABLE ${outSchema}.DCQ_RESULTS ADD COLUMN IF NOT EXISTS EXCEPTION_FLAG BOOLEAN`);
q(`ALTER TABLE ${outSchema}.DCQ_RESULTS ADD COLUMN IF NOT EXISTS DETAILS VARIANT`);
q(`ALTER TABLE ${outSchema}.DCQ_RESULTS ADD COLUMN IF NOT EXISTS CREATED_AT TIMESTAMP_NTZ`);

q(
  `INSERT INTO ${runsTbl}
   (RUN_ID, DB_PARAM, SCHEMA_NAME, MODE, SELECTOR, PART, TARGET_TABLE, PREV_DB_PARAM, PREV_SCHEMA_NAME, STARTED_AT, STATUS)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(), ''RUNNING'')`,
  [runId, DB_PARAM, SCHEMA_NAME, vMode, SELECTOR, vPart, vTargetTable, vPrevDb, vPrevSchema]
);

// Selector temp table (drop to avoid "already exists" in same session)


let numCol = "ROW_NUM";
try {
  q("SELECT CHECK_NUM FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY LIMIT 1");
  numCol = "CHECK_NUM";
} catch (e) {
  numCol = "ROW_NUM";
}
const selectorStr = (SELECTOR || "").toString();
let checksSql = `
  WITH SEL AS (
    SELECT
      TRIM(value) AS VAL,
      UPPER(TRIM(value)) AS VAL_UPPER,
      TRY_TO_DECIMAL(TRIM(value), 10, 2) AS VAL_NUM
    FROM TABLE(SPLIT_TO_TABLE(?, '',''))
    WHERE TRIM(value) <> ''''
  )
  SELECT CHECK_ID, CHECK_NAME, ${numCol} AS CHECK_NUM, EDC_TABLE, PROC_NAME, SOURCE_TABLES, PART
  FROM CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY
  WHERE COALESCE(ENABLED, TRUE)
`;
const binds = [selectorStr];
if (vPart !== "all") {
  checksSql += ` AND LOWER(PART) = ?`;
  binds.push(vPart);
}
if (vMode === "CHECK_NAME") {
  checksSql += ` AND EXISTS (SELECT 1 FROM SEL s WHERE s.VAL_UPPER = UPPER(CHECK_NAME))`;
} else if (vMode === "CHECK_NUM" || vMode === "ROW_NUM") {
  checksSql += ` AND EXISTS (SELECT 1 FROM SEL s WHERE s.VAL_NUM IS NOT NULL AND s.VAL_NUM = ${numCol})`;
} else if (vMode === "SOURCE_TABLE") {
  checksSql += `
    AND EXISTS (
      SELECT 1 FROM SEL s
      WHERE ARRAY_CONTAINS(s.VAL_UPPER::variant, SOURCE_TABLES)
    )`;
} else if (vMode !== "ALL") {
  checksSql += ` AND 1=0`;
}
checksSql += ` ORDER BY ${numCol}, CHECK_NAME`;
const rs = q(checksSql, binds);
let anyFailed = false;

while (rs.next()) {
  const checkId = rs.getColumnValue(1);
  const checkName = rs.getColumnValue(2);
  const rowNum = rs.getColumnValue(3);
  const procName = rs.getColumnValue(5);

  q(
    `INSERT INTO ${logTbl}
     (RUN_ID, CHECK_ID, CHECK_NAME, ROW_NUM, PROC_NAME, STATUS, STARTED_AT, ERROR_MESSAGE)
     VALUES (?, ?, ?, ?, ?, ''RUNNING'', CURRENT_TIMESTAMP(), NULL)`,
    [runId, checkId, checkName, rowNum, procName]
  );

  if (!procName || procName.toString().trim() === '''') {
    q(
      `UPDATE ${logTbl}
       SET STATUS=''SKIPPED'', ENDED_AT=CURRENT_TIMESTAMP(), ERROR_MESSAGE=''PROC_NAME is NULL/empty''
       WHERE RUN_ID=? AND CHECK_ID=? AND STATUS=''RUNNING''`,
      [runId, checkId]
    );
    continue;
  }
  if (!isSafeProcName(procName.toString().trim())) {
    anyFailed = true;
    q(
      `UPDATE ${logTbl}
       SET STATUS=''FAILED'', ENDED_AT=CURRENT_TIMESTAMP(), ERROR_MESSAGE=?
       WHERE RUN_ID=? AND CHECK_ID=? AND STATUS=''RUNNING''`,
      [`Unsafe PROC_NAME: ${procName}`, runId, checkId]
    );
    continue;
  }

  try {
    const maxArgs = getProcMaxArgs(procName.toString().trim());
    const args = [DB_PARAM, SCHEMA_NAME, runId];
    if (maxArgs >= 4) args.push(vTargetTable || ''ALL'');
    if (maxArgs >= 6) args.push(vPrevDb, vPrevSchema);
    const ph = args.map(() => ''?'').join('', '');
    q(`CALL ${procName}(${ph})`, args);

    q(
      `UPDATE ${logTbl}
       SET STATUS=''SUCCEEDED'', ENDED_AT=CURRENT_TIMESTAMP()
       WHERE RUN_ID=? AND CHECK_ID=? AND STATUS=''RUNNING''`,
      [runId, checkId]
    );
  } catch (e) {
    anyFailed = true;
    const msg = (e && e.message) ? e.message.toString().slice(0, 2000) : ''Unknown error'';
    q(
      `UPDATE ${logTbl}
       SET STATUS=''FAILED'', ENDED_AT=CURRENT_TIMESTAMP(), ERROR_MESSAGE=?
       WHERE RUN_ID=? AND CHECK_ID=? AND STATUS=''RUNNING''`,
      [msg, runId, checkId]
    );
  }
}

q(
  `UPDATE ${runsTbl}
   SET STATUS=?, ENDED_AT=CURRENT_TIMESTAMP(), ERROR_MESSAGE=NULL
   WHERE RUN_ID=?`,
  [anyFailed ? ''PARTIAL'' : ''SUCCEEDED'', runId]
);

return `OK RUN_ID=${runId} STATUS=${anyFailed ? ''PARTIAL'' : ''SUCCEEDED''} TARGET_TABLE=${vTargetTable || ''''} PREV_DB_PARAM=${vPrevDb || ''''} PREV_SCHEMA_NAME=${vPrevSchema || ''''}`;
';