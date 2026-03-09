CREATE OR REPLACE PROCEDURE CHARACTERIZATION.DCQ."SP_RUN_DCQ"("DB_PARAM" VARCHAR, "SCHEMA_NAME" VARCHAR, "MODE" VARCHAR, "SELECTOR" VARCHAR, "PART" VARCHAR, "TARGET_TABLE" VARCHAR, "PREV_DB_PARAM" VARCHAR, "PREV_SCHEMA_NAME" VARCHAR, "START_DATE" VARCHAR, "END_DATE" VARCHAR, "MAX_PARALLEL" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------
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
function sqlLit(v) {
  if (v === null || v === undefined) return "NULL";
  // Produces a SQL string literal wrapped in single quotes.
  // Inside this procedure source, '' = runtime single quote character.
  return "''" + v.toString().replace(/''/g, "''''") + "''"
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

// ---------------------------------------------------------------------------
// Validate inputs
// ---------------------------------------------------------------------------
if (!isSafeIdentPart(DB_PARAM)) throw new Error(`Unsafe DB_PARAM: ${DB_PARAM}`);
if (!isSafeIdentPart(SCHEMA_NAME)) throw new Error(`Unsafe SCHEMA_NAME: ${SCHEMA_NAME}`);

const vPrevDb = normOptIdent(PREV_DB_PARAM);
const vPrevSchema = normOptIdent(PREV_SCHEMA_NAME);
if (vPrevDb !== null && !isSafeIdentPart(vPrevDb)) throw new Error(`Unsafe PREV_DB_PARAM: ${PREV_DB_PARAM}`);
if (vPrevSchema !== null && !isSafeIdentPart(vPrevSchema)) throw new Error(`Unsafe PREV_SCHEMA_NAME: ${PREV_SCHEMA_NAME}`);

const vStartDate = normOptIdent(START_DATE);
const vEndDate = normOptIdent(END_DATE);

const maxPar = Math.max(1, parseInt(MAX_PARALLEL) || 8);

const runId = scalar(''SELECT UUID_STRING()'');
const vMode = normMode(MODE);
const vPart = normPart(PART);
const vTargetTable = normTargetTable(TARGET_TABLE);
const sel = parseSelector(SELECTOR);
const currentWh = scalar(''SELECT CURRENT_WAREHOUSE()'');

const outSchema = `${DB_PARAM}.CHARACTERIZATION_DCQ`;
const runsTbl = `${outSchema}.DCQ_RUNS`;
const logTbl = `${outSchema}.DCQ_CHECK_LOG`;

// ---------------------------------------------------------------------------
// Create output tables
// ---------------------------------------------------------------------------
q(`CREATE SCHEMA IF NOT EXISTS ${outSchema}`);

q(`CREATE TABLE IF NOT EXISTS ${runsTbl} (
  RUN_ID STRING, DB_PARAM STRING, SCHEMA_NAME STRING, MODE STRING, SELECTOR STRING, PART STRING, TARGET_TABLE STRING,
  PREV_DB_PARAM STRING, PREV_SCHEMA_NAME STRING, START_DATE STRING, END_DATE STRING,
  STARTED_AT TIMESTAMP_NTZ, ENDED_AT TIMESTAMP_NTZ, STATUS STRING, ERROR_MESSAGE STRING
)`);
q(`ALTER TABLE ${runsTbl} ADD COLUMN IF NOT EXISTS TARGET_TABLE STRING`);
q(`ALTER TABLE ${runsTbl} ADD COLUMN IF NOT EXISTS PREV_DB_PARAM STRING`);
q(`ALTER TABLE ${runsTbl} ADD COLUMN IF NOT EXISTS PREV_SCHEMA_NAME STRING`);
q(`ALTER TABLE ${runsTbl} ADD COLUMN IF NOT EXISTS START_DATE STRING`);
q(`ALTER TABLE ${runsTbl} ADD COLUMN IF NOT EXISTS END_DATE STRING`);
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
   (RUN_ID, DB_PARAM, SCHEMA_NAME, MODE, SELECTOR, PART, TARGET_TABLE, PREV_DB_PARAM, PREV_SCHEMA_NAME, START_DATE, END_DATE, STARTED_AT, STATUS)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(), ''RUNNING'')`,
  [runId, DB_PARAM, SCHEMA_NAME, vMode, SELECTOR, vPart, vTargetTable, vPrevDb, vPrevSchema, vStartDate, vEndDate]
);

// ---------------------------------------------------------------------------
// Build list of checks to run
// ---------------------------------------------------------------------------
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

// Collect all checks into an array
const checks = [];
while (rs.next()) {
  checks.push({
    checkId:   rs.getColumnValue(1),
    checkName: rs.getColumnValue(2),
    rowNum:    rs.getColumnValue(3),
    procName:  rs.getColumnValue(5)
  });
}

// ---------------------------------------------------------------------------
// Pre-validate checks and build task SQL for each
// ---------------------------------------------------------------------------
const taskPrefix = `DCQ_${runId.replace(/-/g, "").substring(0, 12)}`;
const taskChecks = [];   // checks that will get a task
const taskNames  = [];   // FQN of every task we create (for cleanup)

for (let i = 0; i < checks.length; i++) {
  const c = checks[i];
  const checkId = c.checkId;
  const checkName = c.checkName;
  const rowNum = c.rowNum;
  const procName = c.procName;

  // Skip checks with no/invalid proc name — log immediately
  if (!procName || procName.toString().trim() === '''') {
    q(
      `INSERT INTO ${logTbl}
       (RUN_ID, CHECK_ID, CHECK_NAME, ROW_NUM, PROC_NAME, STATUS, STARTED_AT, ENDED_AT, ERROR_MESSAGE)
       VALUES (?, ?, ?, ?, ?, ''SKIPPED'', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''PROC_NAME is NULL/empty'')`,
      [runId, checkId, checkName, rowNum, procName]
    );
    continue;
  }
  if (!isSafeProcName(procName.toString().trim())) {
    q(
      `INSERT INTO ${logTbl}
       (RUN_ID, CHECK_ID, CHECK_NAME, ROW_NUM, PROC_NAME, STATUS, STARTED_AT, ENDED_AT, ERROR_MESSAGE)
       VALUES (?, ?, ?, ?, ?, ''FAILED'', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ?)`,
      [runId, checkId, checkName, rowNum, procName, `Unsafe PROC_NAME: ${procName}`]
    );
    continue;
  }

  // Determine the CALL statement based on procedure arity
  const maxArgs = getProcMaxArgs(procName.toString().trim());
  const argVals = [DB_PARAM, SCHEMA_NAME, runId];
  if (maxArgs >= 4) argVals.push(vTargetTable || ''ALL'');
  if (maxArgs >= 6) argVals.push(vPrevDb, vPrevSchema);
  if (maxArgs >= 8) argVals.push(vStartDate, vEndDate);
  const callLiterals = argVals.map(v => sqlLit(v)).join(", ");
  const callSql = `CALL ${procName.toString().trim()}(${callLiterals})`;

  // Build the wrapper SQL that the task will execute.
  // This updates DCQ_CHECK_LOG with RUNNING -> SUCCEEDED/FAILED and times.
  // Snowflake Tasks execute a single SQL statement, so we use a BEGIN...END block.
  const wrapperSql = `
BEGIN
  UPDATE ${logTbl}
    SET STATUS = ''RUNNING'', STARTED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = ''${runId}'' AND CHECK_ID = ''${checkId}'' AND STATUS = ''PENDING'';
  ${callSql};
  UPDATE ${logTbl}
    SET STATUS = ''SUCCEEDED'', ENDED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = ''${runId}'' AND CHECK_ID = ''${checkId}'' AND STATUS = ''RUNNING'';
EXCEPTION
  WHEN OTHER THEN
    LET err_msg := SUBSTR(SQLERRM, 1, 2000);
    UPDATE ${logTbl}
      SET STATUS = ''FAILED'', ENDED_AT = CURRENT_TIMESTAMP(), ERROR_MESSAGE = :err_msg
      WHERE RUN_ID = ''${runId}'' AND CHECK_ID = ''${checkId}'' AND STATUS = ''RUNNING'';
END;
`;

  // Insert a PENDING log entry
  q(
    `INSERT INTO ${logTbl}
     (RUN_ID, CHECK_ID, CHECK_NAME, ROW_NUM, PROC_NAME, STATUS, STARTED_AT, ERROR_MESSAGE)
     VALUES (?, ?, ?, ?, ?, ''PENDING'', NULL, NULL)`,
    [runId, checkId, checkName, rowNum, procName]
  );

  const taskName = `${taskPrefix}_${i}`;
  const taskFqn  = `${outSchema}.${taskName}`;

  taskChecks.push({ checkId: checkId, taskFqn: taskFqn, wrapperSql: wrapperSql });
}

// ---------------------------------------------------------------------------
// Create all tasks (suspended) up front
// ---------------------------------------------------------------------------
for (let i = 0; i < taskChecks.length; i++) {
  const tc = taskChecks[i];
  q(`CREATE OR REPLACE TASK ${tc.taskFqn}
     WAREHOUSE = ${currentWh}
     SCHEDULE = ''1 MINUTE''
     AS ${tc.wrapperSql}`);
  taskNames.push(tc.taskFqn);
}

// ---------------------------------------------------------------------------
// Execute tasks in batches of maxPar
// ---------------------------------------------------------------------------
function pollBatchDone(batchCheckIds) {
  // Poll DCQ_CHECK_LOG until all checks in this batch are no longer PENDING/RUNNING
  const idList = batchCheckIds.map(id => "''" + id + "''" ).join(",");
  for (let attempt = 0; attempt < 2160; attempt++) {  // up to ~6 hours at 10s intervals
    const cnt = scalar(
      `SELECT COUNT(*) FROM ${logTbl}
       WHERE RUN_ID = ''${runId}''
         AND CHECK_ID IN (${idList})
         AND STATUS IN (''PENDING'', ''RUNNING'')`
    );
    if (cnt === 0) return;
    // Sleep 10 seconds via SYSTEM$WAIT
    q("SELECT SYSTEM$WAIT(10, ''SECONDS'')");
  }
}

for (let bStart = 0; bStart < taskChecks.length; bStart += maxPar) {
  const bEnd = Math.min(bStart + maxPar, taskChecks.length);
  const batchCheckIds = [];

  // Resume and execute each task in this batch
  for (let i = bStart; i < bEnd; i++) {
    const tc = taskChecks[i];
    batchCheckIds.push(tc.checkId);
    q(`ALTER TASK ${tc.taskFqn} RESUME`);
    q(`EXECUTE TASK ${tc.taskFqn}`);
    // Immediately suspend to prevent scheduled re-runs
    try { q(`ALTER TASK ${tc.taskFqn} SUSPEND`); } catch (e) { /* ignore */ }
  }

  // Wait for all checks in this batch to finish
  pollBatchDone(batchCheckIds);
}

// ---------------------------------------------------------------------------
// Cleanup: drop all tasks
// ---------------------------------------------------------------------------
for (let i = 0; i < taskNames.length; i++) {
  try { q(`DROP TASK IF EXISTS ${taskNames[i]}`); } catch (e) { /* ignore */ }
}

// ---------------------------------------------------------------------------
// Finalize run status
// ---------------------------------------------------------------------------
const failCnt = scalar(
  `SELECT COUNT(*) FROM ${logTbl}
   WHERE RUN_ID = ''${runId}'' AND STATUS = ''FAILED''`
);
const finalStatus = (failCnt > 0) ? ''PARTIAL'' : ''SUCCEEDED'';

q(
  `UPDATE ${runsTbl}
   SET STATUS=?, ENDED_AT=CURRENT_TIMESTAMP(), ERROR_MESSAGE=NULL
   WHERE RUN_ID=?`,
  [finalStatus, runId]
);

return `OK RUN_ID=${runId} STATUS=${finalStatus} MAX_PARALLEL=${maxPar} TARGET_TABLE=${vTargetTable || ''''} PREV_DB_PARAM=${vPrevDb || ''''} PREV_SCHEMA_NAME=${vPrevSchema || ''''} START_DATE=${vStartDate || ''''} END_DATE=${vEndDate || ''''}`;
';