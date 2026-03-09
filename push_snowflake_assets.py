'''Utility script to push local Snowflake assets (procedures, tables, views, Streamlit app)
to the Snowflake instance.

The script performs the following actions:
  1. Reads all .sql files under the `sql/` directory (including procedures, tables, and views).
  2. Executes the DDL statements against the Snowflake instance using the Snowflake Python connector.
  3. (Optional) Loads JSON data files from the `snowflake/` directory into their respective tables.
  4. Deploys the Streamlit app from `streamlit/app.py` to Snowflake.

All connection credentials are read from the `.env` file located at the repository root.

Usage:
    python push_snowflake_assets.py
''' 

import os
import pathlib
import glob
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector import DictCursor
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# ---------------------------------------------------------------------------
# Load environment variables (same as other helper scripts)
# ---------------------------------------------------------------------------
BASE_DIR = pathlib.Path(__file__).parent
load_dotenv(BASE_DIR / '.env')

ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
USER = os.getenv('SNOWFLAKE_USER')
PRIVATE_KEY_PATH = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
DATABASE = os.getenv('SNOWFLAKE_DATABASE') or 'CHARACTERIZATION'
ROLE = os.getenv('SNOWFLAKE_ROLE')
SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

# Streamlit app deployment settings
STREAMLIT_DB = os.getenv('STREAMLIT_DATABASE', 'STREAMLIT')
STREAMLIT_SCHEMA = os.getenv('STREAMLIT_SCHEMA', 'PUBLIC')
STREAMLIT_NAME = os.getenv('STREAMLIT_NAME', 'characterization_runner')
STREAMLIT_WAREHOUSE = os.getenv('STREAMLIT_WAREHOUSE', 'STREAMLIT_XS')
STREAMLIT_STAGE = os.getenv('STREAMLIT_STAGE', 'streamlit_deploy_stage')

if not all([ACCOUNT, USER, PRIVATE_KEY_PATH, WAREHOUSE, DATABASE, ROLE]):
    raise RuntimeError('Missing Snowflake connection info in .env')
if not os.path.exists(os.path.expanduser(str(PRIVATE_KEY_PATH))):
    raise RuntimeError(f'Private key not found at {PRIVATE_KEY_PATH}')

# ---------------------------------------------------------------------------
# Load private key
# ---------------------------------------------------------------------------
with open(os.path.expanduser(str(PRIVATE_KEY_PATH)), 'rb') as f:
    key_data = f.read()
    private_key = serialization.load_pem_private_key(
        key_data, password=None, backend=default_backend()
    )

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------
def get_connection():
    """Create and return a Snowflake connection using the loaded credentials."""
    return snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        private_key=private_key,
        role=ROLE,
        warehouse=WAREHOUSE,
        database=DATABASE,
        autocommit=True,
    )

def execute_sql_file(cursor, file_path: pathlib.Path):
    """Read the SQL file, clean up the DDL if needed, and execute it.
    For stored procedures (CREATE ... PROCEDURE), fixes the identifier quoting
    only in the header line (before the AS keyword) so the JavaScript body is
    left intact.
    """
    import re
    sql = file_path.read_text().strip()
    if not sql:
        print(f'Skipping empty file {file_path}')
        return

    # Detect stored procedure DDL: starts with CREATE ... PROCEDURE and has
    # a JavaScript body delimited by AS '...'
    upper = sql.upper()
    is_proc = 'PROCEDURE' in upper.split('\n')[0]

    if is_proc:
        # Find the position of the first "AS" keyword that starts the JS body.
        # We only want to fix quoting in the header portion (before AS).
        as_match = re.search(r"\nAS\s*'", sql, re.IGNORECASE)
        if as_match:
            header = sql[:as_match.start()]
            body = sql[as_match.start():]
        else:
            header = sql
            body = ''

        # Remove ALL double quotes from the header.  The header contains only
        # SQL keywords, identifiers, and type names – none of which require
        # double-quoting for our purposes.
        header = header.replace('"', '')

        sql = header + body
    else:
        # For non-procedure DDL, apply the original transformations
        sql = re.sub(r'"\("', '(', sql)
        sql = re.sub(r'"([A-Za-z_][A-Za-z0-9_$]*)"', r'\1', sql)
        sql = re.sub(r'"\)', r')', sql)

    try:
        cursor.execute(sql)
        print(f'Executed {file_path.name}')
    except Exception as e:
        print(f'Error executing {file_path.name}: {e}')

def push_sql_assets(cs):
    """Find .sql files for tables and views under the sql/ directory and execute them.
    This will create/replace tables and views. Procedures are skipped because their DDL
    contains placeholders that require custom handling.
    It also sets the appropriate database and schema context before each DDL.
    """
    sql_root = BASE_DIR / 'sql'
    pattern = str(sql_root / '**' / '*.sql')
    sql_files = glob.glob(pattern, recursive=True)
    if not sql_files:
        print('No .sql files found to push.')
        return
    # Ensure database context is set
    try:
        cs.execute(f'USE DATABASE {DATABASE}')
    except Exception as e:
        print(f'Error setting database: {e}')
    for f in sorted(sql_files):
        file_path = pathlib.Path(f)
        # Determine what kind of object this .sql file defines
        parts = file_path.relative_to(sql_root).parts
        # Expect at least sql/<schema>/<type>/<file>
        if len(parts) < 3:
            continue
        # parts[1] indicates the object type: 'tables', 'views', or 'procedures'
        obj_type = parts[1].lower()
        if obj_type == 'procedures':
            # Procedures contain fully‑qualified names; no schema context needed
            execute_sql_file(cs, file_path)
            continue
        if obj_type not in ('tables', 'views'):
            # Skip any other files (e.g., scripts, docs)
            continue
        # For tables and views, set the appropriate schema before execution
        try:
            schema = parts[0].upper()
            cs.execute(f'USE SCHEMA {DATABASE}.{schema}')
        except Exception as e:
            print(f'Error setting schema for {file_path}: {e}')
        execute_sql_file(cs, file_path)

# ---------------------------------------------------------------------------
# Load JSON data into tables
# ---------------------------------------------------------------------------
def load_json_data(cs):
    """Load JSON data files from the snowflake/ directory into Snowflake tables.

    Supports both plain `.json` and gzipped `.json.gz` files.  Files are
    uploaded to a temporary stage and loaded via COPY INTO with
    STRIP_OUTER_ARRAY (each file is a JSON array of row-objects).

    Naming conventions:
      - snowflake/edc_ref/<TABLE>_data.json(.gz)  →  CHARACTERIZATION.EDC_REF.<TABLE>
      - snowflake/<TABLE>_data_full.json(.gz)      →  CHARACTERIZATION.DCQ.<TABLE>
    """
    import json as _json, gzip as _gzip

    # Use a plain cursor (not DictCursor) so fetchall() returns tuples
    raw_cs = cs.connection.cursor()

    json_root = BASE_DIR / 'snowflake'

    # Collect all *_data*.json and *_data*.json.gz files
    data_files = sorted(json_root.rglob('*_data*.json')) + sorted(json_root.rglob('*_data*.json.gz'))
    if not data_files:
        print('No JSON data files found – skipping data load.')
        return

    # Ensure schema context for temp stage
    raw_cs.execute(f'USE DATABASE {DATABASE}')
    raw_cs.execute(f'USE SCHEMA {DATABASE}.EDC_REF')
    raw_cs.execute('CREATE OR REPLACE TEMPORARY STAGE data_load_stage')

    for path in data_files:
        # Determine schema and table from path
        rel = path.relative_to(json_root)
        parts = rel.parts

        if len(parts) >= 2:
            # e.g., edc_ref/CDM_PARSEABLE_RAW_data.json.gz
            schema_name = parts[0].upper()
            fname = path.name
        else:
            # e.g., DCQ_CHECK_REGISTRY_data_full.json (top level → DCQ schema)
            schema_name = 'DCQ'
            fname = path.name

        # Extract table name: strip _data*.json(.gz)
        stem = fname.replace('.gz', '').replace('.json', '')
        # Handle both _data and _data_full patterns
        for suffix in ('_data_full', '_data'):
            if stem.endswith(suffix):
                table_name = stem[:-len(suffix)].upper()
                break
        else:
            print(f'Skipping {path}: cannot determine table name')
            continue

        full_table = f'{DATABASE}.{schema_name}.{table_name}'
        is_gz = path.name.endswith('.gz')

        # Check if file has data (skip empty arrays)
        try:
            if is_gz:
                with _gzip.open(path, 'rt', encoding='utf-8') as f:
                    data = _json.load(f)
            else:
                data = _json.loads(path.read_text())
            if not isinstance(data, list) or len(data) == 0:
                print(f'Skipping {fname}: empty or not a JSON array')
                continue
        except Exception as e:
            print(f'Skipping {fname}: cannot read JSON: {e}')
            continue

        row_count = len(data)

        try:
            # Get column metadata for the target table
            raw_cs.execute(
                f"SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE "
                f"FROM {DATABASE}.INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
                (schema_name, table_name)
            )
            col_info = raw_cs.fetchall()
            if not col_info:
                print(f'Skipping {fname}: table {full_table} not found or has no columns')
                continue

            col_names = [c[0] for c in col_info]

            # Build typed SELECT expressions
            select_parts = []
            for c in col_info:
                name, dt, prec, scale = c[0], c[1].upper(), c[2], c[3]
                if 'NUMBER' in dt or 'INT' in dt or 'FLOAT' in dt or 'DOUBLE' in dt or 'DECIMAL' in dt:
                    if prec is not None and scale is not None:
                        select_parts.append(f'$1:{name}::NUMBER({prec},{scale})')
                    else:
                        select_parts.append(f'$1:{name}::NUMBER')
                elif 'BOOLEAN' in dt:
                    select_parts.append(f'$1:{name}::BOOLEAN')
                elif 'VARIANT' in dt:
                    select_parts.append(f'$1:{name}::VARIANT')
                elif 'ARRAY' in dt:
                    select_parts.append(f'$1:{name}::ARRAY')
                else:
                    select_parts.append(f'$1:{name}::VARCHAR')

            # Upload to stage
            raw_cs.execute(f"PUT file://{path} @data_load_stage/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

            # Truncate and load
            raw_cs.execute(f'TRUNCATE TABLE {full_table}')
            compression = "'GZIP'" if is_gz else "'NONE'"
            copy_sql = f"""
                COPY INTO {full_table} ({', '.join(col_names)})
                FROM (SELECT {', '.join(select_parts)} FROM @data_load_stage/{fname})
                FILE_FORMAT = (TYPE='JSON' STRIP_OUTER_ARRAY=TRUE COMPRESSION={compression})
            """
            raw_cs.execute(copy_sql)
            print(f'Loaded {row_count} rows into {full_table}')
        except Exception as e:
            print(f'Error loading {fname} into {full_table}: {e}')

    raw_cs.close()

# ---------------------------------------------------------------------------
# Deploy the Streamlit app
# ---------------------------------------------------------------------------
def push_streamlit_app(cs):
    """Upload streamlit/app.py to an internal stage and CREATE OR REPLACE the
    Snowflake Streamlit app so it picks up the latest code."""
    app_path = BASE_DIR / 'streamlit' / 'app.py'
    if not app_path.exists():
        print('streamlit/app.py not found – skipping Streamlit deployment.')
        return

    stage_fqn = f'{STREAMLIT_DB}.{STREAMLIT_SCHEMA}.{STREAMLIT_STAGE}'
    streamlit_fqn = f'{STREAMLIT_DB}.{STREAMLIT_SCHEMA}.{STREAMLIT_NAME}'

    try:
        cs.execute(f'CREATE STAGE IF NOT EXISTS {stage_fqn}')
        cs.execute(
            f"PUT file://{app_path} @{stage_fqn}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        cs.execute(f"""
            CREATE OR REPLACE STREAMLIT {streamlit_fqn}
            ROOT_LOCATION = '@{stage_fqn}'
            MAIN_FILE = 'app.py'
            QUERY_WAREHOUSE = '{STREAMLIT_WAREHOUSE}'
            TITLE = '{STREAMLIT_NAME}'
        """)
        print(f'Deployed Streamlit app -> {streamlit_fqn}')
    except Exception as e:
        print(f'Error deploying Streamlit app: {e}')


# ---------------------------------------------------------------------------
# Main execution flow
# ---------------------------------------------------------------------------
def main():
    conn = get_connection()
    cs = conn.cursor(DictCursor)
    try:
        push_sql_assets(cs)
        load_json_data(cs)
        push_streamlit_app(cs)
    finally:
        cs.close()
        conn.close()

if __name__ == '__main__':
    main()
