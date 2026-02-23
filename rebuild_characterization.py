"""Rebuild the CHARACTERIZATION database in Snowflake.

This script:
  1. Creates the CHARACTERIZATION database (if missing).
  2. Creates the required schemas: DCQ, DCQ_CHECKS, EDC_REF.
  3. Executes all DDL files for tables and views stored under `sql/edc_ref/`.
  4. Executes the DDL for the DCQ_CHECK_REGISTRY table.
  5. Loads JSON data for every EDC_REF table and for the registry table.
  6. Deploys all stored procedures from `sql/dcq_checks/procedures/` and the driver
     procedure under `sql/dcq/procedures/`.

The script relies on the same ```.env``` file you already have in the repository
for Snowflake connection details.  **Never commit secrets** – keep the .env file
out of version control.

Usage:
    python rebuild_characterization.py
"""

import os
import pathlib
import glob
import json
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector import DictCursor
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# ---------------------------------------------------------------------------
# Load environment variables (same layout as the original repo)
# ---------------------------------------------------------------------------
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
USER = os.getenv('SNOWFLAKE_USER')
PRIVATE_KEY_PATH = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
DATABASE = os.getenv('SNOWFLAKE_DATABASE') or 'CHARACTERIZATION'
ROLE = os.getenv('SNOWFLAKE_ROLE')

required = [ACCOUNT, USER, PRIVATE_KEY_PATH, WAREHOUSE, DATABASE, ROLE]
if not all(required):
    raise RuntimeError('Missing one or more Snowflake connection variables in .env')
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
# Helper functions
# ---------------------------------------------------------------------------
def exec_sql(cs, sql, *args):
    """Execute a SQL statement, printing a short description for debugging."""
    cs.execute(sql, args)

def put_file(cs, local_path, stage_path):
    """Upload a local file to an internal stage using the Snowflake PUT command."""
    # The stage_path is like "@~/rebuild_stage/<filename>"
    cs.execute(f"PUT file://{local_path} {stage_path}")

def copy_into_table(cs, table_fqn, stage_path, file_name):
    """COPY JSON data from the internal stage into the target table.

    Assumes the JSON is an array of objects, each object representing a row.
    """
    cs.execute(
        f"""
        COPY INTO {table_fqn}
        FROM (SELECT $1 FROM {stage_path}/{file_name}.json.gz (FILE_FORMAT => (TYPE=>'JSON')))
        FILE_FORMAT = (TYPE => 'JSON')
        ON_ERROR = 'ABORT_STATEMENT'
        """
    )

# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------
ctx = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    private_key=private_key,
    role=ROLE,
    warehouse=WAREHOUSE,
    database=DATABASE,
    autocommit=True,
)
cs = ctx.cursor(DictCursor)
try:
    # 1. Ensure the database exists (CREATE DATABASE IF NOT EXISTS)
    exec_sql(cs, f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    exec_sql(cs, f"USE DATABASE {DATABASE}")

    # 2. Create required schemas
    for schema in ['DCQ', 'DCQ_CHECKS', 'EDC_REF']:
        exec_sql(cs, f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # -------------------------------------------------------------------
    # 3. Run DDL for EDC_REF tables and views
    # -------------------------------------------------------------------
    # Tables
    table_ddl_files = glob.glob('sql/edc_ref/tables/*.sql')
    for ddl_path in sorted(table_ddl_files):
        ddl_sql = pathlib.Path(ddl_path).read_text()
        print(f'Executing DDL for table: {ddl_path}')
        cs.execute(ddl_sql)

    # Views
    view_ddl_files = glob.glob('sql/edc_ref/views/*.sql')
    for ddl_path in sorted(view_ddl_files):
        ddl_sql = pathlib.Path(ddl_path).read_text()
        print(f'Executing DDL for view: {ddl_path}')
        cs.execute(ddl_sql)

    # -------------------------------------------------------------------
    # 4. Create DCQ_CHECK_REGISTRY table
    # -------------------------------------------------------------------
    registry_ddl_path = 'snowflake/DCQ_CHECK_REGISTRY.sql'
    registry_ddl = pathlib.Path(registry_ddl_path).read_text()
    print('Creating DCQ_CHECK_REGISTRY table')
    cs.execute(registry_ddl)

    # -------------------------------------------------------------------
    # 5. Load JSON data for EDC_REF tables
    # -------------------------------------------------------------------
    # Create a temporary internal stage for the upload
    stage_name = '@~/rebuild_stage'
    exec_sql(cs, f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}")

    edc_json_dir = pathlib.Path('snowflake/edc_ref')
    for json_file in sorted(edc_json_dir.glob('*_data.json')):
        table_name = json_file.name.replace('_data.json', '')
        fqn = f"{DATABASE}.EDC_REF.{table_name}"
        # Upload file
        put_file(cs, str(json_file), stage_name)
        # Copy into table (assuming the file was gzipped automatically by PUT)
        copy_into_table(cs, fqn, stage_name, json_file.stem)
        print(f'Loaded data for {table_name}')

    # -------------------------------------------------------------------
    # 6. Load registry data (the 48‑row JSON)
    # -------------------------------------------------------------------
    registry_json = pathlib.Path('snowflake/DCQ_CHECK_REGISTRY_data_full.json')
    put_file(cs, str(registry_json), stage_name)
    copy_into_table(cs,
                    f"{DATABASE}.DCQ.DCQ_CHECK_REGISTRY",
                    stage_name,
                    registry_json.stem)
    print('Loaded DCQ_CHECK_REGISTRY data')

    # -------------------------------------------------------------------
    # 7. Deploy stored procedures
    # -------------------------------------------------------------------
    # DCQ_CHECKS procedures (including potential_code_errors)
    proc_dir = pathlib.Path('sql/dcq_checks/procedures')
    for proc_path in sorted(proc_dir.glob('*.sql')):
        proc_sql = proc_path.read_text()
        print(f'Deploying procedure {proc_path.name}')
        cs.execute(proc_sql)

    # DCQ driver procedure
    driver_sql = pathlib.Path('sql/dcq/procedures/sp_run_dcq.sql').read_text()
    print('Deploying driver procedure sp_run_dcq')
    cs.execute(driver_sql)

    print('\n✅ CHARACTERIZATION database rebuilt successfully!')
finally:
    cs.close()
    ctx.close()
