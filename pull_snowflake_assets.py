"""Utility script to pull all Snowflake assets used by the Characterization project.

The script performs the following actions:
  1. Pulls **all stored procedures** from the `CHARACTERIZATION.DCQ_CHECKS` schema and stores them under
     `sql/dcq_checks/procedures/` (mirroring the folder structure).
  2. Pulls the driver procedure `SP_RUN_DCQ` from `CHARACTERIZATION.DCQ` and stores it under
     `sql/dcq/procedures/sp_run_dcq.sql`.
  3. Pulls the `POTENTIAL_CODE_ERRORS` procedure from `CHARACTERIZATION.DCQ_CHECKS` and stores it as
     `sql/dcq_checks/procedures/potential_code_errors.sql`.
  4. Exports **all tables** in the `EDC_REF` schema:
       - DDL files are written to `sql/edc_ref/tables/`.
       - Table data (full JSON export) is written to `snowflake/edc_ref/`.
  5. Exports **all views** in the `EDC_REF` schema to `sql/edc_ref/views/`.
  6. Exports the `DCQ_CHECK_REGISTRY` table DDL and data (JSON) to `snowflake/`.

All connections read credentials from the `.env` file located at the repository root.

Usage:
    python pull_snowflake_assets.py

The script is idempotent – it will only rewrite files if the content has changed.
"""

import os
import pathlib
import json
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
def ensure_dir(path: pathlib.Path):
    path.mkdir(parents=True, exist_ok=True)

def write_if_changed(file_path: pathlib.Path, content: str):
    if file_path.is_file() and file_path.read_text().strip() == content.strip():
        print(f'{file_path.name} is up‑to‑date')
    else:
        file_path.write_text(content)
        print(f'Written {file_path}')

def get_connection():
    return snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        private_key=private_key,
        role=ROLE,
        warehouse=WAREHOUSE,
        database=DATABASE,
        autocommit=True,
    )

# ---------------------------------------------------------------------------
# 1. Pull procedures from DCQ_CHECKS
# ---------------------------------------------------------------------------
def pull_dcq_checks_procedures(cs):
    schema = 'DCQ_CHECKS'
    cs.execute(f"SHOW PROCEDURES IN SCHEMA {DATABASE}.{schema}")
    all_procs = cs.fetchall()
    procedures = [p for p in all_procs if p['name'].startswith('SP_')]
    base_dir = BASE_DIR / 'sql' / schema.lower() / 'procedures'
    ensure_dir(base_dir)
    for proc in procedures:
        name = proc['name']
        args_raw = proc.get('arguments')
        if args_raw:
            open_idx = args_raw.find('(')
            close_idx = args_raw.find(')')
            if open_idx != -1 and close_idx != -1 and close_idx > open_idx:
                arg_list = args_raw[open_idx+1:close_idx].strip()
            else:
                arg_list = args_raw.strip()
            qualified = f"{DATABASE}.{schema}.{name}({arg_list})"
        else:
            qualified = f"{DATABASE}.{schema}.{name}"
        cs.execute(f"SELECT GET_DDL('PROCEDURE', '{qualified}') AS ddl")
        res = cs.fetchone()
        ddl = res['DDL'] if res else ''
        if ddl:
            write_if_changed(base_dir / f"{name}.sql", ddl)
        else:
            print(f'No DDL for {name}')

# ---------------------------------------------------------------------------
# 2. Pull SP_RUN_DCQ (driver) from DCQ schema
# ---------------------------------------------------------------------------
def pull_sp_run_dcq(cs):
    schema = 'DCQ'
    cs.execute(f"SHOW PROCEDURES IN SCHEMA {DATABASE}.{schema}")
    procs = cs.fetchall()
    proc = next((p for p in procs if p['name'].upper() == 'SP_RUN_DCQ'), None)
    if not proc:
        raise RuntimeError('SP_RUN_DCQ not found')
    name = proc['name']
    args_raw = proc.get('arguments')
    if args_raw:
        open_idx = args_raw.find('(')
        close_idx = args_raw.find(')')
        if open_idx != -1 and close_idx != -1 and close_idx > open_idx:
            arg_list = args_raw[open_idx+1:close_idx].strip()
        else:
            arg_list = args_raw.strip()
        qualified = f"{DATABASE}.{schema}.{name}({arg_list})"
    else:
        qualified = f"{DATABASE}.{schema}.{name}"
    cs.execute(f"SELECT GET_DDL('PROCEDURE', '{qualified}') AS ddl")
    ddl = cs.fetchone()['DDL']
    base_dir = BASE_DIR / 'sql' / schema.lower() / 'procedures'
    ensure_dir(base_dir)
    write_if_changed(base_dir / 'sp_run_dcq.sql', ddl)

# ---------------------------------------------------------------------------
# 3. Pull POTENTIAL_CODE_ERRORS procedure
# ---------------------------------------------------------------------------
def pull_potential_code_errors(cs):
    schema = 'DCQ_CHECKS'
    cs.execute(f"SHOW PROCEDURES IN SCHEMA {DATABASE}.{schema}")
    all_procs = cs.fetchall()
    proc = next((p for p in all_procs if 'POTENTIAL_CODE_ERRORS' in p['name'].upper()), None)
    if not proc:
        raise RuntimeError('POTENTIAL_CODE_ERRORS procedure not found')
    name = proc['name']
    args_raw = proc.get('arguments')
    if args_raw:
        open_idx = args_raw.find('(')
        close_idx = args_raw.find(')')
        if open_idx != -1 and close_idx != -1 and close_idx > open_idx:
            arg_list = args_raw[open_idx+1:close_idx].strip()
        else:
            arg_list = args_raw.strip()
        qualified = f"{DATABASE}.{schema}.{name}({arg_list})"
    else:
        qualified = f"{DATABASE}.{schema}.{name}"
    cs.execute(f"SELECT GET_DDL('PROCEDURE', '{qualified}') AS ddl")
    ddl = cs.fetchone()['DDL']
    base_dir = BASE_DIR / 'sql' / schema.lower() / 'procedures'
    ensure_dir(base_dir)
    write_if_changed(base_dir / f"{name.lower()}.sql", ddl)

# ---------------------------------------------------------------------------
# 4. Pull EDC_REF tables (DDL + data)
# ---------------------------------------------------------------------------
def pull_edc_ref_tables_and_data(cs):
    schema = 'EDC_REF'
    # DDL files
    ddl_dir = BASE_DIR / 'sql' / schema.lower() / 'tables'
    ensure_dir(ddl_dir)
    # Data JSON files
    json_dir = BASE_DIR / 'snowflake' / schema.lower()
    ensure_dir(json_dir)

    cs.execute(f"SHOW TABLES IN SCHEMA {DATABASE}.{schema}")
    tables = cs.fetchall()
    for tbl in tables:
        name = tbl['name']
        # DDL
        cs.execute(f"SELECT GET_DDL('TABLE', '{DATABASE}.{schema}.{name}') AS ddl")
        ddl = cs.fetchone()['DDL']
        write_if_changed(ddl_dir / f"{name}.sql", ddl)
        # Data export (full fetch)
        cs.execute(f"SELECT * FROM {DATABASE}.{schema}.{name}")
        rows = cs.fetchall()
        json_path = json_dir / f"{name}_data.json"
        json_path.write_text(json.dumps(rows, default=str, indent=2))
        print(f'Exported data for {name} ({len(rows)} rows)')

# ---------------------------------------------------------------------------
# 5. Pull EDC_REF views
# ---------------------------------------------------------------------------
def pull_edc_ref_views(cs):
    schema = 'EDC_REF'
    view_dir = BASE_DIR / 'sql' / schema.lower() / 'views'
    ensure_dir(view_dir)
    cs.execute(f"SHOW VIEWS IN SCHEMA {DATABASE}.{schema}")
    views = cs.fetchall()
    for v in views:
        name = v['name']
        cs.execute(f"SELECT GET_DDL('VIEW', '{DATABASE}.{schema}.{name}') AS ddl")
        ddl = cs.fetchone()['DDL']
        write_if_changed(view_dir / f"{name}.sql", ddl)

# ---------------------------------------------------------------------------
# 6. Pull DCQ_CHECK_REGISTRY table DDL + data
# ---------------------------------------------------------------------------
def pull_registry(cs):
    ddl_path = BASE_DIR / 'snowflake' / 'DCQ_CHECK_REGISTRY.sql'
    cs.execute(f"SELECT GET_DDL('TABLE', '{DATABASE}.DCQ.DCQ_CHECK_REGISTRY') AS ddl")
    ddl = cs.fetchone()['DDL']
    write_if_changed(ddl_path, ddl)
    # Data
    cs.execute(f"SELECT * FROM {DATABASE}.DCQ.DCQ_CHECK_REGISTRY")
    rows = cs.fetchall()
    json_path = BASE_DIR / 'snowflake' / 'DCQ_CHECK_REGISTRY_data_full.json'
    json_path.write_text(json.dumps(rows, default=str, indent=2))
    print(f'Exported DCQ_CHECK_REGISTRY data ({len(rows)} rows)')

# ---------------------------------------------------------------------------
# Main execution flow
# ---------------------------------------------------------------------------
def main():
    conn = get_connection()
    cs = conn.cursor(DictCursor)
    try:
        # 1) Procedures from DCQ_CHECKS
        pull_dcq_checks_procedures(cs)
        # 2) Driver procedure
        pull_sp_run_dcq(cs)
        # 3) Potential code errors procedure
        pull_potential_code_errors(cs)
        # 4) EDC_REF tables + data
        pull_edc_ref_tables_and_data(cs)
        # 5) EDC_REF views
        pull_edc_ref_views(cs)
        # 6) Registry table
        pull_registry(cs)
    finally:
        cs.close()
        conn.close()

if __name__ == '__main__':
    main()
