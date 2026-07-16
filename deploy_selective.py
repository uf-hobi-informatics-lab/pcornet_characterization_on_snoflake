"""Selective Snowflake deploy - pushes ONLY the files changed for the
DC 3.18 / DC 1.13 SNOMED / DC 3.03 IVC+IVD full port work.

Files deployed:
  - sql/dcq_checks/procedures/SP_DC_3_18.sql            (new proc)
  - sql/dcq_checks/procedures/SP_DC_3_03_IVC.sql        (new proc: SAS Table IVC port)
  - sql/dcq_checks/procedures/SP_DC_3_03_IVD.sql        (new proc: SAS Table IVD port)
  - sql/dcq_checks/procedures/potential_code_errors.sql (SNOMED expansion)
  - snowflake/DCQ_CHECK_REGISTRY_data_full.json         (new DC_3_18 row +
                                                         DC_1_13 SOURCE_TABLES +
                                                         DC_3_03_IVC/IVD replaces DC_3_03)

Also drops the legacy proc in Snowflake:
  - CHARACTERIZATION.DCQ_CHECKS.SP_DC_3_03 (superseded by IVC/IVD split)

Files NOT deployed:
  - The other 45 SP_DC_*.sql procedures.
  - sp_run_dcq.sql (orchestrator, unchanged).
  - Any views or reference tables.
  - streamlit/app.py (unchanged, no need to touch the app).

Reuses the same .env / RSA key auth as push_snowflake_assets.py.

Usage:
    python deploy_selective.py             # deploy all artifacts + drop legacy
    python deploy_selective.py --dry-run   # show what would happen, no changes
    python deploy_selective.py --procs     # only redeploy procs + drop legacy
    python deploy_selective.py --registry  # only reload the registry table

Safety:
  - Uses CREATE OR REPLACE PROCEDURE; each proc is one atomic Snowflake
    transaction. If a proc's SQL is bad, the OLD version stays.
  - The DROP of the legacy SP_DC_3_03 uses IF EXISTS, so re-running is safe.
  - The registry load does TRUNCATE + COPY INTO. If the JSON parse fails
    partway, the registry ends up empty. That is why 01_capture_current_state.sql
    creates a clone backup before running this script.
  - Does NOT touch the Streamlit app.
"""
import argparse
import glob
import json
import os
import pathlib
import re
import sys

from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector import DictCursor
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

BASE_DIR = pathlib.Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
USER = os.getenv("SNOWFLAKE_USER")
PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
DATABASE = os.getenv("SNOWFLAKE_DATABASE") or "CHARACTERIZATION"
ROLE = os.getenv("SNOWFLAKE_ROLE")

# Files we intend to deploy. These paths are relative to BASE_DIR.
PROC_FILES = [
    "sql/dcq_checks/procedures/SP_DC_3_18.sql",
    "sql/dcq_checks/procedures/SP_DC_3_03_IVC.sql",
    "sql/dcq_checks/procedures/SP_DC_3_03_IVD.sql",
    "sql/dcq_checks/procedures/potential_code_errors.sql",
]

# Procedures to DROP (superseded / renamed). Uses IF EXISTS so re-running is safe.
# Signature must match exactly for Snowflake to resolve.
LEGACY_DROPS = [
    "DROP PROCEDURE IF EXISTS "
    f"{os.getenv('SNOWFLAKE_DATABASE') or 'CHARACTERIZATION'}"
    ".DCQ_CHECKS.SP_DC_3_03("
    "VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR)",
]
REGISTRY_JSON = "snowflake/DCQ_CHECK_REGISTRY_data_full.json"
REGISTRY_TABLE = f"{DATABASE}.DCQ.DCQ_CHECK_REGISTRY"


def check_env():
    missing = [k for k, v in {
        "SNOWFLAKE_ACCOUNT": ACCOUNT,
        "SNOWFLAKE_USER": USER,
        "SNOWFLAKE_PRIVATE_KEY_PATH": PRIVATE_KEY_PATH,
        "SNOWFLAKE_WAREHOUSE": WAREHOUSE,
        "SNOWFLAKE_ROLE": ROLE,
    }.items() if not v]
    if missing:
        sys.exit(f"Missing required env vars in .env: {', '.join(missing)}")
    key_path = os.path.expanduser(str(PRIVATE_KEY_PATH))
    if not os.path.exists(key_path):
        sys.exit(f"Private key not found at {key_path}")


def load_private_key():
    with open(os.path.expanduser(str(PRIVATE_KEY_PATH)), "rb") as f:
        return serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )


def get_connection():
    return snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        private_key=load_private_key(),
        role=ROLE,
        warehouse=WAREHOUSE,
        database=DATABASE,
        autocommit=True,
    )


def strip_header_quotes(sql: str) -> str:
    """Match push_snowflake_assets.py: strip double-quotes from the CREATE ...
    PROCEDURE header (before AS) but leave the JavaScript body intact."""
    as_match = re.search(r"\nAS\s*'", sql, re.IGNORECASE)
    if as_match:
        header = sql[: as_match.start()]
        body = sql[as_match.start():]
    else:
        header = sql
        body = ""
    header = header.replace('"', "")
    return header + body


def deploy_procs(cs, dry_run: bool) -> list[tuple[str, bool, str]]:
    results = []
    for rel_path in PROC_FILES:
        path = BASE_DIR / rel_path
        if not path.exists():
            results.append((rel_path, False, "file not found"))
            continue
        sql = path.read_text().strip()
        if not sql:
            results.append((rel_path, False, "file is empty"))
            continue
        sql_to_run = strip_header_quotes(sql)
        if dry_run:
            preview = sql_to_run[:120].replace("\n", " ")
            results.append((rel_path, True, f"DRY-RUN would run: {preview}..."))
            continue
        try:
            cs.execute(sql_to_run)
            results.append((rel_path, True, "OK - CREATE OR REPLACE succeeded"))
        except Exception as e:
            results.append((rel_path, False, f"ERROR: {e}"))
    return results


def drop_legacy_procs(cs, dry_run: bool) -> list[tuple[str, bool, str]]:
    """DROP procedures that have been superseded/renamed. Uses IF EXISTS so
    re-running is safe. Called before deploy_procs so if the drop fails for a
    real reason, we haven't yet re-created things."""
    results = []
    for stmt in LEGACY_DROPS:
        if dry_run:
            results.append((stmt, True, "DRY-RUN would execute"))
            continue
        try:
            cs.execute(stmt)
            results.append((stmt, True, "OK - DROP succeeded (or did not exist)"))
        except Exception as e:
            results.append((stmt, False, f"ERROR: {e}"))
    return results


def deploy_registry(cs, dry_run: bool) -> tuple[bool, str]:
    """Reload DCQ_CHECK_REGISTRY from the JSON file.

    Strategy: PUT the file to a temporary stage, TRUNCATE the table, COPY INTO
    using typed SELECT expressions built from INFORMATION_SCHEMA. Mirrors what
    push_snowflake_assets.py does for the registry table, extracted here.
    """
    path = BASE_DIR / REGISTRY_JSON
    if not path.exists():
        return False, f"file not found: {REGISTRY_JSON}"
    try:
        data = json.loads(path.read_text())
    except Exception as e:
        return False, f"JSON parse failed: {e}"
    if not isinstance(data, list) or not data:
        return False, "JSON is empty or not an array"
    row_count = len(data)

    if dry_run:
        return True, f"DRY-RUN would TRUNCATE and reload {row_count} rows into {REGISTRY_TABLE}"

    try:
        raw = cs.connection.cursor()
        raw.execute(f"USE DATABASE {DATABASE}")
        raw.execute(f"USE SCHEMA {DATABASE}.DCQ")

        # Get column metadata.
        raw.execute(
            f"SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE "
            f"FROM {DATABASE}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            f"ORDER BY ORDINAL_POSITION",
            ("DCQ", "DCQ_CHECK_REGISTRY"),
        )
        col_info = raw.fetchall()
        if not col_info:
            return False, f"table {REGISTRY_TABLE} not found"
        col_names = [c[0] for c in col_info]

        # Build typed SELECT expressions from JSON.
        select_parts = []
        for name, dt, prec, scale in col_info:
            dt_u = (dt or "").upper()
            if any(k in dt_u for k in ("NUMBER", "INT", "FLOAT", "DOUBLE", "DECIMAL")):
                if prec is not None and scale is not None:
                    select_parts.append(f"$1:{name}::NUMBER({prec},{scale})")
                else:
                    select_parts.append(f"$1:{name}::NUMBER")
            elif "BOOLEAN" in dt_u:
                select_parts.append(f"$1:{name}::BOOLEAN")
            elif "VARIANT" in dt_u:
                select_parts.append(f"$1:{name}::VARIANT")
            elif "ARRAY" in dt_u:
                select_parts.append(f"$1:{name}::ARRAY")
            else:
                select_parts.append(f"$1:{name}::VARCHAR")

        # Stage upload.
        raw.execute("CREATE OR REPLACE TEMPORARY STAGE registry_reload_stage")
        raw.execute(f"PUT file://{path} @registry_reload_stage/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        # Truncate + reload.
        raw.execute(f"TRUNCATE TABLE {REGISTRY_TABLE}")
        copy_sql = f"""
            COPY INTO {REGISTRY_TABLE} ({', '.join(col_names)})
            FROM (SELECT {', '.join(select_parts)}
                  FROM @registry_reload_stage/{path.name})
            FILE_FORMAT = (TYPE='JSON' STRIP_OUTER_ARRAY=TRUE COMPRESSION='NONE')
        """
        raw.execute(copy_sql)

        # Verify.
        raw.execute(f"SELECT COUNT(*) FROM {REGISTRY_TABLE}")
        actual = raw.fetchone()[0]
        raw.close()
        if actual != row_count:
            return False, f"row count mismatch: expected {row_count}, got {actual}"
        return True, f"OK - reloaded {row_count} rows into {REGISTRY_TABLE}"
    except Exception as e:
        return False, f"ERROR: {e}"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Print what would happen, do not touch Snowflake")
    parser.add_argument("--procs", action="store_true", help="Deploy procedures only (skip registry)")
    parser.add_argument("--registry", action="store_true", help="Reload registry only (skip procedures)")
    args = parser.parse_args()

    do_procs = args.procs or not args.registry
    do_registry = args.registry or not args.procs

    if not args.dry_run:
        check_env()

    print("=" * 70)
    print("SELECTIVE DEPLOY")
    print(f"  dry-run:      {args.dry_run}")
    print(f"  procs:        {do_procs}")
    print(f"  registry:     {do_registry}")
    print("=" * 70)

    if args.dry_run:
        cs = None
    else:
        conn = get_connection()
        cs = conn.cursor(DictCursor)

    all_ok = True

    if do_procs:
        print("\nDropping legacy procedures...")
        for stmt, ok, msg in drop_legacy_procs(cs, args.dry_run):
            marker = "  OK  " if ok else "  FAIL"
            # Truncate long DROP statements for display
            display = stmt if len(stmt) < 80 else stmt[:77] + "..."
            print(f"  [{marker}] {display}: {msg}")
            if not ok:
                all_ok = False

        print("\nDeploying procedures...")
        for rel, ok, msg in deploy_procs(cs, args.dry_run):
            marker = "  OK  " if ok else "  FAIL"
            print(f"  [{marker}] {rel}: {msg}")
            if not ok:
                all_ok = False

    if do_registry:
        print("\nDeploying registry...")
        ok, msg = deploy_registry(cs, args.dry_run)
        marker = "  OK  " if ok else "  FAIL"
        print(f"  [{marker}] {REGISTRY_JSON}: {msg}")
        if not ok:
            all_ok = False

    if cs is not None:
        cs.close()

    print("\n" + "=" * 70)
    if all_ok:
        print("RESULT: all deploy steps succeeded" + (" (dry-run)" if args.dry_run else ""))
        sys.exit(0)
    else:
        print("RESULT: one or more deploy steps FAILED - see messages above")
        print("       If a proc failed, the OLD version is still deployed.")
        print("       If registry failed, run 02_restore_if_broken.sql immediately.")
        sys.exit(1)


if __name__ == "__main__":
    main()
