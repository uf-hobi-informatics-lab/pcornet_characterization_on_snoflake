# Characterization Snowflake Repository

This project reimplements PCORnet's Empirical Data Curation (EDC) Data Quality Characterization (DCQ) checks, originally written in SAS, as native Snowflake stored procedures. By porting the logic to Snowflake, the characterization process runs directly against the data warehouse — eliminating the need for data extraction and leveraging Snowflake's scalability to execute checks in parallel. The system is deployed as a Streamlit-in-Snowflake application that orchestrates up to 48 DCQ checks with configurable concurrency, providing real-time progress monitoring and centralized result storage.

New checks can be added by creating a stored procedure that follows the existing signature convention and registering it in the DCQ_CHECK_REGISTRY table — no changes to the application code are required. This design makes it straightforward to extend the system as PCORnet's EDC specifications evolve or as site-specific quality checks are needed.

---

## 📁 Repository Layout

```
characterization_on_snowflake/
│
├─ .env                     # Snowflake connection details (do **not** commit secrets)
│
├─ streamlit/
│   └─ app.py               # Streamlit UI (you can run it locally)
│
├─ sql/
│   ├─ dcq_checks/
│   │   └─ procedures/       # All 49 DCQ‑check stored procedures (SP_DC_*)
│   │       └─ *.sql
│   ├─ dcq/
│   │   └─ procedures/       # Driver procedure `sp_run_dcq.sql`
│   │       └─ sp_run_dcq.sql
│   └─ edc_ref/
│       ├─ tables/           # CREATE TABLE statements for each EDC_REF table
│       │   └─ *.sql
│       └─ views/            # CREATE VIEW statements for the three EDC_REF views
│           └─ *.sql
│
├─ snowflake/
│   ├─ DCQ_CHECK_REGISTRY.sql            # CREATE TABLE DDL for the registry
│   ├─ DCQ_CHECK_REGISTRY_data_full.json # Full data export (48 rows)
│   ├─ DCQ_CHECK_REGISTRY_sample.json    # Sample export (first 5 rows)
│   └─ edc_ref/                          # JSON data dumps for every EDC_REF table
│       └─ *_data.json
│
└─ README.md                 # **THIS** file
```

## 🔐 .env File

Create a `.env` file at the repository root (it is already included in the repo but **remove any secret values** before committing). It should contain the following variables:
```
SNOWFLAKE_ACCOUNT=<account_id>
SNOWFLAKE_USER=<username>
SNOWFLAKE_PRIVATE_KEY_PATH=<path_to_private_key.p8>
SNOWFLAKE_WAREHOUSE=<warehouse_name>
SNOWFLAKE_DATABASE=CHARACTERIZATION
SNOWFLAKE_SCHEMA=DCQ_CHECKS   # default schema used by the sync script
SNOWFLAKE_ROLE=1FL-AZURE-DB-USERS
```

The following optional variables configure Streamlit app deployment (used by `push_snowflake_assets.py`):
```
STREAMLIT_DATABASE=STREAMLIT          # Database where the Streamlit app lives
STREAMLIT_SCHEMA=PUBLIC               # Schema for the Streamlit app
STREAMLIT_NAME=characterization_runner # Name of the Streamlit app object
STREAMLIT_WAREHOUSE=STREAMLIT_XS      # Warehouse the Streamlit app queries with
STREAMLIT_STAGE=streamlit_deploy_stage # Internal stage used to upload app.py
```
If omitted, the defaults shown above are used.
These are used by the helper scripts to connect to Snowflake.

## 📦 Loading the Snowflake Objects

### 1️⃣ Create Tables & Views

```sql
-- Run the CREATE statements for tables
@/sql/edc_ref/tables/*.sql;

-- Run the CREATE statements for views
@/sql/edc_ref/views/*.sql;

-- Create the DCQ_CHECK_REGISTRY table
@/snowflake/DCQ_CHECK_REGISTRY.sql;
```
*(You can feed the `.sql` files to SnowSQL or any Snowflake client.)*

### 2️⃣ Load Table Data (JSON)

```sql
-- First, upload the JSON files to a stage (e.g., @~/edc_ref_stage)
PUT file://<repo_root>/snowflake/edc_ref/*.json @~/edc_ref_stage AUTO_COMPRESS=TRUE;

-- Then copy the data into each table, for example:
COPY INTO CHARACTERIZATION.EDC_REF.CDM_PARSEABLE_RAW
FROM (SELECT $1 FROM @~/edc_ref_stage/CDM_PARSEABLE_RAW_data.json.gz (FILE_FORMAT => (TYPE=>'JSON')));

-- Repeat the COPY statement for every table in the `edc_ref` folder.
```

### 3️⃣ Load Registry Data

```sql
PUT file://<repo_root>/snowflake/DCQ_CHECK_REGISTRY_data_full.json @~/registry_stage AUTO_COMPRESS=TRUE;
COPY INTO CHARACTERIZATION.DCQ.DCQ_CHECK_REGISTRY
FROM (SELECT $1:CHECK_ID::VARCHAR,
             $1:CHECK_NAME::VARCHAR,
             $1:ROW_NUM::NUMBER(10,2),
             $1:ROW_NUM_STR::VARCHAR,
             $1:PART::VARCHAR,
             $1:EDC_TABLE::VARCHAR,
             $1:DESCRIPTION::VARCHAR,
             $1:PROC_NAME::VARCHAR,
             $1:SOURCE_TABLES::ARRAY,
             $1:ENABLED::BOOLEAN,
             $1:ROW_NUM_CANON::VARCHAR,
             $1:DOC_SUMMARY::VARCHAR,
             $1:DOC_RUN::VARCHAR,
             $1:DOC_OUTPUT::VARCHAR,
             $1:DOC_INTERPRETATION::VARCHAR,
             $1:METRICS::ARRAY,
             $1:EXCEPTION_METRICS::ARRAY,
             $1:THRESHOLDS::VARIANT)
FROM @~/registry_stage/DCQ_CHECK_REGISTRY_data_full.json.gz (FILE_FORMAT => (TYPE=>'JSON'));
```

### 4️⃣ Deploy Stored Procedures

All procedure definitions are stored as `.sql` files under `sql/`. Load them with a simple loop, e.g.:
```bash
snowsql -c my_conn -f sql/dcq_checks/procedures/SP_DC_1_01.sql
# …repeat for each file or use a shell loop:
for f in sql/dcq_checks/procedures/*.sql; do snowsql -c my_conn -f $f; done
snowsql -c my_conn -f sql/dcq/procedures/sp_run_dcq.sql
```
You can also run the provided helper script `sync_snowflake_procs.py` to sync all procedures automatically.

## 📊 Running the Streamlit Dashboard

```bash
# Install dependencies (if not already installed)
pip install streamlit

# From the repository root
streamlit run streamlit/app.py
```
The app will read the Snowflake registry table (or any other tables you expose) and present a simple UI.

---

## 📦 Helper Scripts (Optional)

| Script | Purpose |
|--------|---------|
| `sync_snowflake_procs.py` | Pulls **all** stored procedures from the configured schema and writes them under `sql/`. |
| `fetch_sp_run_dcq.py` | Retrieves the driver procedure `SP_RUN_DCQ`. |
| `fetch_potential_code_errors.py` | Retrieves the `POTENTIAL_CODE_ERRORS` procedure. |
| `fetch_edc_ref_views.py` | Exports the three EDC_REF views. |
| `fetch_edc_ref_tables.py` (included in the repo) | Exports each EDC_REF table’s DDL and data as JSON. |
| `push_snowflake_assets.py` | Pushes all SQL assets (procedures, tables, views) **and** the Streamlit app to Snowflake. |
| `rebuild_characterization.py` | Rebuilds the entire CHARACTERIZATION database (creates DB, schemas, runs all DDL, loads data, and deploys procedures). |

All scripts rely on the `.env` file for credentials.

---

## 🛠️ Rebuild the CHARACTERIZATION Database

The repository includes a **single‑command** script to recreate the whole Snowflake environment:

```bash
# Install required Python packages (if not already installed)
pip install -r requirements.txt   # or install snowflake‑connector‑python, python‑dotenv, cryptography

# Run the rebuild script
python rebuild_characterization.py
```

The script performs the following steps automatically:
1. Creates the `CHARACTERIZATION` database (if it does not exist).
2. Creates the `DCQ`, `DCQ_CHECKS`, and `EDC_REF` schemas.
3. Executes all DDL files for tables and views stored under `sql/edc_ref/`.
4. Creates the `DCQ_CHECK_REGISTRY` table.
5. Loads JSON data for every `EDC_REF` table and the registry table.
6. Deploys all stored procedures from `sql/dcq_checks/procedures/` and the driver procedure `sql/dcq/procedures/sp_run_dcq.sql`.

> **Note:** The script uses the `.env` file for connection details. Ensure that the file contains valid Snowflake credentials and **do not** commit it to version control.

---

## 🚀 Push Local Changes to Snowflake

To deploy local code changes (SQL procedures, tables, views, **and** the Streamlit app) without a full rebuild:

```bash
python push_snowflake_assets.py
```

The script performs the following steps:
1. Executes all `.sql` files under `sql/` (procedures, tables, and views) against the Snowflake instance.
2. Uploads `streamlit/app.py` to an internal Snowflake stage.
3. Runs `CREATE OR REPLACE STREAMLIT` to update the Snowflake Streamlit app with the latest code.

This is the recommended way to push incremental changes during development. The Streamlit deployment settings can be customized via the optional `.env` variables described above (`STREAMLIT_DATABASE`, `STREAMLIT_SCHEMA`, etc.).

---

## 📑 License & Contributions

Feel free to fork, modify, and submit pull requests. Do **not** commit the `.env` file with real credentials.

---

*Happy coding!*