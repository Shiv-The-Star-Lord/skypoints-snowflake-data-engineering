# SkyPoints – Snowflake Data Engineering Project

A configuration-driven data engineering pipeline built with **Python and Snowflake** to ingest, validate, transform, and manage airline loyalty member profiles and redemption transactions from multiple source formats.

The project demonstrates incremental data ingestion, data quality validation, rejected-record handling, Slowly Changing Dimension Type 2 (SCD2), batch tracking, and Snowflake-based transformations.

## 1. Architecture

```text
Source Files
(CSV / XLSX / JSON / XML*)
        |
        v
Python Ingestion Orchestrator
        |
        +---- Source Configuration
        |     (source_config.json)
        |
        +---- Column Mapping
        |     (column_mapping.json)
        |
        v
File Reading & Standardization
        |
        v
Data Validation
        |
        +----------------------+
        |                      |
        v                      v
Valid Records           Rejected Records
        |                      |
        v                      v
Snowflake RAW           OPS Rejected Records
        |
        v
Snowflake STG
        |
        +----------------------+
        |                      |
        v                      v
Member Processing   Redemption Processing
        |                      |
        v                      v
CORE Member Profile   CORE Redemption Transactions
(SCD Type 2)           (MERGE)
        |
        v
Country-Specific Member Tables
(Separate SQL synchronization)
        |
        v
Data Quality Validation
```

*XML file reading is supported, but the XML source is currently disabled in the source configuration.*

## 2. Key Features

- **Multi-format ingestion:** Supports CSV, Excel (XLSX), JSON, and XML file formats.
- **Configuration-driven processing:** Source definitions, enabled status, file formats, default country codes, and column mappings are maintained in JSON configuration files.
- **Header standardization:** Maps different source column names to a common member profile structure.
- **Data validation:** Checks mandatory fields, duplicate member IDs, date values, and other record-level validation rules.
- **Identity conflict validation:** Checks member identity consistency within incoming files and against historical member records in CORE.
- **Rejected-record handling:** Invalid and conflicting records are quarantined in Snowflake OPS tables.
- **Incremental ingestion:** Uses file checksums and batch tracking to support controlled ingestion.
- **SCD Type 2:** Maintains historical member profile versions in the CORE member dimension.
- **Redemption processing:** Handles nested JSON transactions and merges records into STG and CORE.
- **Data quality checks:** Includes SQL-based checks for duplicates, invalid values, SCD2 consistency, and country projection consistency.
- **Automated tests:** Includes pytest coverage for ingestion, validation, orchestration, and transformation logic.

## 3. Technology Stack

| Component | Technology |
|---|---|
| Programming language | Python |
| Data processing | Pandas |
| Data warehouse | Snowflake |
| Snowflake connectivity | Snowflake Python Connector |
| Configuration | JSON |
| Environment variables | python-dotenv |
| Excel support | openpyxl |
| Testing | pytest |
| SQL | Snowflake SQL |

## 4. Project Structure

```text
snowflake-de-project/
│
├── config/
│   ├── source_config.json
│   └── column_mapping.json
│
├── data/
│   ├── sample/
│   ├── raw/
│   └── test/
│
├── docs/
│   └── performance_and_scalability.md
│
├── python/
│   ├── ingestion/
│   │   ├── data_validator.py
│   │   ├── file_reader.py
│   │   ├── header_mapper.py
│   │   ├── ingestion_orchestrator.py
│   │   └── ingestion_pipeline.py
│   │
│   ├── transformations/
│   │   ├── core_processor.py
│   │   ├── member_processor.py
│   │   └── redemption_processor.py
│   │
│   ├── utils/
│   │   └── file_utils.py
│   │
│   └── snowflake_loader.py
│
├── sql/
│   ├── 01_database_setup.sql
│   ├── 02_create_raw_member_profiles.sql
│   ├── 03_create_raw_redemptions.sql
│   ├── 04_create_ingestion_audit.sql
│   ├── 05_create_member_batch_control.sql
│   ├── 06_create_rejected_member_records.sql
│   ├── 07_create_rejected_redemption_records.sql
│   ├── 08_create_staging_member_profiles.sql
│   ├── 09_staging_redemption_transactions.sql
│   ├── 10_create_core_member_profile.sql
│   ├── 11_create_core_redemption_transactions.sql
│   ├── 12_create_country_member_tables.sql
│   ├── 13_create_mart_dim_date.sql
│   ├── 14_create_mart_dim_member.sql
│   ├── 15_create_mart_fact_redemption.sql
│   ├── 16_initialize_member_batch_control.sql
│   ├── 17_incremental_staging_member_profiles.sql
│   ├── 18_core_expire_current.sql
│   ├── 19_core_insert_versions.sql
│   ├── 20_quarantine_redemption_transactions.sql
│   ├── 21_load_staging_redemption_transactions.sql
│   ├── 22_merge_core_redemption_transactions.sql
│   ├── 23_sync_country_member_tables.sql
│   ├── 24_validate_redemption_member_reference.sql
│   ├── 25_data_quality_checks.sql
│   ├── 26_legacy_core_member_profile_scd2.sql
│   └── 27_legacy_core_member_profile_incremental.sql
├── tests/
│   ├── test_core_processor.py
│   ├── test_data_validator.py
│   ├── test_ingestion.py
│   ├── test_ingestion_orchestrator.py
│   ├── test_ingestion_pipeline.py
│   ├── test_member_processor.py
│   └── test_redemption_processor.py
│
├── .gitignore
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

## 5. Snowflake Database Design

The project uses the `SKYPOINTS_DE` database with the following schemas:

| Schema | Purpose |
|---|---|
| RAW | Stores ingested source records |
| STG | Holds standardized and prepared records |
| CORE | Stores transformed business entities and historical member records |
| MART | Reserved for analytical and reporting structures |
| OPS | Stores operational metadata, batch tracking, audit, and rejected records |

### Member Profile Processing

Member profiles are processed through RAW, STG, and CORE.

The CORE member profile dimension uses **SCD Type 2** to preserve historical changes.

The incremental processing flow:

1. Register and track the ingestion batch.
2. Load eligible source records into RAW.
3. Insert new records into STG.
4. Expire previous current CORE records when changes are detected.
5. Insert new CORE versions.
6. Update batch processing status.

### Redemption Transaction Processing

Redemption transactions are ingested from nested JSON.

The processing flow:

1. Load redemption records into RAW.
2. Merge RAW records into STG.
3. Merge STG records into CORE.
4. Validate redemption-to-member references.

Redemption records are maintained using MERGE-based processing rather than member-profile SCD Type 2.

### Country-Specific Tables

The project includes country-specific member tables for:

- India (`IND`)
- USA (`USA`)
- Philippines (`PHIL`)
- Canada (`CAN`)
- Australia (`AU`)

These tables are created and synchronized using separate SQL scripts. Country synchronization is not automatically executed by the standard Python orchestrator.

## 6. Source Configuration

Source definitions are maintained in:

`config/source_config.json`

Currently enabled sources:

| Source ID | File | Format | Default Country |
|---|---|---|---|
| IND_MEMBERS | IND.csv | CSV | IND |
| USA_MEMBERS | USA.xlsx | XLSX | USA |
| AUS_MEMBERS | AUS.xlsx | XLSX | AU |
| REDEMPTION_TRANSACTIONS | redemption_transactions.json | JSON | — |

The XML member source is configured but disabled.

To enable or disable a source, update its `enabled` property in `source_config.json`.

By default, source files are read from:

`data/sample/`

Column mappings are maintained separately in:

`config/column_mapping.json`

This allows source headers to be standardized without changing the ingestion logic.

## 7. Prerequisites

- Python 3.12
- Snowflake account
- Snowflake user with the required database, schema, table, and warehouse permissions
- Snowflake warehouse
- Git

## 8. Installation and Setup

### Step 1: Clone the repository

```bash
git clone <repository-url>
cd snowflake-de-project
```

### Step 2: Create a virtual environment

```bash
python3 -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

### Step 3: Install dependencies

Install application dependencies:

```bash
pip install -r requirements.txt
```

Install development and testing dependencies:

```bash
pip install -r requirements-dev.txt
```

### Step 4: Configure Snowflake credentials

Create a `.env` file in the project root.

```env
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password

SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=SKYPOINTS_DE
SNOWFLAKE_SCHEMA=OPS
```

The account, user, and password are required by the connection loader. Role, warehouse, database, and schema are optional connection settings, depending on your Snowflake environment.

**Security:** Never commit `.env` or expose Snowflake credentials in source code or logs.

### Step 5: Prepare Snowflake objects

Execute the relevant SQL setup scripts in your Snowflake environment.

Start with:

- `01_database_setup.sql` – creates the database and schemas.
- `09_staging_redemption_transactions.sql` – creates the redemption staging table.
- `11_create_core_redemption_transactions.sql` – creates the CORE redemption table.
- `05_create_member_batch_control.sql` – creates the member batch control table.
- `06_create_rejected_member_records.sql` – creates the rejected member records table.
- `12_create_country_member_tables.sql` – creates country-specific member tables when required.

Ensure the required member RAW, STG, CORE, and ingestion audit tables are also present before running the Python pipeline.

Some SQL scripts are intended for initial loads, initialization, standalone processing, or manual validation. They are not all meant to be executed blindly in numerical order. The standard Python workflow invokes its own incremental processing SQL scripts.

## 9. Running the Pipeline

From the repository root, with the virtual environment activated and Snowflake configuration in place, execute:

```bash
python -m python.ingestion.ingestion_orchestrator
```

The orchestrator:

1. Loads the source configuration.
2. Identifies enabled sources.
3. Reads files from `data/sample/`.
4. Standardizes source columns.
5. Validates member and redemption records.
6. Quarantines rejected records.
7. Loads eligible records into Snowflake RAW.
8. Runs member and redemption transformations.
9. Updates member batch tracking.

The member processing workflow uses incremental staging and CORE SCD2 SQL scripts.

The redemption processing workflow executes the RAW-to-STG and STG-to-CORE MERGE operations.

## 10. SQL Script Reference

| Script | Purpose |
|---|---|
| 01 | Database and schema setup |
| 02 | Member staging setup and standalone/full-load processing |
| 03 | Initial CORE member SCD2 load |
| 04 | Standalone combined incremental member SCD2 processing |
| 04a | Expire current CORE member versions |
| 04b | Insert new CORE member versions |
| 05 | Create redemption staging table |
| 06 | Merge RAW redemption records into STG |
| 07 | Create CORE redemption table |
| 08 | Merge STG redemption records into CORE |
| 09 | Validate redemption member references |
| 10 | Create country-specific member tables |
| 11 | Synchronize country-specific member tables |
| 12 | Execute data quality checks |
| 13 | Create member batch control table |
| 14 | Initialize batch control from existing successful audit records |
| 15 | Incremental member staging load |
| 16 | Create rejected member records table |

The normal Python processing path uses scripts `15`, `04a`, and `04b` for incremental member processing, and scripts `06` and `08` for redemption processing.

Scripts for initial loads, initialization, country synchronization, and manual validation should be run when appropriate rather than as part of every pipeline execution.

## 11. Data Quality and Validation

The project includes Python-level and Snowflake SQL-level validation.

Python validation covers:

- Mandatory member fields.
- Duplicate member IDs.
- Date parsing and validation.
- Member identity conflicts.
- Redemption record validation.
- Rejected-record separation and quarantine.

SQL data quality checks include:

- Duplicate member records.
- Missing member names.
- Invalid country codes.
- Invalid age values.
- SCD2 date-range validation.
- SCD2 overlap checks.
- Duplicate redemption business keys.
- Invalid redemption values.
- Country-specific table count and consistency checks.

Run the SQL validation scripts as required:

- `24_validate_redemption_member_reference.sql`
- `25_data_quality_checks.sql`

## 12. Running Tests

Run the test suite from the project root:

```bash
pytest -v
```

The tests cover:

- File ingestion.
- Header standardization.
- Data validation.
- Ingestion pipeline behavior.
- Orchestration.
- Member processing.
- Redemption processing.
- CORE processing.

## 13. Performance and Scalability

Performance observations, current Snowflake development environment details, SQL execution timings, scaling considerations, and future benchmarking recommendations are documented separately.

See:

`docs/performance_and_scalability.md`

The documented SQL statement timings represent measured statement execution durations and should not be interpreted as total end-to-end pipeline runtime.

## 14. Future Enhancements

Potential areas for further development include:

- Automated scheduling and orchestration.
- Improved monitoring and alerting.
- Production-scale performance benchmarking.
- Enhanced retry and recovery handling.
- Additional analytical models in the MART schema.
- Expanded automated data quality reporting.

## 15. Project Objective

This project demonstrates an end-to-end Snowflake data engineering workflow covering multi-source ingestion, Python-based validation, incremental processing, SCD Type 2 history management, transaction MERGE operations, operational tracking, rejected-record handling, and automated testing.