
import os
import re

import snowflake.connector

from python.utils.config_utils import (
    PROJECT_ROOT,
    get_snowflake_database,
    get_snowflake_schemas,
    get_fully_qualified_name,
    load_project_config,
)


# ==========================================================
# 1. CONFIGURATION
# ==========================================================

SQL_DIRECTORY = PROJECT_ROOT / "sql"

DATABASE_SQL_FILE = SQL_DIRECTORY / "01_database_setup.sql"

# Only DDL scripts are executed during provisioning.
# The order respects table dependencies.
DDL_FILES = [
    "02_create_raw_member_profiles.sql",
    "03_create_raw_redemptions.sql",
    "04_create_ingestion_audit.sql",
    "05_create_member_batch_control.sql",
    "06_create_rejected_member_records.sql",
    "06b_create_rejected_redemption_records.sql",
    "07_create_staging_member_profiles.sql",
    "08_staging_redemption_transactions.sql",
    "09_create_core_member_profile.sql",
    "10_create_core_redemption_transactions.sql",
    "11_create_country_member_tables.sql",
    "23_create_mart_dim_date.sql",
    "24_create_mart_dim_member.sql",
    "25_create_mart_fact_redemption.sql",
]


# ==========================================================
# 2. CONNECTION SETTINGS
# ==========================================================

def get_connection_settings():

    required_variables = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_ROLE",
    ]

    settings = {}

    for variable in required_variables:

        value = os.getenv(variable)

        if not value:
            raise ValueError(
                f"Missing environment variable: {variable}"
            )

        settings[variable] = value

    return settings


# ==========================================================
# 3. BUILD SQL PLACEHOLDER MAPPINGS
# ==========================================================

def build_sql_object_mapping(config):

    # ------------------------------------------------------
    # 3.1 SNOWFLAKE OBJECT PLACEHOLDERS
    # ------------------------------------------------------

    placeholder_mapping = {

        "__MEMBER_RAW_TABLE__": "member_raw",

        "__REDEMPTION_RAW_TABLE__": "redemption_raw",

        "__INGESTION_AUDIT_TABLE__": "ingestion_audit",

        "__MEMBER_BATCH_CONTROL_TABLE__": "member_batch_control",

        "__REJECTED_MEMBER_RECORDS_TABLE__": "rejected_member_records",

        "__REJECTED_REDEMPTION_TABLE__": "rejected_redemption_records",

        "__MEMBER_STAGING_TABLE__": "member_staging",

        "__REDEMPTION_STAGING_TABLE__": "redemption_staging",

        "__MEMBER_CORE_TABLE__": "member_core",

        "__REDEMPTION_CORE_TABLE__": "redemption_core",

        "__MEMBER_INDIA_TABLE__": "member_india",

        "__MEMBER_USA_TABLE__": "member_usa",

        "__MEMBER_PHILIPPINES_TABLE__": "member_philippines",

        "__MEMBER_CANADA_TABLE__": "member_canada",

        "__MEMBER_AUSTRALIA_TABLE__": "member_australia",

        "__DIM_DATE_TABLE__": "mart_dim_date",

        "__DIM_MEMBER_TABLE__": "mart_dim_member",

        "__FACT_REDEMPTION_TABLE__": "mart_fact_redemption",
    }

    resolved_mapping = {}

    # Resolve fully qualified Snowflake object names.
    for placeholder, object_key in placeholder_mapping.items():

        resolved_mapping[placeholder] = get_fully_qualified_name(
            object_key,
            config,
        )

    # ------------------------------------------------------
    # 3.2 COUNTRY CODE PLACEHOLDERS
    # ------------------------------------------------------

    snowflake_config = config.get("snowflake", {})

    country_codes = snowflake_config.get("country_codes", {})

    country_placeholder_mapping = {

        "__COUNTRY_INDIA_CODE__": "india",

        "__COUNTRY_USA_CODE__": "usa",

        "__COUNTRY_PHILIPPINES_CODE__": "philippines",

        "__COUNTRY_CANADA_CODE__": "canada",

        "__COUNTRY_AUSTRALIA_CODE__": "australia",
    }

    for placeholder, country_key in country_placeholder_mapping.items():

        country_code = country_codes.get(country_key)

        if not isinstance(country_code, str) or not country_code.strip():

            raise ValueError(
                f"Missing or invalid country code configuration: "
                f"{country_key}"
            )

        # Escape SQL single quotes and add SQL string delimiters.
        escaped_country_code = country_code.strip().replace("'", "''")

        resolved_mapping[placeholder] = (
            f"'{escaped_country_code}'"
        )

    return resolved_mapping


# ==========================================================
# 4. READ AND PREPARE SQL
# ==========================================================

def prepare_sql(sql_file, placeholder_mapping):

    if not sql_file.exists():

        raise FileNotFoundError(
            f"SQL file not found: {sql_file}"
        )

    sql = sql_file.read_text(encoding="utf-8")

    for placeholder, object_name in placeholder_mapping.items():

        sql = sql.replace(
            placeholder,
            object_name,
        )

    unresolved = re.findall(
        r"__[A-Z0-9_]+__",
        sql,
    )

    if unresolved:

        raise ValueError(
            f"Unresolved SQL placeholders in {sql_file.name}: "
            f"{sorted(set(unresolved))}"
        )

    return sql


# ==========================================================
# 5. EXECUTE SQL SCRIPT
# ==========================================================

def execute_sql_script(connection, sql, script_name):

    try:

        result_cursors = connection.execute_string(sql)

        for result_cursor in result_cursors:
            result_cursor.close()

        print(f"Successfully executed: {script_name}")

    except Exception as error:

        raise RuntimeError(
            f"Failed executing {script_name}: {error}"
        ) from error


# ==========================================================
# 6. PROVISION DATABASE, SCHEMAS AND TABLES
# ==========================================================

def provision_snowflake():

    config = load_project_config()

    database = get_snowflake_database()

    schemas = get_snowflake_schemas(config)

    settings = get_connection_settings()

    placeholder_mapping = build_sql_object_mapping(config)

    print("=" * 60)
    print("SKYPOINTS SNOWFLAKE PROVISIONING")
    print("=" * 60)

    print(f"Database: {database}")
    print(f"Configured schemas: {schemas}")

    connection = snowflake.connector.connect(
        account=settings["SNOWFLAKE_ACCOUNT"],
        user=settings["SNOWFLAKE_USER"],
        password=settings["SNOWFLAKE_PASSWORD"],
        warehouse=settings["SNOWFLAKE_WAREHOUSE"],
        role=settings["SNOWFLAKE_ROLE"],
    )

    cursor = connection.cursor()

    try:

        # --------------------------------------------------
        # STEP 1: CREATE DATABASE
        # --------------------------------------------------

        print("\nSTEP 1: Creating database")

        database_sql = prepare_sql(
            DATABASE_SQL_FILE,
            {"__DATABASE__": database},
        )

        execute_sql_script(
            connection,
            database_sql,
            DATABASE_SQL_FILE.name,
        )

        # --------------------------------------------------
        # STEP 2: CREATE SCHEMAS
        # --------------------------------------------------

        print("\nSTEP 2: Creating schemas")

        for schema in schemas:

            schema_sql = (
                f"CREATE SCHEMA IF NOT EXISTS "
                f"{database}.{schema}"
            )

            cursor.execute(schema_sql)

            print(f"Schema ready: {database}.{schema}")

        # --------------------------------------------------
        # STEP 3: CREATE CONFIGURED TABLES
        # --------------------------------------------------

        print("\nSTEP 3: Creating configured tables")

        for filename in DDL_FILES:

            sql_file = SQL_DIRECTORY / filename

            prepared_sql = prepare_sql(
                sql_file,
                placeholder_mapping,
            )

            execute_sql_script(
                connection,
                prepared_sql,
                filename,
            )

        print("\n" + "=" * 60)
        print("SNOWFLAKE PROVISIONING SUCCESSFUL")
        print("=" * 60)

        print(f"Database: {database}")
        print(f"Schemas created/verified: {len(schemas)}")
        print(f"DDL scripts executed: {len(DDL_FILES)}")

    finally:

        cursor.close()
        connection.close()


# ==========================================================
# 7. EXECUTION
# ==========================================================

if __name__ == "__main__":

    provision_snowflake()
