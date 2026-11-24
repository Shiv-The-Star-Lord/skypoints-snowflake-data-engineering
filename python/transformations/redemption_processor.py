
from pathlib import Path

from python.snowflake_loader import get_snowflake_connection
from python.utils.config_utils import (
    get_fully_qualified_name,
    get_snowflake_object,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

QUARANTINE_SQL_PATH = (
    PROJECT_ROOT
    / "sql"
    / "18a_quarantine_redemption_transactions.sql"
)

STG_SQL_PATH = (
    PROJECT_ROOT
    / "sql"
    / "18_load_staging_redemption_transactions.sql"
)

CORE_SQL_PATH = (
    PROJECT_ROOT
    / "sql"
    / "19_merge_core_redemption_transactions.sql"
)


def read_sql_file(file_path):
    """Read a SQL file using UTF-8 encoding."""
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


def replace_sql_placeholders(sql, mapping):
    """Replace SQL placeholders using configuration-driven object names."""
    for placeholder, object_name in mapping.items():
        sql = sql.replace(placeholder, object_name)

    unresolved = [
        placeholder
        for placeholder in (
            "__REJECTED_REDEMPTION_TABLE__",
            "__REDEMPTION_RAW_TABLE__",
            "__MEMBER_CORE_TABLE__",
            "__INGESTION_AUDIT_TABLE__",
            "__REDEMPTION_STAGING_TABLE__",
            "__REDEMPTION_CORE_TABLE__",
        )
        if placeholder in sql
    ]

    if unresolved:
        raise ValueError(
            f"Unresolved SQL placeholders: {unresolved}"
        )

    return sql


def run_redemption_processing():

    connection = None
    cursor = None

    try:
        # Resolve Snowflake objects from configuration.
        object_mapping = {
            "__REJECTED_REDEMPTION_TABLE__":
                get_fully_qualified_name("rejected_redemption_records"),

            "__REDEMPTION_RAW_TABLE__":
                get_fully_qualified_name("redemption_raw"),

            "__MEMBER_CORE_TABLE__":
                get_fully_qualified_name("member_core"),

            "__INGESTION_AUDIT_TABLE__":
                get_fully_qualified_name("ingestion_audit"),

            "__REDEMPTION_STAGING_TABLE__":
                get_fully_qualified_name("redemption_staging"),

            "__REDEMPTION_CORE_TABLE__":
                get_fully_qualified_name("redemption_core"),
        }

        staging_config = get_snowflake_object(
            "redemption_staging"
        )

        core_config = get_snowflake_object(
            "redemption_core"
        )

        database = staging_config["database"]
        staging_schema = staging_config["schema"]
        core_schema = core_config["schema"]

        # Load and prepare SQL templates.
        quarantine_sql = replace_sql_placeholders(
            read_sql_file(QUARANTINE_SQL_PATH),
            object_mapping
        )

        stg_sql = replace_sql_placeholders(
            read_sql_file(STG_SQL_PATH),
            object_mapping
        )

        core_sql = replace_sql_placeholders(
            read_sql_file(CORE_SQL_PATH),
            object_mapping
        )

        # Establish Snowflake connection.
        connection = get_snowflake_connection()
        connection.autocommit(False)

        cursor = connection.cursor()

        # Set database and STG schema.
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {staging_schema}")

        print("\nStarting redemption transaction processing")

        # Step 1: Quarantine invalid RAW transactions.
        print("Step 1: Identifying and quarantining invalid transactions...")

        cursor.execute(quarantine_sql)

        quarantine_merge_results = cursor.fetchone()

        print(
            "Quarantine MERGE results:",
            quarantine_merge_results
        )

        # Step 2: Load valid RAW transactions into STG.
        print("Step 2: Merging valid RAW transactions into STG...")

        cursor.execute(stg_sql)

        stg_merge_results = cursor.fetchone()

        print("STG MERGE results:", stg_merge_results)

        # Step 3: Load STG transactions into CORE.
        cursor.execute(f"USE SCHEMA {core_schema}")

        print("Step 3: Merging STG transactions into CORE...")

        cursor.execute(core_sql)

        core_merge_results = cursor.fetchone()

        print("CORE MERGE results:", core_merge_results)

        # Commit all processing operations together.
        connection.commit()

        print("Redemption processing completed successfully.")

        return {
            "quarantine_merge_results": quarantine_merge_results,
            "stg_merge_results": stg_merge_results,
            "core_merge_results": core_merge_results,
            "status": "SUCCESS",
        }

    except Exception:

        if connection:
            connection.rollback()

        print(
            "Redemption processing failed. "
            "Transaction rolled back."
        )

        raise

    finally:

        if cursor:
            cursor.close()

        if connection:
            connection.close()
