
from pathlib import Path

from python.snowflake_loader import get_snowflake_connection


PROJECT_ROOT = Path(__file__).resolve().parents[2]

STG_SQL_PATH = (
    PROJECT_ROOT / "sql" / "06_load_staging_redemption_transactions.sql"
)

CORE_SQL_PATH = (
    PROJECT_ROOT / "sql" / "08_merge_core_redemption_transactions.sql"
)


def read_sql_file(file_path):
    """Read SQL from a file."""
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


def run_redemption_processing():
    """
    Process redemption transactions from RAW to STG and STG to CORE.

    Both MERGE operations are committed together.
    If either operation fails, both are rolled back.
    """

    connection = None
    cursor = None

    try:
        stg_sql = read_sql_file(STG_SQL_PATH)
        core_sql = read_sql_file(CORE_SQL_PATH)

        connection = get_snowflake_connection()
        connection.autocommit(False)

        cursor = connection.cursor()

        # Set Snowflake execution context.
        cursor.execute("USE DATABASE SKYPOINTS_DE")
        cursor.execute("USE SCHEMA STG")

        print("\nStarting redemption transaction processing")

        print("Step 1: Merging RAW transactions into STG...")
        cursor.execute(stg_sql)
        stg_merge_results = cursor.fetchone()

        print("STG MERGE results:", stg_merge_results)

        # Switch schema for CORE processing.
        cursor.execute("USE SCHEMA CORE")

        print("Step 2: Merging STG transactions into CORE...")
        cursor.execute(core_sql)
        core_merge_results = cursor.fetchone()

        print("CORE MERGE results:", core_merge_results)

        connection.commit()

        print("Redemption processing completed successfully.")

        return {
            "stg_merge_results": stg_merge_results,
            "core_merge_results": core_merge_results,
            "status": "SUCCESS"
        }

    except Exception:
        if connection:
            connection.rollback()

        print("Redemption processing failed. Transaction rolled back.")
        raise

    finally:
        if cursor:
            cursor.close()

        if connection:
            connection.close()
