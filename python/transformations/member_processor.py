
import re
from pathlib import Path

from python.snowflake_loader import get_snowflake_connection


PROJECT_ROOT = Path(__file__).resolve().parents[2]

STG_SQL_PATH = (
    PROJECT_ROOT / "sql" / "02_staging_member_profiles.sql"
)

EXPIRE_SQL_PATH = (
    PROJECT_ROOT / "sql" / "04a_core_expire_current.sql"
)

INSERT_CORE_SQL_PATH = (
    PROJECT_ROOT / "sql" / "04b_core_insert_versions.sql"
)


def read_sql_file(file_path):
    """Read SQL from a file."""

    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


def extract_staging_insert(sql_text):
    """
    Extract only the STG INSERT statement.

    Excludes CREATE TABLE and validation SELECT statements.
    """

    match = re.search(
        r"INSERT\s+INTO\s+SKYPOINTS_DE\.STG\.STG_MEMBER_PROFILES"
        r"\s*\(.*?;",
        sql_text,
        flags=re.IGNORECASE | re.DOTALL
    )

    if not match:
        raise ValueError(
            "STG member INSERT statement not found in SQL file."
        )

    return match.group(0)


def run_member_processing():
    """
    Process member profiles from RAW to STG and STG to CORE.

    All three DML operations run in one transaction:
    1. Insert new profile versions into STG.
    2. Expire existing current CORE records.
    3. Insert new CORE SCD Type 2 versions.
    """

    connection = None
    cursor = None

    try:
        staging_file = read_sql_file(STG_SQL_PATH)
        stg_sql = extract_staging_insert(staging_file)

        expire_sql = read_sql_file(EXPIRE_SQL_PATH)
        core_insert_sql = read_sql_file(INSERT_CORE_SQL_PATH)

        connection = get_snowflake_connection()
        connection.autocommit(False)

        cursor = connection.cursor()

        cursor.execute("USE DATABASE SKYPOINTS_DE")
        cursor.execute("USE SCHEMA STG")

        print("\nStarting member profile processing")

        print("Step 1: Loading RAW member profiles into STG...")
        cursor.execute(stg_sql)
        stg_inserted_count = cursor.rowcount

        print("STG records inserted:", stg_inserted_count)

        cursor.execute("USE SCHEMA CORE")

        print("Step 2: Expiring existing CORE records...")
        cursor.execute(expire_sql)
        expired_count = cursor.rowcount

        print("CORE records expired:", expired_count)

        print("Step 3: Inserting new CORE profile versions...")
        cursor.execute(core_insert_sql)
        core_inserted_count = cursor.rowcount

        print("CORE records inserted:", core_inserted_count)

        connection.commit()

        print("Member profile processing completed successfully.")

        return {
            "stg_inserted_count": stg_inserted_count,
            "expired_count": expired_count,
            "core_inserted_count": core_inserted_count,
            "status": "SUCCESS"
        }

    except Exception:
        if connection:
            connection.rollback()

        print("Member processing failed. Transaction rolled back.")
        raise

    finally:
        if cursor:
            cursor.close()

        if connection:
            connection.close()
