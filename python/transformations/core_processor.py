from pathlib import Path

from python.snowflake_loader import get_snowflake_connection


PROJECT_ROOT = Path(__file__).resolve().parents[2]

EXPIRE_SQL_PATH = (
    PROJECT_ROOT / "sql" / "18_core_expire_current.sql"
)

INSERT_SQL_PATH = (
    PROJECT_ROOT / "sql" / "19_core_insert_versions.sql"
)


def read_sql_file(file_path):
    """Read SQL from a file."""

    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


def run_core_incremental():
    """
    Execute incremental SCD Type 2 processing.

    1. Expire existing current records.
    2. Insert new profile versions.
    3. Commit both operations together.
    4. Roll back if either operation fails.
    """

    connection = None
    cursor = None

    try:
        expire_sql = read_sql_file(EXPIRE_SQL_PATH)
        insert_sql = read_sql_file(INSERT_SQL_PATH)

        connection = get_snowflake_connection()
        connection.autocommit(False)

        cursor = connection.cursor()

        print("\nStarting CORE incremental processing")

        print("Step 1: Expiring existing current records...")
        cursor.execute(expire_sql)
        expired_count = cursor.rowcount

        print("Expired records:", expired_count)

        print("Step 2: Inserting new profile versions...")
        cursor.execute(insert_sql)
        inserted_count = cursor.rowcount

        print("Inserted records:", inserted_count)

        connection.commit()

        print("CORE incremental processing completed successfully.")

        return {
            "expired_count": expired_count,
            "inserted_count": inserted_count,
            "status": "SUCCESS"
        }

    except Exception:
        if connection:
            connection.rollback()

        print("CORE incremental processing failed. Transaction rolled back.")
        raise

    finally:
        if cursor:
            cursor.close()

        if connection:
            connection.close()