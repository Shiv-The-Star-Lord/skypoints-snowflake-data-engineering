
import re
from pathlib import Path

from python.snowflake_loader import get_snowflake_connection


PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Retained for compatibility with existing tests.
STG_SQL_PATH = (
    PROJECT_ROOT / "sql" / "02_staging_member_profiles.sql"
)

INCREMENTAL_STG_SQL_PATH = (
    PROJECT_ROOT / "sql" / "15_incremental_staging_member_profiles.sql"
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
    """Extract the STG INSERT statement from a SQL file."""

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
    Incrementally process successful member RAW batches.

    1. Register successful RAW batches.
    2. Process eligible batches into STG.
    3. Expire existing CORE versions.
    4. Insert new CORE SCD2 versions.
    5. Update batch processing statuses.

    Batch registration is committed separately.
    STG, CORE and their status updates share one transaction.
    """

    connection = None
    cursor = None
    registration_committed = False

    try:
        # --------------------------------------------------
        # STEP 0: LOAD SQL FILES
        # --------------------------------------------------

        incremental_stg_sql = extract_staging_insert(
            read_sql_file(INCREMENTAL_STG_SQL_PATH)
        )

        expire_sql = read_sql_file(EXPIRE_SQL_PATH)
        core_insert_sql = read_sql_file(INSERT_CORE_SQL_PATH)

        connection = get_snowflake_connection()
        connection.autocommit(False)

        cursor = connection.cursor()

        cursor.execute("USE DATABASE SKYPOINTS_DE")
        cursor.execute("USE SCHEMA OPS")

        print("\nStarting incremental member processing")

        # --------------------------------------------------
        # STEP 1: REGISTER SUCCESSFUL RAW BATCHES
        # --------------------------------------------------

        print("\nStep 1: Registering successful RAW batches...")

        register_sql = """
            INSERT INTO SKYPOINTS_DE.OPS.MEMBER_BATCH_CONTROL
            (
                BATCH_ID,
                SOURCE_ID,
                SOURCE_FILE,
                STG_STATUS,
                CORE_STATUS,
                CREATED_AT,
                UPDATED_AT
            )
            SELECT
                A.BATCH_ID,
                A.SOURCE_ID,
                A.SOURCE_FILE,
                'PENDING',
                'PENDING',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            FROM SKYPOINTS_DE.OPS.INGESTION_AUDIT A
            WHERE A.ENTITY_TYPE = 'MEMBER_PROFILE'
              AND A.STATUS = 'SUCCESS'
              AND NOT EXISTS
              (
                  SELECT 1
                  FROM SKYPOINTS_DE.OPS.MEMBER_BATCH_CONTROL C
                  WHERE C.BATCH_ID = A.BATCH_ID
              )
        """

        cursor.execute(register_sql)
        registered_count = cursor.rowcount

        # Preserve registration if later processing fails.
        connection.commit()
        registration_committed = True

        print("New batches registered:", registered_count)

        # --------------------------------------------------
        # STEP 2: PROCESS PENDING BATCHES INTO STG
        # --------------------------------------------------

        print("\nStep 2: Processing pending batches into STG...")

        # Mark only batches with successful RAW ingestion.
        cursor.execute("""
            UPDATE SKYPOINTS_DE.OPS.MEMBER_BATCH_CONTROL C
            SET
                STG_STARTED_AT = CURRENT_TIMESTAMP(),
                STG_ERROR_MESSAGE = NULL,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE C.STG_STATUS IN ('PENDING', 'FAILED')
              AND EXISTS
              (
                  SELECT 1
                  FROM SKYPOINTS_DE.OPS.INGESTION_AUDIT A
                  WHERE A.BATCH_ID = C.BATCH_ID
                    AND A.ENTITY_TYPE = 'MEMBER_PROFILE'
                    AND A.STATUS = 'SUCCESS'
              )
        """)

        # Execute only the INSERT statement, not USE commands.
        cursor.execute(incremental_stg_sql)
        stg_inserted_count = cursor.rowcount

        print("STG records inserted:", stg_inserted_count)

        # Mark eligible batches successful, including batches
        # that produced zero changed STG records.
        cursor.execute("""
            UPDATE SKYPOINTS_DE.OPS.MEMBER_BATCH_CONTROL C
            SET
                STG_STATUS = 'SUCCESS',
                STG_COMPLETED_AT = CURRENT_TIMESTAMP(),
                STG_ERROR_MESSAGE = NULL,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE C.STG_STATUS IN ('PENDING', 'FAILED')
              AND EXISTS
              (
                  SELECT 1
                  FROM SKYPOINTS_DE.OPS.INGESTION_AUDIT A
                  WHERE A.BATCH_ID = C.BATCH_ID
                    AND A.ENTITY_TYPE = 'MEMBER_PROFILE'
                    AND A.STATUS = 'SUCCESS'
              )
        """)

        # --------------------------------------------------
        # STEP 3: EXPIRE EXISTING CORE VERSIONS
        # --------------------------------------------------

        cursor.execute("USE SCHEMA CORE")

        print("\nStep 3: Expiring existing CORE records...")

        cursor.execute("""
            UPDATE SKYPOINTS_DE.OPS.MEMBER_BATCH_CONTROL C
            SET
                CORE_STARTED_AT = CURRENT_TIMESTAMP(),
                CORE_ERROR_MESSAGE = NULL,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE C.STG_STATUS = 'SUCCESS'
              AND C.CORE_STATUS IN ('PENDING', 'FAILED')
              AND EXISTS
              (
                  SELECT 1
                  FROM SKYPOINTS_DE.OPS.INGESTION_AUDIT A
                  WHERE A.BATCH_ID = C.BATCH_ID
                    AND A.ENTITY_TYPE = 'MEMBER_PROFILE'
                    AND A.STATUS = 'SUCCESS'
              )
        """)

        cursor.execute(expire_sql)
        expired_count = cursor.rowcount

        print("CORE records expired:", expired_count)

        # --------------------------------------------------
        # STEP 4: INSERT NEW CORE SCD2 VERSIONS
        # --------------------------------------------------

        print("\nStep 4: Inserting new CORE versions...")

        cursor.execute(core_insert_sql)
        core_inserted_count = cursor.rowcount

        print("CORE records inserted:", core_inserted_count)

        # --------------------------------------------------
        # STEP 5: MARK CORE BATCHES SUCCESSFUL
        # --------------------------------------------------

        cursor.execute("""
            UPDATE SKYPOINTS_DE.OPS.MEMBER_BATCH_CONTROL C
            SET
                CORE_STATUS = 'SUCCESS',
                CORE_COMPLETED_AT = CURRENT_TIMESTAMP(),
                CORE_ERROR_MESSAGE = NULL,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE C.STG_STATUS = 'SUCCESS'
              AND C.CORE_STATUS IN ('PENDING', 'FAILED')
              AND EXISTS
              (
                  SELECT 1
                  FROM SKYPOINTS_DE.OPS.INGESTION_AUDIT A
                  WHERE A.BATCH_ID = C.BATCH_ID
                    AND A.ENTITY_TYPE = 'MEMBER_PROFILE'
                    AND A.STATUS = 'SUCCESS'
              )
        """)

        # Commit STG, CORE and status changes together.
        connection.commit()

        print("\nIncremental member processing completed successfully.")

        return {
            "registered_batch_count": registered_count,
            "stg_inserted_count": stg_inserted_count,
            "expired_count": expired_count,
            "core_inserted_count": core_inserted_count,
            "status": "SUCCESS"
        }

    except Exception as error:

        if connection:
            connection.rollback()

        print(
            "\nMember processing failed. "
            "Transformation transaction rolled back."
        )
        print("Error:", error)

        # Record failure separately so eligible batches can retry.
        if connection and cursor and registration_committed:

            try:
                cursor.execute("""
                    UPDATE SKYPOINTS_DE.OPS.MEMBER_BATCH_CONTROL C
                    SET
                        STG_STATUS = 'FAILED',
                        STG_COMPLETED_AT = CURRENT_TIMESTAMP(),
                        STG_ERROR_MESSAGE = %s,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE C.STG_STATUS IN ('PENDING', 'FAILED')
                      AND EXISTS
                      (
                          SELECT 1
                          FROM SKYPOINTS_DE.OPS.INGESTION_AUDIT A
                          WHERE A.BATCH_ID = C.BATCH_ID
                            AND A.ENTITY_TYPE = 'MEMBER_PROFILE'
                            AND A.STATUS = 'SUCCESS'
                      )
                """, (str(error)[:5000],))

                cursor.execute("""
                    UPDATE SKYPOINTS_DE.OPS.MEMBER_BATCH_CONTROL C
                    SET
                        CORE_STATUS = 'FAILED',
                        CORE_COMPLETED_AT = CURRENT_TIMESTAMP(),
                        CORE_ERROR_MESSAGE = %s,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE C.STG_STATUS = 'SUCCESS'
                      AND C.CORE_STATUS IN ('PENDING', 'FAILED')
                      AND EXISTS
                      (
                          SELECT 1
                          FROM SKYPOINTS_DE.OPS.INGESTION_AUDIT A
                          WHERE A.BATCH_ID = C.BATCH_ID
                            AND A.ENTITY_TYPE = 'MEMBER_PROFILE'
                            AND A.STATUS = 'SUCCESS'
                      )
                """, (str(error)[:5000],))

                connection.commit()

            except Exception as status_error:
                connection.rollback()

                print(
                    "WARNING: Could not record batch failure:",
                    status_error
                )

        raise

    finally:

        if cursor:
            cursor.close()

        if connection:
            connection.close()
