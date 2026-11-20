
import os
import uuid
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

from python.utils.file_utils import (
    calculate_file_checksum,
    get_file_size
)


# --------------------------------------------------
# 1. LOAD ENVIRONMENT VARIABLES
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(ENV_FILE)


# --------------------------------------------------
# 2. SNOWFLAKE CONNECTION
# --------------------------------------------------

def get_snowflake_connection():
    """Establish a connection to Snowflake."""

    required_variables = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD"
    ]

    missing_variables = [
        variable
        for variable in required_variables
        if not os.getenv(variable)
    ]

    if missing_variables:
        raise ValueError(
            f"Missing environment variables: {missing_variables}"
        )

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )


# --------------------------------------------------
# 3. AUDIT HELPER FUNCTIONS
# --------------------------------------------------

def create_audit_record(
    cursor,
    batch_id,
    source_id,
    source_file,
    checksum,
    entity_type,
    file_size,
    record_count,
    status
):
    """Insert one ingestion attempt into OPS audit."""

    sql = """
        INSERT INTO SKYPOINTS_DE.OPS.INGESTION_AUDIT (
            BATCH_ID,
            SOURCE_ID,
            SOURCE_FILE,
            FILE_CHECKSUM,
            ENTITY_TYPE,
            FILE_SIZE_BYTES,
            RECORD_COUNT,
            STATUS
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    cursor.execute(
        sql,
        (
            batch_id,
            source_id,
            source_file,
            checksum,
            entity_type,
            file_size,
            record_count,
            status
        )
    )


def is_file_already_processed(cursor, source_id, checksum):
    """
    Check whether identical content from the same source
    has already completed successfully.
    """

    sql = """
        SELECT COUNT(*)
        FROM SKYPOINTS_DE.OPS.INGESTION_AUDIT
        WHERE SOURCE_ID = %s
          AND FILE_CHECKSUM = %s
          AND STATUS = 'SUCCESS'
    """

    cursor.execute(sql, (source_id, checksum))

    return cursor.fetchone()[0] > 0


def update_audit_status(
    cursor,
    batch_id,
    status,
    error_message=None
):
    """Update the final ingestion status."""

    sql = """
        UPDATE SKYPOINTS_DE.OPS.INGESTION_AUDIT
        SET
            STATUS = %s,
            COMPLETED_AT = CURRENT_TIMESTAMP(),
            ERROR_MESSAGE = %s
        WHERE BATCH_ID = %s
    """

    cursor.execute(
        sql,
        (
            status,
            error_message[:5000] if error_message else None,
            batch_id
        )
    )


# --------------------------------------------------
# 4. PREPARE MEMBER RECORDS
# --------------------------------------------------

def prepare_member_records(df, source_file, batch_id):

    target_columns = [
        "MEMBER_ID",
        "MEMBER_NAME",
        "DATE_OF_BIRTH",
        "TIER_CODE",
        "ENROLLMENT_DATE",
        "LAST_FLIGHT_DATE",
        "AGENT_NAME",
        "STATE_CODE",
        "POSTAL_CODE",
        "IS_ACTIVE",
        "MEMBER_TYPE",
        "COUNTRY_CODE"
    ]

    date_columns = [
        "DATE_OF_BIRTH",
        "ENROLLMENT_DATE",
        "LAST_FLIGHT_DATE"
    ]

    records = []

    for row_number, (_, row) in enumerate(
        df.iterrows(),
        start=1
    ):

        values = []

        for column in target_columns:

            value = row.get(column.lower())

            if pd.isna(value) or str(value).strip() == "":
                value = None

            elif column in date_columns:
                value = pd.to_datetime(
                    value,
                    errors="raise"
                ).date()

            elif not isinstance(value, str):
                value = str(value)

            values.append(value)

        values.extend([
            source_file,
            row_number,
            batch_id
        ])

        records.append(tuple(values))

    return records


# --------------------------------------------------
# 5. LOAD MEMBER DATA INTO RAW
# --------------------------------------------------

def load_member_data(
    connection,
    df,
    file_path,
    source_id,
    entity_type="MEMBER_PROFILE"
):
    """
    Load member records into RAW and track the attempt
    in OPS.INGESTION_AUDIT.
    """

    file_path = Path(file_path)

    source_file = file_path.name
    checksum = calculate_file_checksum(file_path)
    file_size = get_file_size(file_path)

    batch_id = str(uuid.uuid4())

    cursor = connection.cursor()

    try:

        # ------------------------------------------
        # STEP 1: CHECK DUPLICATE FILE
        # ------------------------------------------

        print("\nChecking file duplication...")

        already_processed = is_file_already_processed(
            cursor,
            source_id,
            checksum
        )

        if already_processed:

            create_audit_record(
                cursor,
                batch_id,
                source_id,
                source_file,
                checksum,
                entity_type,
                file_size,
                len(df),
                "SKIPPED_DUPLICATE"
            )

            update_audit_status(
                cursor,
                batch_id,
                "SKIPPED_DUPLICATE"
            )

            connection.commit()

            print("File already processed successfully.")
            print("Skipping RAW loading.")
            print("Batch ID:", batch_id)

            return {
                "batch_id": batch_id,
                "status": "SKIPPED_DUPLICATE",
                "records_loaded": 0
            }

        # ------------------------------------------
        # STEP 2: CREATE STARTED AUDIT RECORD
        # ------------------------------------------

        create_audit_record(
            cursor,
            batch_id,
            source_id,
            source_file,
            checksum,
            entity_type,
            file_size,
            len(df),
            "STARTED"
        )

        # Commit STARTED independently so that
        # a failed RAW transaction does not erase it.
        connection.commit()

        # ------------------------------------------
        # STEP 3: PREPARE MEMBER RECORDS
        # ------------------------------------------

        records = prepare_member_records(
            df,
            source_file,
            batch_id
        )

        # ------------------------------------------
        # STEP 4: INSERT INTO RAW
        # ------------------------------------------

        if records:

            target_columns = [
                "MEMBER_ID",
                "MEMBER_NAME",
                "DATE_OF_BIRTH",
                "TIER_CODE",
                "ENROLLMENT_DATE",
                "LAST_FLIGHT_DATE",
                "AGENT_NAME",
                "STATE_CODE",
                "POSTAL_CODE",
                "IS_ACTIVE",
                "MEMBER_TYPE",
                "COUNTRY_CODE",
                "SOURCE_FILE",
                "SOURCE_ROW_NUMBER",
                "BATCH_ID"
            ]

            column_names = ", ".join(target_columns)

            placeholders = ", ".join(
                ["%s"] * len(target_columns)
            )

            sql = f"""
                INSERT INTO SKYPOINTS_DE.RAW.RAW_MEMBER_PROFILES
                ({column_names})
                VALUES ({placeholders})
            """

            cursor.executemany(sql, records)

        # ------------------------------------------
        # STEP 5: MARK AUDIT SUCCESS
        # ------------------------------------------

        update_audit_status(
            cursor,
            batch_id,
            "SUCCESS"
        )

        # RAW insertion and SUCCESS update
        # are committed together.
        connection.commit()

        print("\nMember data loaded successfully!")
        print("Source file:", source_file)
        print("Source ID:", source_id)
        print("Records loaded:", len(records))
        print("Batch ID:", batch_id)
        print("Status: SUCCESS")

        return {
            "batch_id": batch_id,
            "status": "SUCCESS",
            "records_loaded": len(records)
        }

    except Exception as error:

        # ------------------------------------------
        # STEP 6: ROLLBACK RAW TRANSACTION
        # ------------------------------------------

        connection.rollback()

        # Record failure separately.
        try:

            update_audit_status(
                cursor,
                batch_id,
                "FAILED",
                str(error)
            )

            connection.commit()

        except Exception:

            connection.rollback()

            print(
                "WARNING: Failed to update audit status. "
                "Check Snowflake batch state:",
                batch_id
            )

        print("\nMember data loading failed!")
        print("Error:", error)

        raise

    finally:

        cursor.close()


# --------------------------------------------------
# 6. MAIN EXECUTION
# --------------------------------------------------

if __name__ == "__main__":

    connection = None

    try:

        connection = get_snowflake_connection()

        cursor = connection.cursor()

        cursor.execute("""
            SELECT
                CURRENT_ACCOUNT(),
                CURRENT_ROLE(),
                CURRENT_WAREHOUSE(),
                CURRENT_DATABASE(),
                CURRENT_SCHEMA()
        """)

        result = cursor.fetchone()

        print("\nSnowflake connection successful!")
        print("Account:", result[0])
        print("Role:", result[1])
        print("Warehouse:", result[2])
        print("Database:", result[3])
        print("Schema:", result[4])

        cursor.close()

    except Exception as error:

        print("Snowflake connection failed:", error)
        raise

    finally:

        if connection:
            connection.close()
