
import os
import uuid
import json
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

from python.utils.file_utils import (
    calculate_file_checksum,
    get_file_size
)

from python.utils.config_utils import (
    get_fully_qualified_name
)


# --------------------------------------------------
# 1. LOAD ENVIRONMENT VARIABLES
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(ENV_FILE)


# --------------------------------------------------
# 2. CONFIGURATION-DRIVEN TABLE RESOLUTION
# --------------------------------------------------

def get_table_name(object_key):
    """
    Resolve a fully qualified Snowflake table name.

    Database: .env
    Schema and table: source_config.json
    """

    return get_fully_qualified_name(object_key)


# --------------------------------------------------
# 3. SNOWFLAKE CONNECTION
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
# 4. AUDIT HELPER FUNCTIONS
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

    audit_table = get_table_name("ingestion_audit")

    sql = f"""
        INSERT INTO {audit_table} (
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
    """Check whether identical content from the same source
    has already completed successfully."""

    audit_table = get_table_name("ingestion_audit")

    sql = f"""
        SELECT COUNT(*)
        FROM {audit_table}
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

    audit_table = get_table_name("ingestion_audit")

    sql = f"""
        UPDATE {audit_table}
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
# 5. PREPARE MEMBER RECORDS
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

        source_row_number = row.get(
            "_source_row_number",
            row_number
        )

        values.extend([
            source_file,
            int(source_row_number),
            batch_id
        ])

        records.append(tuple(values))

    return records


# --------------------------------------------------
# 6. PREPARE REJECTED MEMBER RECORDS
# --------------------------------------------------

def prepare_rejected_member_records(
    rejected_df,
    source_file,
    source_id,
    batch_id
):
    """Prepare one quarantine row per validation error."""

    records = []

    for row_number, (_, row) in enumerate(
        rejected_df.iterrows(),
        start=1
    ):

        source_row_number = int(
            row.get("_source_row_number", row_number)
        )

        validation_errors = row.get(
            "validation_errors",
            []
        )

        record_data = {}

        for column, value in row.items():

            if column == "validation_errors":
                continue

            if isinstance(value, pd.Timestamp):
                value = value.isoformat()

            elif value is None:
                pass

            elif isinstance(value, (dict, list, tuple)):
                pass

            else:
                try:
                    if pd.isna(value):
                        value = None
                except (TypeError, ValueError):
                    pass

            if hasattr(value, "item"):
                value = value.item()

            record_data[column] = value

        record_json = json.dumps(
            record_data,
            default=str
        )

        for error in validation_errors:

            records.append((
                source_id,
                source_file,
                source_row_number,
                batch_id,
                record_json,
                error.get("check"),
                error.get("column"),
                error.get("message")
            ))

    return records


# --------------------------------------------------
# 7. INSERT REJECTED RECORDS INTO OPS
# --------------------------------------------------

def load_rejected_member_records(
    cursor,
    rejected_df,
    source_file,
    source_id,
    batch_id
):
    """Insert rejected member records into OPS quarantine."""

    if rejected_df is None or rejected_df.empty:
        return 0

    records = prepare_rejected_member_records(
        rejected_df,
        source_file,
        source_id,
        batch_id
    )

    if not records:
        return 0

    rejected_table = get_table_name(
        "rejected_member_records"
    )

    sql = f"""
        INSERT INTO {rejected_table} (
            SOURCE_ID,
            SOURCE_FILE,
            SOURCE_ROW_NUMBER,
            BATCH_ID,
            RECORD_DATA,
            ERROR_CHECK,
            ERROR_COLUMN,
            ERROR_MESSAGE
        )
        SELECT
            %s,
            %s,
            %s,
            %s,
            PARSE_JSON(%s),
            %s,
            %s,
            %s
    """

    inserted_count = 0

    for record in records:
        cursor.execute(sql, record)
        inserted_count += 1

    return inserted_count


# --------------------------------------------------
# 8. LOAD MEMBER DATA INTO RAW AND QUARANTINE
# --------------------------------------------------

def load_member_data(
    connection,
    df,
    file_path,
    source_id,
    entity_type="MEMBER_PROFILE",
    rejected_df=None
):
    """
    Load valid records into RAW and rejected records
    into OPS quarantine using the same batch ID.
    """

    file_path = Path(file_path)

    source_file = file_path.name
    checksum = calculate_file_checksum(file_path)
    file_size = get_file_size(file_path)

    batch_id = str(uuid.uuid4())

    cursor = connection.cursor()

    rejected_record_count = 0

    try:

        print("\nChecking file duplication...")

        already_processed = is_file_already_processed(
            cursor,
            source_id,
            checksum
        )

        total_records = len(df) + (
            len(rejected_df)
            if rejected_df is not None else 0
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
                total_records,
                "SKIPPED_DUPLICATE"
            )

            connection.commit()

            print("File already processed successfully.")
            print("Skipping RAW and quarantine loading.")
            print("Batch ID:", batch_id)

            return {
                "batch_id": batch_id,
                "status": "SKIPPED_DUPLICATE",
                "records_loaded": 0,
                "records_rejected": 0,
                "quarantine_entries": 0
            }

        # Create STARTED audit record.

        create_audit_record(
            cursor,
            batch_id,
            source_id,
            source_file,
            checksum,
            entity_type,
            file_size,
            total_records,
            "STARTED"
        )

        connection.commit()

        # Prepare valid records.

        records = prepare_member_records(
            df,
            source_file,
            batch_id
        )

        # Insert valid records into RAW.

        if records:

            raw_table = get_table_name("member_raw")

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
                INSERT INTO {raw_table}
                ({column_names})
                VALUES ({placeholders})
            """

            cursor.executemany(sql, records)

        # Insert rejected records into quarantine.

        rejected_record_count = load_rejected_member_records(
            cursor,
            rejected_df,
            source_file,
            source_id,
            batch_id
        )

        # Mark audit SUCCESS.

        update_audit_status(
            cursor,
            batch_id,
            "SUCCESS"
        )

        # Commit RAW + quarantine + audit SUCCESS.

        connection.commit()

        print("\nMember ingestion completed!")
        print("Source file:", source_file)
        print("Source ID:", source_id)
        print("Valid records loaded:", len(records))
        print(
            "Rejected records:",
            len(rejected_df) if rejected_df is not None else 0
        )
        print("Quarantine entries:", rejected_record_count)
        print("Batch ID:", batch_id)
        print("Status: SUCCESS")

        return {
            "batch_id": batch_id,
            "status": "SUCCESS",
            "records_loaded": len(records),
            "records_rejected": (
                len(rejected_df)
                if rejected_df is not None else 0
            ),
            "quarantine_entries": rejected_record_count
        }

    except Exception as error:

        connection.rollback()

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
# 9. LOAD REDEMPTION DATA INTO RAW
# --------------------------------------------------

def load_redemption_data(
    connection,
    data,
    file_path,
    source_id,
    entity_type="REDEMPTION"
):
    """
    Load nested redemption JSON into RAW with
    duplicate-file detection and audit tracking.
    """

    file_path = Path(file_path)

    source_file = file_path.name
    checksum = calculate_file_checksum(file_path)
    file_size = get_file_size(file_path)

    batch_id = str(uuid.uuid4())

    cursor = connection.cursor()

    try:

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
                1,
                "SKIPPED_DUPLICATE"
            )

            connection.commit()

            return {
                "batch_id": batch_id,
                "status": "SKIPPED_DUPLICATE",
                "records_loaded": 0
            }

        create_audit_record(
            cursor,
            batch_id,
            source_id,
            source_file,
            checksum,
            entity_type,
            file_size,
            1,
            "STARTED"
        )

        connection.commit()

        member_id = str(data["member_id"])

        feed_date = pd.to_datetime(
            str(data["feed_date"]),
            format="%Y%m%d"
        ).date()

        redemption_data = json.dumps(
            data["redemptions"]
        )

        raw_table = get_table_name("redemption_raw")

        sql = f"""
            INSERT INTO {raw_table} (
                MEMBER_ID,
                FEED_DATE,
                REDEMPTION_DATA,
                SOURCE_FILE,
                BATCH_ID
            )
            SELECT
                %s,
                %s,
                PARSE_JSON(%s),
                %s,
                %s
        """

        cursor.execute(
            sql,
            (
                member_id,
                feed_date,
                redemption_data,
                source_file,
                batch_id
            )
        )

        update_audit_status(
            cursor,
            batch_id,
            "SUCCESS"
        )

        connection.commit()

        print("\nRedemption data loaded successfully!")
        print("Source file:", source_file)
        print("Member ID:", member_id)
        print("Transactions:", len(data["redemptions"]))
        print("Batch ID:", batch_id)
        print("Status: SUCCESS")

        return {
            "batch_id": batch_id,
            "status": "SUCCESS",
            "records_loaded": 1
        }

    except Exception as error:

        connection.rollback()

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

        print("\nRedemption data loading failed!")
        print("Error:", error)

        raise

    finally:
        cursor.close()


# --------------------------------------------------
# 10. MAIN EXECUTION
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
