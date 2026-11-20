
import os
import uuid

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv


# --------------------------------------------------
# 1. LOAD ENVIRONMENT VARIABLES
# --------------------------------------------------

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

ENV_FILE = os.path.join(PROJECT_ROOT, ".env")

load_dotenv(ENV_FILE)


# --------------------------------------------------
# 2. SNOWFLAKE CONNECTION
# --------------------------------------------------

def get_snowflake_connection():
    """
    Establish a connection to Snowflake.
    """

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

    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    return connection


# --------------------------------------------------
# 3. LOAD MEMBER DATA INTO RAW TABLE
# --------------------------------------------------

def load_member_data(connection, df, source_file):
    """
    Load standardized member records into Snowflake.

    Target table:
        SKYPOINTS_DE.RAW.RAW_MEMBER_PROFILES
    """

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

    metadata_columns = [
        "SOURCE_FILE",
        "SOURCE_ROW_NUMBER",
        "BATCH_ID"
    ]

    # Generate a unique ID for this ingestion batch
    batch_id = str(uuid.uuid4())

    # Explicit Snowflake column mapping
    insert_columns = target_columns + metadata_columns

    column_names = ", ".join(insert_columns)

    placeholders = ", ".join(
        ["%s"] * len(insert_columns)
    )

    sql = f"""
        INSERT INTO SKYPOINTS_DE.RAW.RAW_MEMBER_PROFILES
        ({column_names})
        VALUES ({placeholders})
    """

    records = []

    date_columns = [
        "DATE_OF_BIRTH",
        "ENROLLMENT_DATE",
        "LAST_FLIGHT_DATE"
    ]

    # Prepare records for Snowflake
    for row_number, (_, row) in enumerate(
        df.iterrows(),
        start=1
    ):

        values = []

        for column in target_columns:

            value = row.get(column.lower())

            # Convert null and empty values to None
            if pd.isna(value) or str(value).strip() == "":
                value = None

            # Convert date values
            elif column in date_columns:
                value = pd.to_datetime(
                    value,
                    errors="raise"
                ).date()

            # Convert other non-string values
            elif not isinstance(value, str):
                value = str(value)

            values.append(value)

        # Add ingestion metadata
        values.extend([
            source_file,
            row_number,
            batch_id
        ])

        records.append(tuple(values))

    if not records:
        print("No member records available for loading.")
        return batch_id

    cursor = connection.cursor()

    try:

        cursor.executemany(
            sql,
            records
        )

        connection.commit()

        print("\nMember data loaded successfully!")
        print("Source file:", source_file)
        print("Records loaded:", len(records))
        print("Batch ID:", batch_id)

    except Exception:

        connection.rollback()

        raise

    finally:

        cursor.close()

    return batch_id


# --------------------------------------------------
# 4. MAIN EXECUTION
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
