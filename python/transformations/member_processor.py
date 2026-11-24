
import re
from pathlib import Path

from python.snowflake_loader import get_snowflake_connection
from python.provision_snowflake import (
    build_sql_object_mapping,
    prepare_sql,
)
from python.utils.config_utils import (
    get_fully_qualified_name,
    get_snowflake_object,
    load_project_config,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

INCREMENTAL_STG_SQL_PATH = (
    PROJECT_ROOT / "sql" / "15_incremental_staging_member_profiles.sql"
)

EXPIRE_SQL_PATH = (
    PROJECT_ROOT / "sql" / "16a_core_expire_current.sql"
)

INSERT_CORE_SQL_PATH = (
    PROJECT_ROOT / "sql" / "17b_core_insert_versions.sql"
)

COUNTRY_SYNC_SQL_PATH = (
    PROJECT_ROOT / "sql" / "20_sync_country_member_tables.sql"
)


def read_sql_file(file_path):
    """Read SQL from a file."""

    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


def extract_staging_insert(sql_text, target_table=None):
    """
    Extract the STG INSERT statement.

    Supports fully qualified table names and configuration
    placeholders as the INSERT target.
    """

    table_identifier = (
        r"(?:[A-Za-z_][A-Za-z0-9_$]*\.){0,2}"
        r"[A-Za-z_][A-Za-z0-9_$]*"
    )

    match = re.search(
        r"INSERT\s+INTO\s+"
        + table_identifier
        + r"\s*\(.*?;",
        sql_text,
        flags=re.IGNORECASE | re.DOTALL
    )

    if not match:
        raise ValueError(
            "STG member INSERT statement not found in SQL file."
        )

    statement = match.group(0)

    # Preserve compatibility with existing callers and tests.
    if target_table is not None:
        statement = re.sub(
            r"(INSERT\s+INTO\s+)"
            + table_identifier,
            rf"\g<1>{target_table}",
            statement,
            count=1,
            flags=re.IGNORECASE
        )

    return statement


def replace_configured_objects(sql_text, object_mapping):
    """
    Replace SQL placeholders with configured fully qualified
    Snowflake object names.
    """

    for placeholder, configured_name in object_mapping.items():
        sql_text = sql_text.replace(
            placeholder,
            configured_name
        )

    return sql_text


def validate_sql_placeholders(sql_statements):
    """Prevent unresolved configuration placeholders from executing."""

    unresolved = set()

    for sql_text in sql_statements:
        unresolved.update(
            re.findall(
                r"__[A-Z0-9_]+__",
                sql_text
            )
        )

    if unresolved:
        raise ValueError(
            "Unresolved SQL configuration placeholders: "
            f"{sorted(unresolved)}"
        )


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
        # STEP 0: RESOLVE CONFIGURED SNOWFLAKE OBJECTS
        # --------------------------------------------------

        member_raw_table = get_fully_qualified_name(
            "member_raw"
        )

        member_staging_table = get_fully_qualified_name(
            "member_staging"
        )

        member_core_table = get_fully_qualified_name(
            "member_core"
        )

        batch_control_table = get_fully_qualified_name(
            "member_batch_control"
        )

        ingestion_audit_table = get_fully_qualified_name(
            "ingestion_audit"
        )

        batch_control_schema = get_snowflake_object(
            "member_batch_control"
        )["schema"]

        core_schema = get_snowflake_object(
            "member_core"
        )["schema"]

        database = get_snowflake_object(
            "member_batch_control"
        )["database"]

        # --------------------------------------------------
        # STEP 1: BUILD CONFIGURATION-DRIVEN SQL MAPPING
        # --------------------------------------------------

        object_mapping = {
            "__MEMBER_RAW_TABLE__": member_raw_table,
            "__MEMBER_STAGING_TABLE__": member_staging_table,
            "__MEMBER_CORE_TABLE__": member_core_table,
            "__MEMBER_BATCH_CONTROL_TABLE__": batch_control_table,
            "__INGESTION_AUDIT_TABLE__": ingestion_audit_table,
        }

        # --------------------------------------------------
        # STEP 2: LOAD AND CONFIGURE SQL FILES
        # --------------------------------------------------

        # Extract the incremental STG INSERT statement.
        incremental_stg_sql = extract_staging_insert(
            read_sql_file(INCREMENTAL_STG_SQL_PATH)
        )

        # Resolve all five configured objects.
        incremental_stg_sql = replace_configured_objects(
            incremental_stg_sql,
            object_mapping
        )

        # Load and configure CORE expiry SQL.
        expire_sql = replace_configured_objects(
            read_sql_file(EXPIRE_SQL_PATH),
            object_mapping
        )

        # Load and configure CORE insertion SQL.
        core_insert_sql = replace_configured_objects(
            read_sql_file(INSERT_CORE_SQL_PATH),
            object_mapping
        )

        # Load and configure country synchronization SQL.
        project_config = load_project_config()
        country_mapping = build_sql_object_mapping(project_config)
        country_sync_sql = prepare_sql(
            COUNTRY_SYNC_SQL_PATH,
            country_mapping
        )

        # The member processor owns the transaction.
        # Remove the standalone SQL transaction wrapper.
        country_sync_sql = re.sub(
            r"(?im)^\s*BEGIN\s+TRANSACTION\s*;\s*",
            "",
            country_sync_sql,
            count=1
        )
        country_sync_sql = re.sub(
            r"(?im)^\s*COMMIT\s*;\s*$",
            "",
            country_sync_sql,
            count=1
        )

        # Ensure no unresolved placeholders reach Snowflake.
        validate_sql_placeholders([
            incremental_stg_sql,
            expire_sql,
            core_insert_sql,
            country_sync_sql
        ])

        # --------------------------------------------------
        # STEP 3: CONNECT TO SNOWFLAKE
        # --------------------------------------------------

        connection = get_snowflake_connection()
        connection.autocommit(False)

        cursor = connection.cursor()

        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {batch_control_schema}")

        print("\nStarting incremental member processing")

        # --------------------------------------------------
        # STEP 4: REGISTER SUCCESSFUL RAW BATCHES
        # --------------------------------------------------

        print("\nStep 1: Registering successful RAW batches...")

        register_sql = f"""
            INSERT INTO {batch_control_table}
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
            FROM {ingestion_audit_table} A
            WHERE A.ENTITY_TYPE = 'MEMBER_PROFILE'
              AND A.STATUS = 'SUCCESS'
              AND NOT EXISTS
              (
                  SELECT 1
                  FROM {batch_control_table} C
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
        # STEP 5: PROCESS PENDING BATCHES INTO STG
        # --------------------------------------------------

        print("\nStep 2: Processing pending batches into STG...")

        cursor.execute(f"""
            UPDATE {batch_control_table} C
            SET
                STG_STARTED_AT = CURRENT_TIMESTAMP(),
                STG_ERROR_MESSAGE = NULL,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE C.STG_STATUS IN ('PENDING', 'FAILED')
              AND EXISTS
              (
                  SELECT 1
                  FROM {ingestion_audit_table} A
                  WHERE A.BATCH_ID = C.BATCH_ID
                    AND A.ENTITY_TYPE = 'MEMBER_PROFILE'
                    AND A.STATUS = 'SUCCESS'
              )
        """)

        cursor.execute(incremental_stg_sql)

        stg_inserted_count = cursor.rowcount

        print("STG records inserted:", stg_inserted_count)

        cursor.execute(f"""
            UPDATE {batch_control_table} C
            SET
                STG_STATUS = 'SUCCESS',
                STG_COMPLETED_AT = CURRENT_TIMESTAMP(),
                STG_ERROR_MESSAGE = NULL,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE C.STG_STATUS IN ('PENDING', 'FAILED')
              AND EXISTS
              (
                  SELECT 1
                  FROM {ingestion_audit_table} A
                  WHERE A.BATCH_ID = C.BATCH_ID
                    AND A.ENTITY_TYPE = 'MEMBER_PROFILE'
                    AND A.STATUS = 'SUCCESS'
              )
        """)

        # --------------------------------------------------
        # STEP 6: EXPIRE EXISTING CORE VERSIONS
        # --------------------------------------------------

        cursor.execute(f"USE SCHEMA {core_schema}")

        print("\nStep 3: Expiring existing CORE records...")

        cursor.execute(f"""
            UPDATE {batch_control_table} C
            SET
                CORE_STARTED_AT = CURRENT_TIMESTAMP(),
                CORE_ERROR_MESSAGE = NULL,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE C.STG_STATUS = 'SUCCESS'
              AND C.CORE_STATUS IN ('PENDING', 'FAILED')
              AND EXISTS
              (
                  SELECT 1
                  FROM {ingestion_audit_table} A
                  WHERE A.BATCH_ID = C.BATCH_ID
                    AND A.ENTITY_TYPE = 'MEMBER_PROFILE'
                    AND A.STATUS = 'SUCCESS'
              )
        """)

        cursor.execute(expire_sql)

        expired_count = cursor.rowcount

        print("CORE records expired:", expired_count)

        # --------------------------------------------------
        # STEP 7: INSERT NEW CORE SCD2 VERSIONS
        # --------------------------------------------------

        print("\nStep 4: Inserting new CORE versions...")

        cursor.execute(core_insert_sql)

        core_inserted_count = cursor.rowcount

        print("CORE records inserted:", core_inserted_count)

        # --------------------------------------------------
        # STEP 8: MARK CORE BATCHES SUCCESSFUL
        # --------------------------------------------------

        cursor.execute(f"""
            UPDATE {batch_control_table} C
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
                  FROM {ingestion_audit_table} A
                  WHERE A.BATCH_ID = C.BATCH_ID
                    AND A.ENTITY_TYPE = 'MEMBER_PROFILE'
                    AND A.STATUS = 'SUCCESS'
              )
        """)

        # --------------------------------------------------
        # STEP 9: SYNCHRONIZE COUNTRY-SPECIFIC TABLES
        # --------------------------------------------------

        print("\nStep 5: Synchronizing country member tables...")

        country_sync_statements = [
            statement.strip()
            for statement in country_sync_sql.split(";")
            if statement.strip()
        ]

        for statement in country_sync_statements:
            cursor.execute(statement)

        print(
            "Country synchronization statements executed:",
            len(country_sync_statements)
        )

        # Commit STG, CORE, country tables and status changes together.
        connection.commit()

        print("\nIncremental member processing completed successfully.")

        return {
            "registered_batch_count": registered_count,
            "stg_inserted_count": stg_inserted_count,
            "expired_count": expired_count,
            "core_inserted_count": core_inserted_count,
            "country_sync_statement_count": len(country_sync_statements),
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
                cursor.execute(f"""
                    UPDATE {batch_control_table} C
                    SET
                        STG_STATUS = 'FAILED',
                        STG_COMPLETED_AT = CURRENT_TIMESTAMP(),
                        STG_ERROR_MESSAGE = %s,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE C.STG_STATUS IN ('PENDING', 'FAILED')
                      AND EXISTS
                      (
                          SELECT 1
                          FROM {ingestion_audit_table} A
                          WHERE A.BATCH_ID = C.BATCH_ID
                            AND A.ENTITY_TYPE = 'MEMBER_PROFILE'
                            AND A.STATUS = 'SUCCESS'
                      )
                """, (str(error)[:5000],))

                cursor.execute(f"""
                    UPDATE {batch_control_table} C
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
                          FROM {ingestion_audit_table} A
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
