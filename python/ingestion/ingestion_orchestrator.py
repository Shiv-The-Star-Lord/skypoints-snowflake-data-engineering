
from pathlib import Path

import pandas as pd

from python.utils.config_utils import (
    load_project_config,
    get_fully_qualified_name
)

from python.transformations.member_processor import (
    run_member_processing
)

from python.transformations.redemption_processor import (
    run_redemption_processing
)

from python.ingestion.ingestion_pipeline import (
    run_member_ingestion
)

from python.ingestion.file_reader import read_file

from python.ingestion.data_validator import (
    validate_redemption_data,
    validate_member_identity_records
)

from python.snowflake_loader import (
    get_snowflake_connection,
    load_member_data,
    load_redemption_data
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

MAPPING_PATH = PROJECT_ROOT / "config" / "column_mapping.json"


# --------------------------------------------------
# 1. LOAD SOURCE CONFIGURATION
# --------------------------------------------------

def load_source_config():
    """Load the complete project configuration."""

    return load_project_config()


# --------------------------------------------------
# 2. NORMALIZE MEMBER IDENTITY
# --------------------------------------------------

def normalize_identity_value(value):
    """Normalize country codes and member IDs for comparison."""

    if pd.isna(value):
        return ""

    if isinstance(value, float) and value.is_integer():
        return str(int(value))

    return str(value).strip().upper()


# --------------------------------------------------
# 3. GET EXISTING MEMBER IDENTITIES FROM CORE
# --------------------------------------------------

def get_existing_member_identities(connection, incoming_records):
    """
    Retrieve matching historical composite identities from CORE.

    Business identity:
        COUNTRY_CODE + MEMBER_ID

    All SCD2 history is checked, not only current records.

    Returns:
        {
            ("IND", "1"),
            ("USA", "1")
        }
    """

    if incoming_records.empty:
        return set()

    required_columns = {"country_code", "member_id"}

    if not required_columns.issubset(incoming_records.columns):
        return set()

    incoming_identities = set()

    for _, row in incoming_records.iterrows():

        country_code = normalize_identity_value(
            row["country_code"]
        )

        member_id = normalize_identity_value(
            row["member_id"]
        )

        if country_code and member_id:
            incoming_identities.add(
                (country_code, member_id)
            )

    if not incoming_identities:
        return set()

    core_table = get_fully_qualified_name("member_core")

    conditions = []
    parameters = []

    for country_code, member_id in sorted(incoming_identities):

        conditions.append(
            """
            (
                UPPER(TRIM(COUNTRY_CODE)) = %s
                AND TRIM(TO_VARCHAR(MEMBER_ID)) = %s
            )
            """
        )

        parameters.extend([
            country_code,
            member_id
        ])

    sql = f"""
        SELECT DISTINCT
            UPPER(TRIM(COUNTRY_CODE)) AS COUNTRY_CODE,
            TRIM(TO_VARCHAR(MEMBER_ID)) AS MEMBER_ID
        FROM {core_table}
        WHERE {" OR ".join(conditions)}
    """

    cursor = connection.cursor()

    try:
        cursor.execute(sql, tuple(parameters))
        rows = cursor.fetchall()

    finally:
        cursor.close()

    existing_identities = set()

    for country_code, member_id in rows:

        normalized_country = normalize_identity_value(
            country_code
        )

        normalized_id = normalize_identity_value(
            member_id
        )

        if normalized_country and normalized_id:
            existing_identities.add(
                (normalized_country, normalized_id)
            )

    return existing_identities


# --------------------------------------------------
# 4. RUN MEMBER SOURCE
# --------------------------------------------------

def run_member_source(source_config, file_path=None):
    """
    Execute one configured member source.

    1. Read and standardize source records.
    2. Run normal data validation.
    3. Check composite identities across the source file.
    4. Check identities against CORE history.
    5. Quarantine invalid and conflicting records.
    6. Load eligible records into RAW.
    7. Process RAW -> STG -> CORE.
    """

    source_file = (
        Path(file_path)
        if file_path
        else PROJECT_ROOT / source_config["sample_file"]
    )

    if not source_file.exists():
        raise FileNotFoundError(
            f"Source file not found: {source_file}"
        )

    print("\nStarting member ingestion")
    print("Source ID:", source_config["source_id"])
    print("Source file:", source_file.name)

    # Step 1: Read, standardize and run normal validation.
    ingestion_result = run_member_ingestion(
        file_path=source_file,
        mapping_path=MAPPING_PATH,
        file_format=source_config["format"],
        default_country_code=source_config.get(
            "default_country_code"
        ),
        date_format=source_config.get("date_format")
    )

    all_records_df = ingestion_result["data"].copy()
    valid_df = ingestion_result["valid_data"].copy()
    rejected_df = ingestion_result["rejected_data"].copy()

    identity_conflict_count = 0
    connection = None

    try:
        # Step 2: Connect to Snowflake.
        connection = get_snowflake_connection()

        # Step 3: Retrieve existing composite identities using
        # all incoming rows, including normally rejected rows.
        existing_identities = get_existing_member_identities(
            connection,
            all_records_df
        )

        # Step 4: Validate identities across the complete source.
        identity_result = validate_member_identity_records(
            all_records_df,
            existing_identities
        )

        identity_rejected_df = identity_result["rejected_records"]

        identity_conflict_count = len(identity_rejected_df)

        # Identify conflicts by original source row number.
        conflict_row_numbers = set(
            identity_rejected_df["_source_row_number"].tolist()
        )

        # Step 5: Remove identity-conflicting rows from valid records.
        if not valid_df.empty:
            valid_df = valid_df[
                ~valid_df["_source_row_number"].isin(
                    conflict_row_numbers
                )
            ].copy()

        # Merge identity errors into existing validation rejects.
        # Avoid inserting the same source row twice.
        identity_only_rejects = []

        rejected_row_numbers = (
            set(rejected_df["_source_row_number"].tolist())
            if not rejected_df.empty
            else set()
        )

        for _, identity_row in identity_rejected_df.iterrows():

            source_row_number = identity_row["_source_row_number"]

            identity_errors = identity_row["validation_errors"]

            if source_row_number in rejected_row_numbers:

                mask = (
                    rejected_df["_source_row_number"]
                    == source_row_number
                )

                existing_errors = rejected_df.loc[
                    mask, "validation_errors"
                ].iloc[0]

                if not isinstance(existing_errors, list):
                    existing_errors = []

                rejected_df.loc[
                    mask, "validation_errors"
                ] = pd.Series(
                    [existing_errors + identity_errors],
                    index=rejected_df.index[mask]
                )

            else:
                identity_only_rejects.append(identity_row)

        if identity_only_rejects:

            identity_only_df = pd.DataFrame(
                identity_only_rejects
            )

            rejected_df = pd.concat(
                [rejected_df, identity_only_df],
                ignore_index=True
            )

        print("\nMember validation results:")
        print("Total records:", ingestion_result["total_records"])
        print("Valid records:", len(valid_df))
        print("Rejected records:", len(rejected_df))
        print("Identity conflicts:", identity_conflict_count)

        # Step 6: Load eligible records into RAW and rejects into OPS.
        load_result = load_member_data(
            connection=connection,
            df=valid_df,
            rejected_df=rejected_df,
            file_path=source_file,
            source_id=source_config["source_id"],
            entity_type=source_config["entity"]
        )

    finally:
        if connection:
            connection.close()

    print("\nRAW and quarantine ingestion result:", load_result)

    # Stop processing when the file was already ingested.
    if load_result["status"] == "SKIPPED_DUPLICATE":

        return {
            "raw_load": load_result,
            "transformation": None,
            "status": "SKIPPED_DUPLICATE"
        }

    # Step 7: Process RAW -> STG -> CORE.
    processing_result = run_member_processing()

    return {
        "ingestion": {
            "total_records": ingestion_result["total_records"],
            "valid_records": len(valid_df),
            "rejected_records": len(rejected_df),
            "identity_conflicts": identity_conflict_count
        },
        "raw_load": load_result,
        "transformation": processing_result,
        "status": "SUCCESS"
    }


# --------------------------------------------------
# 5. RUN REDEMPTION SOURCE
# --------------------------------------------------

def run_redemption_source(source_config, file_path=None):
    """Execute one configured redemption JSON source."""

    source_file = (
        Path(file_path)
        if file_path
        else PROJECT_ROOT / source_config["sample_file"]
    )

    if not source_file.exists():
        raise FileNotFoundError(
            f"Source file not found: {source_file}"
        )

    print("\nStarting redemption ingestion")
    print("Source ID:", source_config["source_id"])
    print("Source file:", source_file.name)

    data = read_file(
        source_file,
        source_config["format"]
    )

    validation_result = validate_redemption_data(data)

    if not validation_result["is_valid"]:
        raise ValueError(
            "Redemption data validation failed: "
            + "; ".join(validation_result["errors"])
        )

    connection = None

    try:
        connection = get_snowflake_connection()

        load_result = load_redemption_data(
            connection=connection,
            data=data,
            file_path=source_file,
            source_id=source_config["source_id"],
            entity_type=source_config["entity"]
        )

    finally:
        if connection:
            connection.close()

    print("\nRAW ingestion result:", load_result)

    if load_result["status"] == "SKIPPED_DUPLICATE":

        return {
            "raw_load": load_result,
            "transformation": None,
            "status": "SKIPPED_DUPLICATE"
        }

    processing_result = run_redemption_processing()

    return {
        "raw_load": load_result,
        "transformation": processing_result,
        "status": "SUCCESS"
    }


# --------------------------------------------------
# 6. RUN ALL ENABLED SOURCES
# --------------------------------------------------

def run_all_member_sources():
    """Discover and process all matching production files."""

    config = load_source_config()

    for source in config["sources"]:

        if not source.get("enabled", False):
            print(
                "\nSkipping disabled source:",
                source.get("source_id")
            )
            continue

        landing_directory = (
            PROJECT_ROOT / source["landing_directory"]
        )

        file_pattern = source["file_pattern"]

        matching_files = sorted(
            landing_directory.glob(file_pattern)
        )

        if not matching_files:
            print(
                "\nNo matching production files found for:",
                source["source_id"]
            )
            continue

        for source_file in matching_files:

            print("\nDiscovered production file:", source_file)

            if source["entity"] == "MEMBER_PROFILE":

                run_member_source(
                    source,
                    file_path=source_file
                )

            elif source["entity"] == "REDEMPTION":

                run_redemption_source(
                    source,
                    file_path=source_file
                )

            else:

                print(
                    "\nSkipping unsupported source entity:",
                    source["entity"]
                )


# --------------------------------------------------
# 7. MAIN ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":
    run_all_member_sources()
