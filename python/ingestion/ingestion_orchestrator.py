
import json
from pathlib import Path

import pandas as pd

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

CONFIG_PATH = PROJECT_ROOT / "config" / "source_config.json"
MAPPING_PATH = PROJECT_ROOT / "config" / "column_mapping.json"


# --------------------------------------------------
# 1. LOAD SOURCE CONFIGURATION
# --------------------------------------------------

def load_source_config():
    """Load source definitions from JSON configuration."""

    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        return json.load(file)


# --------------------------------------------------
# 2. GET EXISTING MEMBER IDENTITIES FROM CORE
# --------------------------------------------------

def get_existing_member_identities(connection, member_names):
    """
    Retrieve all historical member IDs from CORE for incoming names.

    All SCD2 history is checked so an earlier identity conflict
    cannot be hidden by looking only at current records.
    """

    normalized_names = sorted({
        str(name).strip().upper()
        for name in member_names
        if name is not None
        and not pd.isna(name)
        and str(name).strip()
    })

    if not normalized_names:
        return {}

    placeholders = ", ".join(
        ["%s"] * len(normalized_names)
    )

    sql = f"""
        SELECT DISTINCT
            UPPER(TRIM(MEMBER_NAME)) AS MEMBER_NAME,
            TRIM(TO_VARCHAR(MEMBER_ID)) AS MEMBER_ID
        FROM SKYPOINTS_DE.CORE.MEMBER_PROFILE
        WHERE UPPER(TRIM(MEMBER_NAME)) IN ({placeholders})
    """

    cursor = connection.cursor()

    try:
        cursor.execute(sql, tuple(normalized_names))
        rows = cursor.fetchall()

    finally:
        cursor.close()

    identities = {}

    for name, member_id in rows:
        if name is None or member_id is None:
            continue

        normalized_name = str(name).strip().upper()
        normalized_id = str(member_id).strip()

        if not normalized_name or not normalized_id:
            continue

        identities.setdefault(
            normalized_name, set()
        ).add(normalized_id)

    return identities


# --------------------------------------------------
# 3. RUN MEMBER SOURCE
# --------------------------------------------------

def run_member_source(source_config, file_path=None):
    """
    Execute one configured member source.

    1. Read and standardize source records.
    2. Run normal data validation.
    3. Check identities across the complete source file.
    4. Check identities against CORE history.
    5. Quarantine invalid and conflicting records.
    6. Load only eligible records into RAW.
    7. Process RAW -> STG -> CORE.
    """

    source_file = (
        Path(file_path)
        if file_path
        else PROJECT_ROOT / "data" / "sample" / source_config["file_name"]
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
        default_country_code=source_config.get("default_country_code"),
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

        # Step 3: Retrieve identities using ALL incoming rows,
        # including rows that failed normal validation.
        existing_identities = get_existing_member_identities(
            connection,
            all_records_df["member_name"].tolist()
            if "member_name" in all_records_df.columns else []
        )

        # Step 4: Validate identities across the COMPLETE source file.
        identity_result = validate_member_identity_records(
            all_records_df,
            existing_identities
        )

        identity_rejected_df = identity_result["rejected_records"]

        identity_conflict_count = len(identity_rejected_df)

        # Identify conflicts by original source row number.
        # This prevents duplicate quarantine records.
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
        # Rows already rejected by normal validation are updated,
        # not appended a second time.
        identity_only_rejects = []

        rejected_row_numbers = set(
            rejected_df["_source_row_number"].tolist()
        ) if not rejected_df.empty else set()

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
            identity_only_df = pd.DataFrame(identity_only_rejects)

            rejected_df = pd.concat(
                [rejected_df, identity_only_df],
                ignore_index=True
            )

        print("\nMember validation results:")
        print("Total records:", ingestion_result["total_records"])
        print("Valid records:", len(valid_df))
        print("Rejected records:", len(rejected_df))
        print("Identity conflicts:", identity_conflict_count)

        # Step 6: Load eligible records into RAW and all rejects into OPS.
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

    # Step 7: Process RAW -> STG -> CORE.
    # This is still called when all rows are rejected or the file
    # is skipped as a duplicate, matching the existing workflow.
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
# 4. RUN REDEMPTION SOURCE
# --------------------------------------------------

def run_redemption_source(source_config, file_path=None):
    """
    Execute one configured redemption JSON source.
    """

    source_file = (
        Path(file_path)
        if file_path
        else PROJECT_ROOT / "data" / "raw" / source_config["file_name"]
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

    processing_result = run_redemption_processing()

    return {
        "raw_load": load_result,
        "transformation": processing_result,
        "status": "SUCCESS"
    }


# --------------------------------------------------
# 5. RUN ALL ENABLED SOURCES
# --------------------------------------------------

def run_all_member_sources():
    """Run all enabled member and redemption sources."""

    config = load_source_config()

    for source in config["sources"]:

        if not source.get("enabled", False):
            print(
                "\nSkipping disabled source:",
                source.get("source_id")
            )
            continue

        if source["entity"] == "MEMBER_PROFILE":
            run_member_source(source)

        elif source["entity"] == "REDEMPTION":
            run_redemption_source(source)

        else:
            print(
                "\nSkipping unsupported source entity:",
                source["entity"]
            )


# --------------------------------------------------
# 6. MAIN ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":
    run_all_member_sources()
