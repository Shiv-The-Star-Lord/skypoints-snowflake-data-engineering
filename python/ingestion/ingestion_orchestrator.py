
import json
from pathlib import Path

from python.transformations.member_processor import (
    run_member_processing
)

from python.transformations.redemption_processor import (
    run_redemption_processing
)

from python.ingestion.ingestion_pipeline import (
    run_member_ingestion,
    validate_before_loading
)

from python.ingestion.file_reader import read_file
from python.ingestion.data_validator import validate_redemption_data

from python.snowflake_loader import (
    get_snowflake_connection,
    load_member_data,
    load_redemption_data
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONFIG_PATH = PROJECT_ROOT / "config" / "source_config.json"
MAPPING_PATH = PROJECT_ROOT / "config" / "column_mapping.json"


def load_source_config():
    """Load source definitions from JSON configuration."""

    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        return json.load(file)


def run_member_source(source_config, file_path=None):
    """
    Execute one configured member source.

    1. Read and validate source file.
    2. Load records into RAW with audit tracking.
    3. Transform RAW into STG.
    4. Process STG into CORE using SCD Type 2.
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

    # Step 1: Read and validate member source.
    ingestion_result = run_member_ingestion(
        file_path=source_file,
        mapping_path=MAPPING_PATH,
        file_format=source_config["format"]
    )

    validated_df = validate_before_loading(ingestion_result)

    # Step 2: Load validated records into RAW.
    connection = None

    try:
        connection = get_snowflake_connection()

        load_result = load_member_data(
            connection=connection,
            df=validated_df,
            file_path=source_file,
            source_id=source_config["source_id"],
            entity_type=source_config["entity"]
        )

    finally:
        if connection:
            connection.close()

    print("\nRAW ingestion result:", load_result)

    # Step 3 and 4: Process RAW -> STG -> CORE.
    # Run even when RAW ingestion was skipped as a duplicate.
    processing_result = run_member_processing()

    return {
        "raw_load": load_result,
        "transformation": processing_result,
        "status": "SUCCESS"
    }


def run_redemption_source(source_config, file_path=None):
    """
    Execute one configured redemption JSON source.

    1. Read nested JSON.
    2. Validate redemption data.
    3. Load into RAW_REDEMPTIONS with audit tracking.
    4. Merge RAW into STG.
    5. Merge STG into CORE.
    """

    source_file = (
        Path(file_path)
        if file_path
        else PROJECT_ROOT / "data" / "raw" / source_config["file_name"]
    )

    if not source_file.exists():
        raise FileNotFoundError(
            f"Redemption source file not found: {source_file}"
        )

    print("\nStarting redemption ingestion")
    print("Source ID:", source_config["source_id"])
    print("Source file:", source_file.name)

    # Step 1: Read and validate JSON.
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

    # Step 2: Load into RAW.
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

    # Step 3 and 4: Process RAW -> STG -> CORE.
    # Run even when RAW ingestion was skipped as a duplicate.
    processing_result = run_redemption_processing()

    return {
        "raw_load": load_result,
        "transformation": processing_result,
        "status": "SUCCESS"
    }


def run_all_member_sources():
    """Run all enabled member and redemption sources."""

    config = load_source_config()

    for source in config["sources"]:

        if not source.get("enabled", False):
            continue

        if source["entity"] == "MEMBER_PROFILE":
            run_member_source(source)

        elif source["entity"] == "REDEMPTION":
            run_redemption_source(source)


if __name__ == "__main__":
    run_all_member_sources()
