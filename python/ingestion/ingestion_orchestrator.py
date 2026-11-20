
import json
from pathlib import Path

from python.ingestion.ingestion_pipeline import (
    run_member_ingestion,
    validate_before_loading
)

from python.snowflake_loader import (
    get_snowflake_connection,
    load_member_data
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
    Execute one configured member source:

    1. Read and validate source file.
    2. Connect to Snowflake.
    3. Load records into RAW with audit tracking.

    An optional file_path allows testing with a separate file.
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

    ingestion_result = run_member_ingestion(
        file_path=source_file,
        mapping_path=MAPPING_PATH,
        file_format=source_config["format"]
    )

    validated_df = validate_before_loading(ingestion_result)

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

        return load_result

    finally:
        if connection:
            connection.close()


def run_all_member_sources():
    """Run all enabled member profile sources."""

    config = load_source_config()

    for source in config["sources"]:

        if not source.get("enabled", False):
            continue

        if source["entity"] != "MEMBER_PROFILE":
            continue

        run_member_source(source)


if __name__ == "__main__":
    run_all_member_sources()
