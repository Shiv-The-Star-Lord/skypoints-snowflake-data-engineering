
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from python.ingestion.ingestion_pipeline import run_member_ingestion


CSV_FILE = PROJECT_ROOT / "data/sample/IND.csv"
MAPPING_FILE = PROJECT_ROOT / "config/column_mapping.json"


def test_member_ingestion_pipeline():

    result = run_member_ingestion(
        file_path=CSV_FILE,
        mapping_path=MAPPING_FILE
    )

    # Test 1: Source file
    assert result["source_file"] == "IND.csv"

    # Test 2: Record count
    assert result["total_records"] == 4

    # Test 3: Validation status
    assert result["is_valid"] is True

    # Test 4: No validation errors
    assert len(result["errors"]) == 0

    # Test 5: Standardized columns
    expected_columns = [
        "member_name",
        "member_id",
        "enrollment_date",
        "last_flight_date",
        "tier_code",
        "agent_name",
        "state_code",
        "country_code",
        "postal_code",
        "date_of_birth",
        "is_active"
    ]

    for column in expected_columns:
        assert column in result["data"].columns

    print("\nAll ingestion pipeline tests passed!")


if __name__ == "__main__":
    test_member_ingestion_pipeline()
