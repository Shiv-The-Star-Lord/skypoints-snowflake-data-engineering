
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from python.ingestion.file_reader import read_file
from python.ingestion.header_mapper import standardize_headers
from python.ingestion.data_validator import validate_member_data


CSV_FILE = PROJECT_ROOT / "data/sample/IND.csv"
MAPPING_FILE = PROJECT_ROOT / "config/column_mapping.json"


def load_standardized_data():
    df = read_file(CSV_FILE)

    standardized_df, missing_columns = standardize_headers(
        df,
        MAPPING_FILE
    )

    return standardized_df


def test_valid_member_data():

    df = load_standardized_data()

    result = validate_member_data(df)

    print("\nTest 1: Valid member data")
    print(result)

    assert result["is_valid"] is True
    assert len(result["errors"]) == 0


def test_duplicate_member_ids():

    df = load_standardized_data()

    # Add a duplicate member record
    df.loc[len(df)] = df.iloc[0]

    result = validate_member_data(df)

    print("\nTest 2: Duplicate member IDs")
    print(result)

    assert result["is_valid"] is False
    assert any(
        error["check"] == "DUPLICATE_MEMBER_ID"
        for error in result["errors"]
    )


def test_missing_mandatory_value():

    df = load_standardized_data()

    # Make one member ID blank
    df.loc[0, "member_id"] = ""

    result = validate_member_data(df)

    print("\nTest 3: Missing mandatory value")
    print(result)

    assert result["is_valid"] is False
    assert any(
        error["check"] == "MANDATORY_VALUES"
        for error in result["errors"]
    )


def test_invalid_date():

    df = load_standardized_data()

    # Introduce an invalid enrollment date
    df.loc[0, "enrollment_date"] = "invalid-date"

    result = validate_member_data(df)

    print("\nTest 4: Invalid date")
    print(result)

    assert result["is_valid"] is False
    assert any(
        error["check"] == "INVALID_DATE"
        for error in result["errors"]
    )


if __name__ == "__main__":

    test_valid_member_data()
    test_duplicate_member_ids()
    test_missing_mandatory_value()
    test_invalid_date()

    print("\nAll data validation tests passed!")
