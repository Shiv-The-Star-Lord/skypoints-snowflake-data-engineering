
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from python.ingestion.file_reader import read_file
from python.ingestion.header_mapper import standardize_headers
from python.ingestion.data_validator import (
    validate_member_data,
    validate_member_identity_records
)


CSV_FILE = PROJECT_ROOT / "data" / "sample" / "IND.csv"
MAPPING_FILE = PROJECT_ROOT / "config" / "column_mapping.json"


# --------------------------------------------------
# HELPER FUNCTION
# --------------------------------------------------

def load_standardized_data():
    df = read_file(CSV_FILE)

    standardized_df, missing_columns = standardize_headers(
        df,
        MAPPING_FILE
    )

    return standardized_df


# --------------------------------------------------
# NORMAL MEMBER VALIDATION TESTS
# --------------------------------------------------

def test_valid_member_data():

    df = load_standardized_data()

    result = validate_member_data(df)

    print("\nTest 1: Valid member data")
    print(result)

    assert result["is_valid"] is True
    assert len(result["errors"]) == 0


def test_duplicate_member_ids():

    df = load_standardized_data()

    # Add a duplicate member record.
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

    # Make one member ID blank.
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

    # Introduce an invalid enrollment date.
    df.loc[0, "enrollment_date"] = "invalid-date"

    result = validate_member_data(df)

    print("\nTest 4: Invalid date")
    print(result)

    assert result["is_valid"] is False
    assert any(
        error["check"] == "INVALID_DATE"
        for error in result["errors"]
    )


# --------------------------------------------------
# MEMBER IDENTITY VALIDATION TESTS
# --------------------------------------------------

def test_same_name_same_id_is_allowed():

    df = pd.DataFrame([
        {"member_name": "Mike", "member_id": "3"}
    ])

    result = validate_member_identity_records(
        df,
        {"MIKE": {"3"}}
    )

    assert len(result["valid_records"]) == 1
    assert len(result["rejected_records"]) == 0


def test_same_name_different_core_id_is_rejected():

    df = pd.DataFrame([
        {"member_name": "Mike", "member_id": "1"}
    ])

    result = validate_member_identity_records(
        df,
        {"MIKE": {"3"}}
    )

    assert len(result["valid_records"]) == 0
    assert len(result["rejected_records"]) == 1

    assert result["errors"][0]["check"] == "IDENTITY_CONFLICT"


def test_same_name_different_ids_in_batch_are_rejected():

    df = pd.DataFrame([
        {"member_name": "Mike", "member_id": "1"},
        {"member_name": "Mike", "member_id": "3"}
    ])

    result = validate_member_identity_records(
        df,
        {}
    )

    assert len(result["valid_records"]) == 0
    assert len(result["rejected_records"]) == 2
    assert len(result["errors"]) == 2


def test_multiple_ids_in_core_history_are_rejected():

    df = pd.DataFrame([
        {"member_name": "Mike", "member_id": "3"}
    ])

    result = validate_member_identity_records(
        df,
        {"MIKE": {"1", "3"}}
    )

    assert len(result["valid_records"]) == 0
    assert len(result["rejected_records"]) == 1

    assert result["errors"][0]["check"] == "IDENTITY_CONFLICT"


def test_identity_conflict_preserves_existing_validation_errors():

    df = pd.DataFrame([
        {
            "member_name": "Mike",
            "member_id": "1",
            "_source_row_number": 1,
            "validation_errors": [
                {
                    "check": "INVALID_DATE",
                    "column": "date_of_birth",
                    "message": "Invalid date value."
                }
            ]
        },
        {
            "member_name": "Mike",
            "member_id": "3",
            "_source_row_number": 2,
            "validation_errors": []
        }
    ])

    result = validate_member_identity_records(
        df,
        {}
    )

    rejected = result["rejected_records"]

    # Both records must be rejected.
    assert len(result["valid_records"]) == 0
    assert len(rejected) == 2

    # Original validation error must remain on row 1.
    row_1_errors = rejected.loc[
        rejected["_source_row_number"] == 1,
        "validation_errors"
    ].iloc[0]

    assert any(
        error["check"] == "INVALID_DATE"
        for error in row_1_errors
    )

    assert any(
        error["check"] == "IDENTITY_CONFLICT"
        for error in row_1_errors
    )

    # Row 2 must also have an identity-conflict error.
    row_2_errors = rejected.loc[
        rejected["_source_row_number"] == 2,
        "validation_errors"
    ].iloc[0]

    assert any(
        error["check"] == "IDENTITY_CONFLICT"
        for error in row_2_errors
    )


# --------------------------------------------------
# MANUAL EXECUTION
# --------------------------------------------------

if __name__ == "__main__":

    test_valid_member_data()
    test_duplicate_member_ids()
    test_missing_mandatory_value()
    test_invalid_date()

    test_same_name_same_id_is_allowed()
    test_same_name_different_core_id_is_rejected()
    test_same_name_different_ids_in_batch_are_rejected()
    test_multiple_ids_in_core_history_are_rejected()
    test_identity_conflict_preserves_existing_validation_errors()

    print("\nAll data validation tests passed!")
