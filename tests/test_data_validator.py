
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

CSV_FILE = PROJECT_ROOT / "data" / "sample" / "members" / "IND.csv"
MAPPING_FILE = PROJECT_ROOT / "config" / "column_mapping.json"


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

    assert result["is_valid"] is True
    assert len(result["errors"]) == 0


def test_duplicate_member_ids():
    df = load_standardized_data()

    # Add an exact duplicate composite identity.
    df.loc[len(df)] = df.iloc[0]

    result = validate_member_data(df)

    assert result["is_valid"] is False
    assert any(
        error["check"] == "DUPLICATE_MEMBER_IDENTITY"
        for error in result["errors"]
    )


def test_missing_mandatory_value():
    df = load_standardized_data()
    df.loc[0, "member_id"] = ""

    result = validate_member_data(df)

    assert result["is_valid"] is False
    assert any(
        error["check"] == "MANDATORY_VALUES"
        for error in result["errors"]
    )


def test_invalid_date():
    df = load_standardized_data()
    df.loc[0, "enrollment_date"] = "invalid-date"

    result = validate_member_data(df)

    assert result["is_valid"] is False
    assert any(
        error["check"] == "INVALID_DATE"
        for error in result["errors"]
    )


# --------------------------------------------------
# COMPOSITE IDENTITY TESTS
# --------------------------------------------------

def test_same_composite_identity_already_in_core_is_allowed():
    df = pd.DataFrame([
        {
            "member_name": "Mike",
            "country_code": "IND",
            "member_id": "3"
        }
    ])

    result = validate_member_identity_records(
        df,
        {("IND", "3")}
    )

    assert len(result["valid_records"]) == 1
    assert len(result["rejected_records"]) == 0


def test_same_member_id_in_different_countries_is_allowed():
    df = pd.DataFrame([
        {
            "member_name": "Mike",
            "country_code": "IND",
            "member_id": "1"
        },
        {
            "member_name": "Sam",
            "country_code": "USA",
            "member_id": "1"
        }
    ])

    result = validate_member_identity_records(df, set())

    assert len(result["valid_records"]) == 2
    assert len(result["rejected_records"]) == 0


def test_same_name_with_different_ids_is_allowed():
    df = pd.DataFrame([
        {
            "member_name": "Mike",
            "country_code": "IND",
            "member_id": "1"
        },
        {
            "member_name": "Mike",
            "country_code": "IND",
            "member_id": "3"
        }
    ])

    result = validate_member_identity_records(df, set())

    assert len(result["valid_records"]) == 2
    assert len(result["rejected_records"]) == 0


def test_duplicate_composite_identity_in_batch_is_rejected():
    df = pd.DataFrame([
        {
            "member_name": "Mike",
            "country_code": "IND",
            "member_id": "3"
        },
        {
            "member_name": "Sam",
            "country_code": "IND",
            "member_id": "3"
        }
    ])

    result = validate_member_identity_records(df, set())

    assert len(result["valid_records"]) == 0
    assert len(result["rejected_records"]) == 2
    assert len(result["errors"]) == 2

    assert all(
        error["check"] == "DUPLICATE_MEMBER_IDENTITY"
        for error in result["errors"]
    )


def test_multiple_ids_in_core_history_are_allowed():
    df = pd.DataFrame([
        {
            "member_name": "Mike",
            "country_code": "IND",
            "member_id": "3"
        }
    ])

    # CORE can contain multiple historical identities
    # for a country. An exact matching identity is allowed.
    result = validate_member_identity_records(
        df,
        {
            ("IND", "1"),
            ("IND", "3")
        }
    )

    assert len(result["valid_records"]) == 1
    assert len(result["rejected_records"]) == 0


def test_duplicate_identity_preserves_existing_validation_errors():
    df = pd.DataFrame([
        {
            "member_name": "Mike",
            "country_code": "IND",
            "member_id": "3",
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
            "member_name": "Sam",
            "country_code": "IND",
            "member_id": "3",
            "_source_row_number": 2,
            "validation_errors": []
        }
    ])

    result = validate_member_identity_records(df, set())

    rejected = result["rejected_records"]

    assert len(result["valid_records"]) == 0
    assert len(rejected) == 2

    row_1_errors = rejected.loc[
        rejected["_source_row_number"] == 1,
        "validation_errors"
    ].iloc[0]

    row_2_errors = rejected.loc[
        rejected["_source_row_number"] == 2,
        "validation_errors"
    ].iloc[0]

    assert any(
        error["check"] == "INVALID_DATE"
        for error in row_1_errors
    )

    assert any(
        error["check"] == "DUPLICATE_MEMBER_IDENTITY"
        for error in row_1_errors
    )

    assert any(
        error["check"] == "DUPLICATE_MEMBER_IDENTITY"
        for error in row_2_errors
    )


if __name__ == "__main__":
    test_valid_member_data()
    test_duplicate_member_ids()
    test_missing_mandatory_value()
    test_invalid_date()

    test_same_composite_identity_already_in_core_is_allowed()
    test_same_member_id_in_different_countries_is_allowed()
    test_same_name_with_different_ids_is_allowed()
    test_duplicate_composite_identity_in_batch_is_rejected()
    test_multiple_ids_in_core_history_are_allowed()
    test_duplicate_identity_preserves_existing_validation_errors()

    print("\nAll data validation tests passed!")
