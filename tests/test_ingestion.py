
from pathlib import Path

from python.ingestion.file_reader import read_file
from python.ingestion.header_mapper import standardize_headers


PROJECT_ROOT = Path(__file__).resolve().parents[1]

CSV_FILE = PROJECT_ROOT / "data/sample/IND.csv"
MAPPING_FILE = PROJECT_ROOT / "config/column_mapping.json"


def test_csv_reader_and_header_mapper():

    # Step 1: Read source CSV
    df = read_file(CSV_FILE)

    print("Original columns:")
    print(df.columns.tolist())

    # Step 2: Standardize headers
    standardized_df, missing_columns = standardize_headers(
        df,
        MAPPING_FILE
    )

    print("\nStandardized columns:")
    print(standardized_df.columns.tolist())

    # Step 3: Expected assessment columns
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

    # Step 4: Validate all expected columns
    for column in expected_columns:
        assert column in standardized_df.columns, (
            f"Missing expected column: {column}"
        )

    # Step 5: Validate record count
    assert len(standardized_df) == 4

    # Step 6: Validate member IDs
    assert standardized_df["member_id"].notna().all()

    # Step 7: Validate nullable last flight date
    assert standardized_df["last_flight_date"].isna().sum() == 1

    # Step 8: Display missing optional columns
    print("\nMissing standard columns:")
    print(missing_columns)

    print("\nAll 11 assessment columns are present.")
    print("All ingestion tests passed!")


if __name__ == "__main__":
    test_csv_reader_and_header_mapper()
