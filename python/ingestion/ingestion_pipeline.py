
from pathlib import Path

import pandas as pd

from python.ingestion.file_reader import read_file
from python.ingestion.header_mapper import standardize_headers
from python.ingestion.data_validator import validate_member_data


def run_member_ingestion(file_path, mapping_path, file_format=None):
    """
    Execute the member ingestion workflow.

    Steps:
        1. Read source file.
        2. Standardize column names.
        3. Validate member records.
        4. Return processed data and validation results.
    """

    file_path = Path(file_path)
    mapping_path = Path(mapping_path)

    # Step 1: Read source file
    print(f"\nReading source file: {file_path.name}")

    df = read_file(file_path, file_format)

    if not isinstance(df, pd.DataFrame):
        raise TypeError(
            "Member ingestion expects tabular data, "
            "but the source did not return a DataFrame."
        )

    print(f"Records read: {len(df)}")

    # Step 2: Standardize column names
    print("\nStandardizing column names...")

    standardized_df, missing_columns = standardize_headers(
        df,
        mapping_path
    )

    print("Header standardization completed.")

    # Step 3: Validate member records
    print("\nValidating member records...")

    validation_result = validate_member_data(standardized_df)

    # Step 4: Prepare ingestion result
    result = {
        "source_file": file_path.name,
        "total_records": len(standardized_df),
        "missing_standard_columns": missing_columns,
        "is_valid": validation_result["is_valid"],
        "errors": validation_result["errors"],
        "data": standardized_df
    }

    print("\nIngestion Summary")
    print("-----------------")
    print(f"Source file: {result['source_file']}")
    print(f"Total records: {result['total_records']}")
    print(f"Valid: {result['is_valid']}")
    print(f"Validation errors: {len(result['errors'])}")
    print(f"Missing standard columns: {missing_columns}")

    return result
