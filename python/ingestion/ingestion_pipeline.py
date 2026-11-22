
from pathlib import Path

import pandas as pd

from python.ingestion.file_reader import read_file
from python.ingestion.header_mapper import (
    standardize_headers,
    reorder_standard_columns
)
from python.ingestion.data_validator import validate_member_records


def run_member_ingestion(
    file_path,
    mapping_path,
    file_format=None,
    default_country_code=None,
    date_format=None
):
    """
    Read, standardize and validate member records.

    Valid records are returned for RAW ingestion.
    Invalid records are returned separately for quarantine.
    """

    file_path = Path(file_path)

    print("\nStarting Member Ingestion")
    print("-------------------------")
    print("Source file:", file_path.name)

    # STEP 1: READ SOURCE FILE
    df = read_file(
        file_path,
        file_format
    )

    if not isinstance(df, pd.DataFrame):
        raise ValueError(
            "Member source file must contain tabular data."
        )

    df = df.copy()

    # STEP 2: PRESERVE SOURCE ROW NUMBERS
    if "_source_row_number" in df.columns:
        raise ValueError(
            "Source file contains reserved column "
            "'_source_row_number'."
        )

    df["_source_row_number"] = range(1, len(df) + 1)

    # STEP 3: STANDARDIZE COLUMN HEADERS
    df, missing_columns = standardize_headers(
        df,
        mapping_path
    )

    # STEP 4: APPLY DEFAULT COUNTRY CODE
    if default_country_code:

        if "country_code" not in df.columns:
            df["country_code"] = default_country_code

        else:
            df["country_code"] = (
                df["country_code"]
                .fillna(default_country_code)
                .replace("", default_country_code)
            )

    # STEP 5: APPLY SOURCE DATE FORMAT
    if date_format:

        date_columns = [
            "date_of_birth",
            "enrollment_date",
            "flight_date",
            "last_flight_date"
        ]

        for column in date_columns:

            if column in df.columns:

                original_values = df[column].copy()

                parsed_values = pd.to_datetime(
                    original_values,
                    format=date_format,
                    errors="coerce"
                )

                # Preserve invalid original values for validation
                # and quarantine instead of silently replacing them.
                df[column] = parsed_values.where(
                    parsed_values.notna(),
                    original_values
                )

    # STEP 6: REORDER STANDARD COLUMNS
    df = reorder_standard_columns(df)

    # STEP 7: VALIDATE RECORDS INDIVIDUALLY
    validation_result = validate_member_records(df)

    valid_data = validation_result["valid_records"].copy()
    rejected_data = validation_result["rejected_records"].copy()

    # STEP 8: BUILD RESULT
    result = {
        "source_file": file_path.name,
        "total_records": len(df),
        "valid_records": len(valid_data),
        "rejected_records": len(rejected_data),
        "missing_standard_columns": missing_columns,
        "is_valid": len(rejected_data) == 0,
        "errors": validation_result["errors"],
        "data": df,
        "valid_data": valid_data,
        "rejected_data": rejected_data
    }

    # STEP 9: DISPLAY SUMMARY
    print("\nIngestion Summary")
    print("-----------------")
    print("Source file:", result["source_file"])
    print("Total records:", result["total_records"])
    print("Valid records:", result["valid_records"])
    print("Rejected records:", result["rejected_records"])
    print("Validation errors:", len(result["errors"]))
    print("Missing standard columns:", missing_columns)

    return result


def validate_before_loading(ingestion_result):
    """
    Return valid records for RAW loading.

    New result format:
        Return valid_data and leave rejected rows for quarantine.

    Legacy result format:
        Abort loading when validation fails.
    """

    # Support the legacy result format.
    if "valid_data" not in ingestion_result:

        if not ingestion_result.get("is_valid", False):

            error_messages = [
                error.get("message", "Unknown validation error")
                for error in ingestion_result.get("errors", [])
            ]

            raise ValueError(
                "Loading aborted due to validation errors: "
                + "; ".join(error_messages)
            )

        return ingestion_result.get("data")

    # Handle the new row-level quarantine format.
    valid_data = ingestion_result["valid_data"]
    rejected_data = ingestion_result["rejected_data"]

    print(
        f"Validation split complete. "
        f"{len(valid_data)} valid records ready for RAW loading; "
        f"{len(rejected_data)} records require quarantine."
    )

    return valid_data
