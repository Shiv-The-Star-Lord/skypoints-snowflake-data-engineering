
import pandas as pd


# Mandatory fields for member profile records
REQUIRED_COLUMNS = [
    "member_id",
    "member_name",
    "enrollment_date",
    "country_code"
]

# Date columns that need validation
DATE_COLUMNS = [
    "date_of_birth",
    "enrollment_date",
    "last_flight_date"
]


def validate_member_data(df):
    """
    Validate standardized member profile data.

    Returns:
        A dictionary containing validation status and errors.
    """

    errors = []

    # Step 1: Check mandatory columns exist
    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        errors.append({
            "check": "MANDATORY_COLUMNS",
            "message": f"Missing columns: {missing_columns}"
        })

        return {
            "is_valid": False,
            "errors": errors
        }

    # Step 2: Check mandatory fields are not null or blank
    for column in REQUIRED_COLUMNS:

        invalid_rows = df[
            df[column].isna() |
            df[column].astype(str).str.strip().eq("")
        ]

        if not invalid_rows.empty:
            errors.append({
                "check": "MANDATORY_VALUES",
                "column": column,
                "row_count": len(invalid_rows),
                "message": f"{column} contains null or blank values"
            })

    # Step 3: Check duplicate member IDs
    duplicate_rows = df[
        df["member_id"].duplicated(keep=False)
    ]

    if not duplicate_rows.empty:
        errors.append({
            "check": "DUPLICATE_MEMBER_ID",
            "row_count": len(duplicate_rows),
            "message": "Duplicate member IDs found"
        })

    # Step 4: Check date formats
    for column in DATE_COLUMNS:

        if column not in df.columns:
            continue

        # Ignore genuinely blank values.
        non_blank = (
            df[column].notna() &
            df[column].astype(str).str.strip().ne("")
        )

        parsed_dates = pd.to_datetime(
            df.loc[non_blank, column],
            errors="coerce"
        )

        invalid_dates = parsed_dates[parsed_dates.isna()]

        if not invalid_dates.empty:
            errors.append({
                "check": "INVALID_DATE",
                "column": column,
                "row_count": len(invalid_dates),
                "message": f"{column} contains invalid dates"
            })

    return {
        "is_valid": len(errors) == 0,
        "errors": errors
    }
