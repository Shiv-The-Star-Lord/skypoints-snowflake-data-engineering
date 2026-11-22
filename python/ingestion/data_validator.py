
import pandas as pd


REQUIRED_COLUMNS = [
    "member_id",
    "member_name",
    "enrollment_date",
    "country_code"
]

DATE_COLUMNS = [
    "date_of_birth",
    "enrollment_date",
    "last_flight_date"
]


def validate_member_records(df):
    """
    Validate member records individually.

    Returns:
        valid_records: DataFrame containing valid records.
        rejected_records: DataFrame containing invalid records,
                          including their validation errors.
        errors: List of validation errors.
    """

    df = df.copy()

    # Schema-level validation: processing cannot continue
    # if mandatory columns are completely absent.
    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing mandatory columns: {missing_columns}"
        )

    row_errors = {
        index: []
        for index in df.index
    }

    # 1. Mandatory field validation
    for column in REQUIRED_COLUMNS:

        invalid_mask = (
            df[column].isna() |
            df[column].astype(str).str.strip().eq("")
        )

        for index in df.index[invalid_mask]:
            row_errors[index].append({
                "check": "MANDATORY_VALUES",
                "column": column,
                "message": f"{column} contains a null or blank value"
            })

    # 2. Duplicate member ID validation
    duplicate_mask = df["member_id"].duplicated(keep=False)

    for index in df.index[duplicate_mask]:
        row_errors[index].append({
            "check": "DUPLICATE_MEMBER_ID",
            "column": "member_id",
            "message": "Duplicate member ID found in source file"
        })

    # 3. Date validation
    for column in DATE_COLUMNS:

        if column not in df.columns:
            continue

        non_blank = (
            df[column].notna() &
            df[column].astype(str).str.strip().ne("")
        )

        parsed_dates = pd.to_datetime(
            df.loc[non_blank, column],
            errors="coerce"
        )

        invalid_indexes = parsed_dates[parsed_dates.isna()].index

        for index in invalid_indexes:
            row_errors[index].append({
                "check": "INVALID_DATE",
                "column": column,
                "message": f"{column} contains an invalid date"
            })

    # 4. Split valid and rejected records
    rejected_indexes = [
        index
        for index, errors in row_errors.items()
        if errors
    ]

    valid_indexes = [
        index
        for index, errors in row_errors.items()
        if not errors
    ]

    valid_records = df.loc[valid_indexes].copy()
    rejected_records = df.loc[rejected_indexes].copy()

    rejected_records["validation_errors"] = [
        row_errors[index]
        for index in rejected_indexes
    ]

    # 5. Build error summary
    errors = []

    for index in rejected_indexes:
        for error in row_errors[index]:
            errors.append({
                "row_index": index,
                **error
            })

    return {
        "valid_records": valid_records,
        "rejected_records": rejected_records,
        "errors": errors,
        "is_valid": len(rejected_records) == 0
    }


def validate_member_data(df):
    """
    Backward-compatible validation summary.
    """

    result = validate_member_records(df)

    return {
        "is_valid": result["is_valid"],
        "errors": result["errors"]
    }


def validate_redemption_data(data):
    """
    Validate nested JSON redemption feed data.
    """

    errors = []

    if not isinstance(data, dict):
        return {
            "is_valid": False,
            "errors": ["Redemption feed must be a JSON object."]
        }

    required_fields = [
        "member_id",
        "feed_date",
        "redemptions"
    ]

    for field in required_fields:
        if field not in data or data[field] is None:
            errors.append(f"Missing mandatory field: {field}")

    if errors:
        return {
            "is_valid": False,
            "errors": errors
        }

    if not str(data["member_id"]).strip():
        errors.append("member_id cannot be blank.")

    try:
        pd.to_datetime(
            str(data["feed_date"]),
            format="%Y%m%d",
            errors="raise"
        )
    except (ValueError, TypeError):
        errors.append("feed_date must use YYYYMMDD format.")

    redemptions = data["redemptions"]

    if not isinstance(redemptions, list):
        errors.append("redemptions must be a JSON array.")

    else:
        transaction_fields = [
            "txn_id",
            "txn_date",
            "partner",
            "miles_redeemed",
            "status"
        ]

        for index, transaction in enumerate(redemptions):

            if not isinstance(transaction, dict):
                errors.append(
                    f"Redemption at index {index} must be a JSON object."
                )
                continue

            for field in transaction_fields:
                if (
                    field not in transaction
                    or transaction[field] is None
                    or str(transaction[field]).strip() == ""
                ):
                    errors.append(
                        f"Redemption at index {index}: "
                        f"missing or blank {field}."
                    )

            if "txn_date" in transaction:
                try:
                    pd.to_datetime(
                        str(transaction["txn_date"]),
                        format="%Y%m%d",
                        errors="raise"
                    )
                except (ValueError, TypeError):
                    errors.append(
                        f"Redemption at index {index}: "
                        "txn_date must use YYYYMMDD format."
                    )

            if "miles_redeemed" in transaction:
                try:
                    miles = float(transaction["miles_redeemed"])

                    if miles <= 0:
                        raise ValueError

                except (ValueError, TypeError):
                    errors.append(
                        f"Redemption at index {index}: "
                        "miles_redeemed must be a positive number."
                    )

    return {
        "is_valid": len(errors) == 0,
        "errors": errors
    }
