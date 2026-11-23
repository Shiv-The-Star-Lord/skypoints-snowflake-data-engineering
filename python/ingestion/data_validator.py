
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


def _normalize_member_name(value):
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def _normalize_member_id(value):
    if pd.isna(value):
        return ""

    if isinstance(value, float) and value.is_integer():
        return str(int(value))

    return str(value).strip()


def validate_member_records(df):
    """
    Validate member records individually.

    Returns valid records, rejected records and validation errors.
    """

    df = df.copy()

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
            format="mixed",
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


def validate_member_identity_records(df, existing_identities):
    """
    Detect identity conflicts across the complete incoming source file.

    Rules:
    1. Same name + same ID: allowed.
    2. Same name + different IDs in the incoming file:
       reject every identifiable row for that name.
    3. Same name + different ID in CORE:
       reject the incoming row.
    4. Multiple IDs in CORE history for the same name:
       reject the incoming row until the historical conflict is resolved.

    Existing validation_errors are preserved and identity errors
    are added rather than replacing them.
    """

    df = df.copy()

    if df.empty:
        return {
            "valid_records": df.copy(),
            "rejected_records": df.copy(),
            "errors": [],
            "is_valid": True
        }

    required = ["member_name", "member_id"]
    missing = [column for column in required if column not in df.columns]

    if missing:
        raise ValueError(
            f"Identity validation requires columns: {missing}"
        )

    def normalize_name(value):
        if pd.isna(value):
            return ""
        return str(value).strip().upper()

    def normalize_id(value):
        if pd.isna(value):
            return ""

        if isinstance(value, float) and value.is_integer():
            return str(int(value))

        return str(value).strip()

    normalized_existing = {}

    for name, member_ids in existing_identities.items():
        normalized_name = normalize_name(name)

        if isinstance(member_ids, (set, list, tuple)):
            values = member_ids
        else:
            values = [member_ids]

        normalized_ids = {
            normalize_id(value)
            for value in values
            if normalize_id(value)
        }

        if normalized_name:
            normalized_existing.setdefault(
                normalized_name, set()
            ).update(normalized_ids)

    working = df.copy()

    working["_identity_name"] = (
        working["member_name"].map(normalize_name)
    )
    working["_identity_id"] = (
        working["member_id"].map(normalize_id)
    )

    # Only rows with both identity fields populated participate
    # in identity comparisons. Missing fields remain normal DQ errors.
    identifiable = working[
        (working["_identity_name"] != "")
        & (working["_identity_id"] != "")
    ]

    batch_identity_counts = (
        identifiable.groupby("_identity_name")["_identity_id"]
        .nunique()
    )

    conflicting_batch_names = set(
        batch_identity_counts[
            batch_identity_counts > 1
        ].index
    )

    rejected_errors = {}
    errors = []

    for index, row in working.iterrows():
        name = row["_identity_name"]
        member_id = row["_identity_id"]

        # No identity comparison is possible if either field is missing.
        if not name or not member_id:
            continue

        existing_ids = normalized_existing.get(name, set())
        conflict_message = None

        if name in conflicting_batch_names:
            conflict_message = (
                f"Multiple different member IDs were found for "
                f"member name '{name}' in the same incoming batch. "
                f"All identifiable records for this name are rejected."
            )

        elif len(existing_ids) > 1:
            conflict_message = (
                f"CORE history contains multiple member IDs for "
                f"member name '{name}'. Identity requires investigation."
            )

        elif existing_ids and member_id not in existing_ids:
            existing_id = next(iter(existing_ids))

            conflict_message = (
                f"Member name '{name}' already exists in CORE "
                f"with member ID '{existing_id}', but the incoming "
                f"record has member ID '{member_id}'."
            )

        if conflict_message:
            identity_error = {
                "check": "IDENTITY_CONFLICT",
                "column": "member_id",
                "message": conflict_message
            }

            rejected_errors[index] = [identity_error]

            errors.append({
                "row_index": index,
                **identity_error
            })

    rejected_indexes = list(rejected_errors.keys())

    valid_records = df.drop(
        index=rejected_indexes
    ).copy()

    rejected_records = df.loc[
        rejected_indexes
    ].copy()

    if not rejected_records.empty:
        if "validation_errors" in rejected_records.columns:
            merged_errors = []

            for index in rejected_indexes:
                existing_errors = rejected_records.at[
                    index, "validation_errors"
                ]

                if not isinstance(existing_errors, list):
                    existing_errors = []

                merged_errors.append(
                    existing_errors + rejected_errors[index]
                )

            rejected_records["validation_errors"] = merged_errors

        else:
            rejected_records["validation_errors"] = [
                rejected_errors[index]
                for index in rejected_indexes
            ]

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
