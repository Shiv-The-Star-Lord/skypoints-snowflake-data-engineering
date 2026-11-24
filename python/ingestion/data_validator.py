
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


def _normalize_country_code(value):
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def validate_member_records(df):
    """
    Validate member records individually.

    Member identity is defined by:
        (country_code, member_id)

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
            df[column].isna()
            | df[column].astype(str).str.strip().eq("")
        )

        for index in df.index[invalid_mask]:
            row_errors[index].append({
                "check": "MANDATORY_VALUES",
                "column": column,
                "message": f"{column} contains a null or blank value"
            })

    # 2. Duplicate composite identity validation
    normalized_countries = (
        df["country_code"].map(_normalize_country_code)
    )

    normalized_ids = (
        df["member_id"].map(_normalize_member_id)
    )

    duplicate_mask = pd.DataFrame({
        "country_code": normalized_countries,
        "member_id": normalized_ids
    }).duplicated(
        subset=["country_code", "member_id"],
        keep=False
    )

    # Do not report blank identities as duplicate identities.
    identifiable_mask = (
        normalized_countries.ne("")
        & normalized_ids.ne("")
    )

    duplicate_mask = duplicate_mask & identifiable_mask

    for index in df.index[duplicate_mask]:
        row_errors[index].append({
            "check": "DUPLICATE_MEMBER_IDENTITY",
            "column": "country_code, member_id",
            "message": (
                "Duplicate country code and member ID "
                "combination found in source file."
            )
        })

    # 3. Date validation
    for column in DATE_COLUMNS:

        if column not in df.columns:
            continue

        non_blank = (
            df[column].notna()
            & df[column].astype(str).str.strip().ne("")
        )

        parsed_dates = pd.to_datetime(
            df.loc[non_blank, column],
            format="mixed",
            errors="coerce"
        )

        invalid_indexes = parsed_dates[
            parsed_dates.isna()
        ].index

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
    Validate member identities against incoming records and CORE.

    Identity key:
        (country_code, member_id)

    Rules:
    1. Same composite identity: same member.
    2. Same member ID in different countries: allowed.
    3. Same name with different composite identities: allowed.
    4. Different member IDs in the same country: different members.
    5. Existing CORE identity is compared using both country and ID.

    existing_identities must contain composite identity keys:
        {
            ("IND", "1"),
            ("USA", "1")
        }

    Existing validation_errors are preserved.
    """

    df = df.copy()

    if df.empty:
        return {
            "valid_records": df.copy(),
            "rejected_records": df.copy(),
            "errors": [],
            "is_valid": True
        }

    required = ["country_code", "member_id"]

    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Identity validation requires columns: {missing}"
        )

    if existing_identities is None:
        existing_identities = set()

    def normalize_identity(country_code, member_id):
        return (
            _normalize_country_code(country_code),
            _normalize_member_id(member_id)
        )

    # Normalize existing CORE identities.
    normalized_existing = set()

    for identity in existing_identities:

        if not isinstance(identity, (tuple, list)) or len(identity) != 2:
            raise ValueError(
                "Existing identities must use "
                "(country_code, member_id) pairs."
            )

        country_code, member_id = normalize_identity(
            identity[0],
            identity[1]
        )

        if country_code and member_id:
            normalized_existing.add(
                (country_code, member_id)
            )

    # Normalize incoming identities.
    working = df.copy()

    working["_identity_country"] = (
        working["country_code"].map(_normalize_country_code)
    )

    working["_identity_id"] = (
        working["member_id"].map(_normalize_member_id)
    )

    # Only complete identities participate in comparison.
    identifiable = working[
        working["_identity_country"].ne("")
        & working["_identity_id"].ne("")
    ]

    # Detect duplicate composite identities in the incoming batch.
    duplicate_mask = identifiable.duplicated(
        subset=["_identity_country", "_identity_id"],
        keep=False
    )

    duplicate_indexes = set(
        identifiable.index[duplicate_mask]
    )

    rejected_errors = {}
    errors = []

    for index, row in working.iterrows():

        country_code = row["_identity_country"]
        member_id = row["_identity_id"]

        if not country_code or not member_id:
            continue

        identity = (country_code, member_id)
        conflict_message = None

        if index in duplicate_indexes:
            conflict_message = (
                f"Duplicate member identity found for "
                f"country '{country_code}' and "
                f"member ID '{member_id}' in the incoming batch."
            )

        elif identity in normalized_existing:
            # This identity already exists in CORE.
            # It is the same member, not an identity conflict.
            continue

        if conflict_message:

            identity_error = {
                "check": "DUPLICATE_MEMBER_IDENTITY",
                "column": "country_code, member_id",
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

    # Preserve existing validation errors.
    if not rejected_records.empty:

        merged_errors = []

        for index in rejected_indexes:

            existing_errors = []

            if "validation_errors" in rejected_records.columns:

                existing_errors = rejected_records.at[
                    index,
                    "validation_errors"
                ]

                if not isinstance(existing_errors, list):
                    existing_errors = []

            merged_errors.append(
                existing_errors + rejected_errors[index]
            )

        rejected_records["validation_errors"] = merged_errors

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
            errors.append(
                f"Missing mandatory field: {field}"
            )

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
        errors.append(
            "feed_date must use YYYYMMDD format."
        )

    redemptions = data["redemptions"]

    if not isinstance(redemptions, list):

        errors.append(
            "redemptions must be a JSON array."
        )

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
                    f"Redemption at index {index} "
                    "must be a JSON object."
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
                    miles = float(
                        transaction["miles_redeemed"]
                    )

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
