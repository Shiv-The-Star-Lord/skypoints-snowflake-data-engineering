
import json
import re


def normalize_column_name(column_name):
    """
    Normalize column names for comparison.

    Examples:
        Member_Id   -> member_id
        Member ID   -> member_id
        MEMBER NAME -> member_name
    """
    column_name = str(column_name).strip().lower()
    column_name = re.sub(r"[^a-z0-9]+", "_", column_name)

    return column_name.strip("_")


def load_column_mapping(mapping_path):
    """Load column aliases from the JSON configuration."""

    with open(mapping_path, "r", encoding="utf-8") as file:
        return json.load(file)


def standardize_headers(df, mapping_path):
    """
    Standardize source DataFrame column names.

    Returns:
        standardized_df: DataFrame with standardized headers.
        missing_columns: Standard columns not found in source.
    """

    mapping = load_column_mapping(mapping_path)

    # Step 1: Build alias lookup
    alias_lookup = {}

    for standard_name, aliases in mapping.items():

        # Include the standard name itself as a valid alias
        all_aliases = [standard_name] + aliases

        for alias in all_aliases:

            normalized_alias = normalize_column_name(alias)

            if normalized_alias in alias_lookup:

                existing_standard_name = alias_lookup[normalized_alias]

                # Same alias can map repeatedly to the same standard name
                if existing_standard_name != standard_name:
                    raise ValueError(
                        f"Alias '{alias}' maps to multiple "
                        f"standard columns: "
                        f"{existing_standard_name} and {standard_name}"
                    )

            else:
                alias_lookup[normalized_alias] = standard_name

    # Step 2: Build source-to-standard rename dictionary
    rename_dict = {}

    for source_column in df.columns:

        normalized_column = normalize_column_name(source_column)

        if normalized_column in alias_lookup:
            rename_dict[source_column] = alias_lookup[normalized_column]

    # Step 3: Detect duplicate standardized columns
    mapped_names = list(rename_dict.values())

    if len(mapped_names) != len(set(mapped_names)):
        raise ValueError(
            "Multiple source columns map to the same standard column."
        )

    # Step 4: Rename source columns
    standardized_df = df.rename(columns=rename_dict)

    # Step 5: Identify missing standard columns
    missing_columns = [
        standard_name
        for standard_name in mapping.keys()
        if standard_name not in standardized_df.columns
    ]

    return standardized_df, missing_columns
