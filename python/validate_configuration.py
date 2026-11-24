
import os
import re
import sys

from python.utils.config_utils import (
    load_project_config,
    get_snowflake_object,
    validate_snowflake_identifier
)


# --------------------------------------------------
# 1. VALIDATION RULES
# --------------------------------------------------

REQUIRED_ENVIRONMENT_VARIABLES = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA"
]

SUPPORTED_FORMATS = {
    "CSV",
    "XLSX",
    "JSON",
    "XML"
}

FILE_PATTERN_REGEX = re.compile(r"^[^/\\]+\.[^/\\]+$")


# --------------------------------------------------
# 2. VALIDATE ENVIRONMENT VARIABLES
# --------------------------------------------------

def validate_environment():
    """Validate required Snowflake environment variables."""

    errors = []

    for variable in REQUIRED_ENVIRONMENT_VARIABLES:

        if not os.getenv(variable):
            errors.append(
                f"Missing required environment variable: {variable}"
            )

    if os.getenv("SNOWFLAKE_DATABASE"):

        try:
            validate_snowflake_identifier(
                os.getenv("SNOWFLAKE_DATABASE")
            )

        except ValueError as error:
            errors.append(str(error))

    if os.getenv("SNOWFLAKE_SCHEMA"):

        try:
            validate_snowflake_identifier(
                os.getenv("SNOWFLAKE_SCHEMA")
            )

        except ValueError as error:
            errors.append(str(error))

    return errors


# --------------------------------------------------
# 3. VALIDATE SNOWFLAKE OBJECTS
# --------------------------------------------------

def validate_snowflake_objects(config):
    """Validate configured Snowflake database, schemas and tables."""

    errors = []

    objects = (
        config.get("snowflake", {})
        .get("objects", {})
    )

    if not isinstance(objects, dict) or not objects:
        return [
            "No Snowflake objects are defined in configuration."
        ]

    for object_key, object_config in objects.items():

        if not isinstance(object_config, dict):
            errors.append(
                f"Invalid Snowflake object definition: {object_key}"
            )
            continue

        try:
            get_snowflake_object(object_key, config)

        except (KeyError, ValueError) as error:
            errors.append(
                f"Object '{object_key}': {error}"
            )
            continue

        for field in ["schema", "table"]:

            value = object_config.get(field)

            try:
                validate_snowflake_identifier(value)

            except ValueError as error:
                errors.append(
                    f"Object '{object_key}': {error}"
                )

    return errors


# --------------------------------------------------
# 4. VALIDATE SOURCE DEFINITIONS
# --------------------------------------------------

def validate_sources(config):
    """Validate source definitions and their central object references."""

    errors = []

    sources = config.get("sources", [])

    if not isinstance(sources, list) or not sources:
        return [
            "No valid source definitions found."
        ]

    source_ids = set()

    for index, source in enumerate(sources):

        if not isinstance(source, dict):
            errors.append(
                f"Source at position {index} must be an object."
            )
            continue

        source_id = source.get("source_id")

        if not isinstance(source_id, str) or not source_id.strip():
            errors.append(
                f"Source at position {index} has no valid source_id."
            )
            continue

        if source_id in source_ids:
            errors.append(
                f"Duplicate source_id found: {source_id}"
            )

        source_ids.add(source_id)

        if not source.get("entity"):
            errors.append(
                f"{source_id}: Missing entity."
            )

        if not source.get("landing_directory"):
            errors.append(
                f"{source_id}: Missing landing_directory."
            )

        # Validate file pattern.
        file_pattern = source.get("file_pattern")

        if not file_pattern:
            errors.append(
                f"{source_id}: Missing file_pattern."
            )

        elif not isinstance(file_pattern, str) or not FILE_PATTERN_REGEX.fullmatch(
            file_pattern
        ):
            errors.append(
                f"{source_id}: Invalid file_pattern: {file_pattern}"
            )

        # Validate file format.
        file_format = source.get("format")

        if not isinstance(file_format, str) or file_format.upper() not in SUPPORTED_FORMATS:
            errors.append(
                f"{source_id}: Unsupported format: {file_format}"
            )

        # Validate target object reference.
        target_object = source.get("target_object")

        if not isinstance(target_object, str) or not target_object.strip():
            errors.append(
                f"{source_id}: Missing or invalid target_object."
            )
            continue

        try:
            get_snowflake_object(
                target_object,
                config
            )

        except (KeyError, ValueError) as error:
            errors.append(
                f"{source_id}: Invalid target_object "
                f"'{target_object}': {error}"
            )

    return errors


# --------------------------------------------------
# 5. RUN ALL CONFIGURATION CHECKS
# --------------------------------------------------

def validate_configuration():
    """Run all project configuration validations."""

    print("\n========================================")
    print(" SKYPOINTS CONFIGURATION VALIDATION")
    print("========================================\n")

    errors = []

    print("[1/3] Checking environment variables...")

    environment_errors = validate_environment()
    errors.extend(environment_errors)

    print("[2/3] Checking Snowflake object mappings...")

    try:
        config = load_project_config()

    except Exception as error:
        print("\nConfiguration file could not be loaded:")
        print(error)
        return False

    object_errors = validate_snowflake_objects(config)
    errors.extend(object_errors)

    print("[3/3] Checking source definitions...")

    source_errors = validate_sources(config)
    errors.extend(source_errors)

    # ----------------------------------------------
    # DISPLAY RESULTS
    # ----------------------------------------------

    if errors:

        print("\nCONFIGURATION VALIDATION FAILED\n")

        for index, error in enumerate(errors, start=1):
            print(f"{index}. {error}")

        print(
            "\nFix the configuration errors before running the pipeline."
        )

        return False

    print("\nCONFIGURATION VALIDATION SUCCESSFUL")
    print("Environment settings: VALID")
    print("Snowflake object mappings: VALID")
    print("Source definitions: VALID")

    return True


# --------------------------------------------------
# 6. SCRIPT ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":

    success = validate_configuration()

    if not success:
        sys.exit(1)

    sys.exit(0)
