
import json
import os
import re
from pathlib import Path

from dotenv import load_dotenv


# --------------------------------------------------
# 1. PROJECT CONFIGURATION PATHS
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONFIG_PATH = PROJECT_ROOT / "config" / "source_config.json"
ENV_FILE = PROJECT_ROOT / ".env"

# Load environment-specific configuration
load_dotenv(ENV_FILE)


# --------------------------------------------------
# 2. LOAD PROJECT CONFIGURATION
# --------------------------------------------------

def load_project_config():
    """Load the complete project configuration."""

    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Project configuration file not found: {CONFIG_PATH}"
        )

    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        return json.load(file)


# --------------------------------------------------
# 3. VALIDATE SNOWFLAKE IDENTIFIERS
# --------------------------------------------------

def validate_snowflake_identifier(identifier):
    """Validate a Snowflake database, schema or table identifier."""

    if not isinstance(identifier, str):
        raise ValueError(
            f"Snowflake identifier must be a string: {identifier}"
        )

    identifier_pattern = re.compile(
        r"^[A-Za-z_][A-Za-z0-9_$]*$"
    )

    if not identifier_pattern.fullmatch(identifier):
        raise ValueError(
            f"Invalid Snowflake identifier: {identifier}"
        )

    return identifier


# --------------------------------------------------
# 4. GET SNOWFLAKE DATABASE
# --------------------------------------------------

def get_snowflake_database():
    """Retrieve and validate the Snowflake database from .env."""

    database = os.getenv("SNOWFLAKE_DATABASE")

    if not database:
        raise ValueError(
            "SNOWFLAKE_DATABASE is missing from the environment configuration."
        )

    return validate_snowflake_identifier(database)


# --------------------------------------------------
# 5. GET CONFIGURED SNOWFLAKE SCHEMAS
# --------------------------------------------------

def get_snowflake_schemas(config=None):
    """
    Retrieve and validate all configured Snowflake schemas.

    Schema names are maintained centrally in source_config.json.
    """

    if config is None:
        config = load_project_config()

    snowflake_config = config.get("snowflake", {})
    schemas = snowflake_config.get("schemas")

    if not isinstance(schemas, list) or not schemas:
        raise ValueError(
            "Snowflake schemas are missing or invalid in source_config.json."
        )

    validated_schemas = [
        validate_snowflake_identifier(schema)
        for schema in schemas
    ]

    if len(validated_schemas) != len(set(validated_schemas)):
        raise ValueError(
            "Duplicate schema names found in source_config.json."
        )

    return validated_schemas


# --------------------------------------------------
# 6. GET SNOWFLAKE OBJECT CONFIGURATION
# --------------------------------------------------

def get_snowflake_object(object_key, config=None):
    """
    Retrieve a Snowflake object's database, schema and table.

    Database comes from the environment.
    Schema and table come from source_config.json.
    """

    if config is None:
        config = load_project_config()

    snowflake_config = config.get("snowflake", {})
    objects = snowflake_config.get("objects", {})

    if object_key not in objects:
        raise KeyError(
            f"Snowflake object '{object_key}' "
            "is not defined in configuration."
        )

    database = get_snowflake_database()
    schema = objects[object_key].get("schema")
    table = objects[object_key].get("table")

    if not all([database, schema, table]):
        raise ValueError(
            f"Incomplete Snowflake configuration for '{object_key}'. "
            "Check SNOWFLAKE_DATABASE in .env and the object "
            "definition in source_config.json."
        )

    return {
        "database": database,
        "schema": schema,
        "table": table
    }


# --------------------------------------------------
# 7. GET FULLY QUALIFIED OBJECT NAME
# --------------------------------------------------

def get_fully_qualified_name(object_key, config=None):
    """
    Return a validated fully qualified Snowflake object name.

    Format: DATABASE.SCHEMA.TABLE
    """

    snowflake_object = get_snowflake_object(object_key, config)

    database = validate_snowflake_identifier(
        snowflake_object["database"]
    )

    schema = validate_snowflake_identifier(
        snowflake_object["schema"]
    )

    table = validate_snowflake_identifier(
        snowflake_object["table"]
    )

    return f"{database}.{schema}.{table}"
