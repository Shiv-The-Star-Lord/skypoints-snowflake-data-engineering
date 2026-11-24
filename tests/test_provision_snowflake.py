
from unittest.mock import Mock
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from python import provision_snowflake as provisioning
import pytest

from python import provision_snowflake as provisioning


@pytest.fixture
def mocked_provisioning(monkeypatch):

    config = {"snowflake": {"database": "TEST_DB"}}
    schemas = ["RAW", "STG", "CORE", "OPS", "MART"]

    settings = {
        "SNOWFLAKE_ACCOUNT": "test_account",
        "SNOWFLAKE_USER": "test_user",
        "SNOWFLAKE_PASSWORD": "test_password",
        "SNOWFLAKE_WAREHOUSE": "test_warehouse",
        "SNOWFLAKE_ROLE": "test_role",
    }

    connection = Mock()
    cursor = Mock()

    connection.cursor.return_value = cursor

    connect = Mock(return_value=connection)

    monkeypatch.setattr(
        provisioning,
        "load_project_config",
        Mock(return_value=config),
    )

    monkeypatch.setattr(
        provisioning,
        "get_snowflake_database",
        Mock(return_value="TEST_DB"),
    )

    monkeypatch.setattr(
        provisioning,
        "get_snowflake_schemas",
        Mock(return_value=schemas),
    )

    monkeypatch.setattr(
        provisioning,
        "get_connection_settings",
        Mock(return_value=settings),
    )

    monkeypatch.setattr(
        provisioning,
        "build_sql_object_mapping",
        Mock(return_value={"__DATABASE__": "TEST_DB"}),
    )

    prepare_sql = Mock(
        side_effect=lambda path, mapping: f"PREPARED::{path.name}"
    )

    execute_sql_script = Mock()

    monkeypatch.setattr(
        provisioning,
        "prepare_sql",
        prepare_sql,
    )

    monkeypatch.setattr(
        provisioning,
        "execute_sql_script",
        execute_sql_script,
    )

    monkeypatch.setattr(
        provisioning.snowflake.connector,
        "connect",
        connect,
    )

    return {
        "connection": connection,
        "cursor": cursor,
        "connect": connect,
        "prepare_sql": prepare_sql,
        "execute_sql_script": execute_sql_script,
        "schemas": schemas,
        "settings": settings,
    }


def test_provision_snowflake_success(mocked_provisioning):

    mocks = mocked_provisioning

    provisioning.provision_snowflake()

    # Verify Snowflake connection settings.
    mocks["connect"].assert_called_once_with(
        account="test_account",
        user="test_user",
        password="test_password",
        warehouse="test_warehouse",
        role="test_role",
    )

    # Database SQL + all configured DDL scripts.
    assert mocks["prepare_sql"].call_count == (
        1 + len(provisioning.DDL_FILES)
    )

    assert mocks["execute_sql_script"].call_count == (
        1 + len(provisioning.DDL_FILES)
    )

    # Verify all configured schemas are created.
    schema_statements = [
        call.args[0]
        for call in mocks["cursor"].execute.call_args_list
    ]

    assert schema_statements == [
        f"CREATE SCHEMA IF NOT EXISTS TEST_DB.{schema}"
        for schema in mocks["schemas"]
    ]

    # Verify resources are closed.
    mocks["cursor"].close.assert_called_once()
    mocks["connection"].close.assert_called_once()


def test_provision_snowflake_closes_resources_on_failure(
    mocked_provisioning,
):

    mocks = mocked_provisioning

    # Database setup succeeds; first DDL execution fails.
    mocks["execute_sql_script"].side_effect = [
        None,
        RuntimeError("Simulated DDL failure"),
    ]

    with pytest.raises(
        RuntimeError,
        match="Simulated DDL failure",
    ):
        provisioning.provision_snowflake()

    # Resources must close even after failure.
    mocks["cursor"].close.assert_called_once()
    mocks["connection"].close.assert_called_once()
