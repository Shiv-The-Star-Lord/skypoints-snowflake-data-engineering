
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from python.transformations.member_processor import (
    run_member_processing,
    INCREMENTAL_STG_SQL_PATH,
    EXPIRE_SQL_PATH,
    INSERT_CORE_SQL_PATH,
    read_sql_file,
    extract_staging_insert,
    replace_configured_objects,
)

from python.utils.config_utils import get_fully_qualified_name


def get_member_object_mapping():

    return {
        "__MEMBER_RAW_TABLE__": get_fully_qualified_name("member_raw"),
        "__MEMBER_STAGING_TABLE__": get_fully_qualified_name("member_staging"),
        "__MEMBER_CORE_TABLE__": get_fully_qualified_name("member_core"),
        "__MEMBER_BATCH_CONTROL_TABLE__": get_fully_qualified_name(
            "member_batch_control"
        ),
        "__INGESTION_AUDIT_TABLE__": get_fully_qualified_name(
            "ingestion_audit"
        ),
    }


def test_member_processing_success():

    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 5

    with patch(
        "python.transformations.member_processor.get_snowflake_connection",
        return_value=mock_connection
    ):

        result = run_member_processing()

        assert result["status"] == "SUCCESS"
        assert result["registered_batch_count"] == 5
        assert result["stg_inserted_count"] == 5
        assert result["expired_count"] == 5
        assert result["core_inserted_count"] == 5
        assert result["country_sync_statement_count"] == 10

        mock_connection.autocommit.assert_called_once_with(False)

        assert mock_cursor.execute.call_count == 21

        executed_sql = [
            call.args[0]
            for call in mock_cursor.execute.call_args_list
        ]

        object_mapping = get_member_object_mapping()

        database_name = object_mapping[
            "__MEMBER_RAW_TABLE__"
        ].split(".")[0]

        ops_schema = object_mapping[
            "__MEMBER_BATCH_CONTROL_TABLE__"
        ].split(".")[1]

        core_schema = object_mapping[
            "__MEMBER_CORE_TABLE__"
        ].split(".")[1]

        expected_stg_sql = replace_configured_objects(
            extract_staging_insert(
                read_sql_file(INCREMENTAL_STG_SQL_PATH)
            ),
            object_mapping
        )

        expected_expire_sql = replace_configured_objects(
            read_sql_file(EXPIRE_SQL_PATH),
            object_mapping
        )

        expected_core_insert_sql = replace_configured_objects(
            read_sql_file(INSERT_CORE_SQL_PATH),
            object_mapping
        )

        assert executed_sql[0] == f"USE DATABASE {database_name}"
        assert executed_sql[1] == f"USE SCHEMA {ops_schema}"

        # Batch registration.
        assert (
            f"INSERT INTO {object_mapping['__MEMBER_BATCH_CONTROL_TABLE__']}"
            in executed_sql[2]
        )

        # STG processing.
        assert (
            f"UPDATE {object_mapping['__MEMBER_BATCH_CONTROL_TABLE__']}"
            in executed_sql[3]
        )

        assert executed_sql[4] == expected_stg_sql
        assert "STG_STATUS = 'SUCCESS'" in executed_sql[5]

        # CORE processing.
        assert executed_sql[6] == f"USE SCHEMA {core_schema}"
        assert "CORE_STARTED_AT" in executed_sql[7]
        assert executed_sql[8] == expected_expire_sql
        assert executed_sql[9] == expected_core_insert_sql
        assert "CORE_STATUS = 'SUCCESS'" in executed_sql[10]

        # Country synchronization runs after CORE processing.
        country_statements = executed_sql[11:21]

        assert len(country_statements) == 10

        for statement in country_statements:
            assert "__" not in statement

        assert sum(
            "MERGE INTO" in statement.upper()
            for statement in country_statements
        ) == 5

        assert sum(
            "DELETE FROM" in statement.upper()
            for statement in country_statements
        ) == 5

        # No unresolved placeholders should reach Snowflake.
        for sql in executed_sql:
            assert "__MEMBER_" not in sql
            assert "__INGESTION_" not in sql

        assert mock_connection.commit.call_count == 2
        mock_connection.rollback.assert_not_called()

        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    print("\nMember processing success test passed!")


def test_member_processing_rollback():

    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 5

    mock_cursor.execute.side_effect = (
        [None] * 9
        + [Exception("Simulated CORE INSERT failure")]
        + [None, None]
    )

    with patch(
        "python.transformations.member_processor.get_snowflake_connection",
        return_value=mock_connection
    ):

        try:
            run_member_processing()
            assert False, "Expected an exception but none was raised."

        except Exception as error:
            assert str(error) == "Simulated CORE INSERT failure"

        assert mock_connection.commit.call_count == 2
        mock_connection.rollback.assert_called_once()

        executed_sql = [
            call.args[0]
            for call in mock_cursor.execute.call_args_list
        ]

        assert len(executed_sql) == 12
        assert "STG_STATUS = 'FAILED'" in executed_sql[10]
        assert "CORE_STATUS = 'FAILED'" in executed_sql[11]

        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    print("\nMember processing rollback test passed!")


def test_core_sql_maps_derived_member_attributes():

    project_root = Path(__file__).resolve().parents[1]

    incremental_sql = read_sql_file(INSERT_CORE_SQL_PATH)

    initial_sql = read_sql_file(
        project_root / "sql" / "26_legacy_core_member_profile_scd2.sql"
    )

    for sql in [incremental_sql, initial_sql]:

        normalized_sql = sql.upper()

        assert "AGE" in normalized_sql
        assert "STALE_MEMBER" in normalized_sql

    assert "P.AGE" in incremental_sql.upper()
    assert "P.STALE_MEMBER" in incremental_sql.upper()

    assert "        AGE," in initial_sql
    assert "        STALE_MEMBER," in initial_sql

    print("\nCORE derived attribute mapping test passed!")


def test_member_processing_retry_after_failure():

    failed_connection = MagicMock()
    failed_cursor = MagicMock()
    failed_connection.cursor.return_value = failed_cursor
    failed_cursor.rowcount = 5

    failed_cursor.execute.side_effect = (
        [None] * 9
        + [Exception("Simulated CORE INSERT failure")]
        + [None, None]
    )

    retry_connection = MagicMock()
    retry_cursor = MagicMock()
    retry_connection.cursor.return_value = retry_cursor
    retry_cursor.rowcount = 5

    with patch(
        "python.transformations.member_processor.get_snowflake_connection",
        side_effect=[failed_connection, retry_connection]
    ):

        try:
            run_member_processing()
            assert False, "Expected the first execution to fail."

        except Exception as error:
            assert str(error) == "Simulated CORE INSERT failure"

        failure_sql = [
            call.args[0]
            for call in failed_cursor.execute.call_args_list
        ]

        assert "STG_STATUS = 'FAILED'" in failure_sql[10]
        assert "CORE_STATUS = 'FAILED'" in failure_sql[11]

        result = run_member_processing()

        assert result["status"] == "SUCCESS"
        assert result["registered_batch_count"] == 5
        assert result["stg_inserted_count"] == 5
        assert result["expired_count"] == 5
        assert result["core_inserted_count"] == 5

        assert retry_connection.commit.call_count == 2
        retry_connection.rollback.assert_not_called()

        retry_sql = [
            call.args[0]
            for call in retry_cursor.execute.call_args_list
        ]

        assert "STG_STATUS IN ('PENDING', 'FAILED')" in retry_sql[3]
        assert "CORE_STATUS IN ('PENDING', 'FAILED')" in retry_sql[7]

        failed_cursor.close.assert_called_once()
        retry_cursor.close.assert_called_once()
        failed_connection.close.assert_called_once()
        retry_connection.close.assert_called_once()


if __name__ == "__main__":
    test_member_processing_success()
    test_member_processing_rollback()
    test_core_sql_maps_derived_member_attributes()
