
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from python.transformations.member_processor import (
    run_member_processing,
    STG_SQL_PATH,
    INCREMENTAL_STG_SQL_PATH,
    EXPIRE_SQL_PATH,
    INSERT_CORE_SQL_PATH,
    read_sql_file,
    extract_staging_insert
)


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

        # Verify returned results.
        assert result["status"] == "SUCCESS"
        assert result["registered_batch_count"] == 5
        assert result["stg_inserted_count"] == 5
        assert result["expired_count"] == 5
        assert result["core_inserted_count"] == 5

        # Verify connection configuration.
        mock_connection.autocommit.assert_called_once_with(False)

        # Verify all 11 SQL statements were executed.
        assert mock_cursor.execute.call_count == 11

        executed_sql = [
            call.args[0]
            for call in mock_cursor.execute.call_args_list
        ]

        expected_stg_sql = extract_staging_insert(
            read_sql_file(INCREMENTAL_STG_SQL_PATH)
        )

        expected_expire_sql = read_sql_file(EXPIRE_SQL_PATH)

        expected_core_insert_sql = read_sql_file(
            INSERT_CORE_SQL_PATH
        )

        assert executed_sql[0] == "USE DATABASE SKYPOINTS_DE"
        assert executed_sql[1] == "USE SCHEMA OPS"

        # Batch registration.
        assert "INSERT INTO SKYPOINTS_DE.OPS.MEMBER_BATCH_CONTROL" in executed_sql[2]

        # STG processing.
        assert "UPDATE SKYPOINTS_DE.OPS.MEMBER_BATCH_CONTROL" in executed_sql[3]
        assert executed_sql[4] == expected_stg_sql
        assert "STG_STATUS = 'SUCCESS'" in executed_sql[5]

        # CORE processing.
        assert executed_sql[6] == "USE SCHEMA CORE"
        assert "CORE_STARTED_AT" in executed_sql[7]
        assert executed_sql[8] == expected_expire_sql
        assert executed_sql[9] == expected_core_insert_sql
        assert "CORE_STATUS = 'SUCCESS'" in executed_sql[10]

        # Registration commit + final transformation commit.
        assert mock_connection.commit.call_count == 2
        mock_connection.rollback.assert_not_called()

        # Verify resources are closed.
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    print("\nMember processing success test passed!")


def test_member_processing_rollback():

    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 5

    # Fail on CORE INSERT (10th SQL execution).
    # The remaining two executions record failure statuses.
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

        # Registration is committed before transformations.
        # Failure status is committed after rollback.
        assert mock_connection.commit.call_count == 2

        # STG and CORE transformation transaction is rolled back.
        mock_connection.rollback.assert_called_once()

        # Verify failure status updates were attempted.
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

    incremental_sql = read_sql_file(
        INSERT_CORE_SQL_PATH
    )

    initial_sql = read_sql_file(
        project_root / "sql" / "03_core_member_profile_scd2.sql"
    )

    for sql in [incremental_sql, initial_sql]:

        normalized_sql = sql.upper()

        assert "AGE" in normalized_sql
        assert "STALE_MEMBER" in normalized_sql

    # Verify incremental mapping from STG to CORE.
    assert "P.AGE" in incremental_sql.upper()
    assert "P.STALE_MEMBER" in incremental_sql.upper()

    # Verify initial-load mapping from STG to CORE.
    assert "        AGE," in initial_sql
    assert "        STALE_MEMBER," in initial_sql

    print("\nCORE derived attribute mapping test passed!")


def test_member_processing_retry_after_failure():

    # First execution: CORE INSERT fails.
    failed_connection = MagicMock()
    failed_cursor = MagicMock()
    failed_connection.cursor.return_value = failed_cursor
    failed_cursor.rowcount = 5

    failed_cursor.execute.side_effect = (
        [None] * 9
        + [Exception("Simulated CORE INSERT failure")]
        + [None, None]
    )

    # Second execution: retry succeeds.
    retry_connection = MagicMock()
    retry_cursor = MagicMock()
    retry_connection.cursor.return_value = retry_cursor
    retry_cursor.rowcount = 5

    with patch(
        "python.transformations.member_processor.get_snowflake_connection",
        side_effect=[failed_connection, retry_connection]
    ):

        # First execution should fail.
        try:
            run_member_processing()
            assert False, "Expected the first execution to fail."
        except Exception as error:
            assert str(error) == "Simulated CORE INSERT failure"

        # Failure must be recorded.
        failure_sql = [
            call.args[0]
            for call in failed_cursor.execute.call_args_list
        ]

        assert "STG_STATUS = 'FAILED'" in failure_sql[10]
        assert "CORE_STATUS = 'FAILED'" in failure_sql[11]

        # Second execution should retry successfully.
        result = run_member_processing()

        assert result["status"] == "SUCCESS"
        assert result["registered_batch_count"] == 5
        assert result["stg_inserted_count"] == 5
        assert result["expired_count"] == 5
        assert result["core_inserted_count"] == 5

        # Retry completes its own transaction.
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
