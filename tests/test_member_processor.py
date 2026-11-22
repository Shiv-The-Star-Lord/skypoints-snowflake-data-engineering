
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from python.transformations.member_processor import (
    run_member_processing,
    STG_SQL_PATH,
    EXPIRE_SQL_PATH,
    INSERT_CORE_SQL_PATH,
    read_sql_file,
    extract_staging_insert
)


def test_member_processing_success():

    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connection.cursor.return_value = mock_cursor

    # Simulate affected-row counts.
    mock_cursor.rowcount = 5

    with patch(
        "python.transformations.member_processor.get_snowflake_connection",
        return_value=mock_connection
    ):

        result = run_member_processing()

        assert result["status"] == "SUCCESS"
        assert result["stg_inserted_count"] == 5
        assert result["expired_count"] == 5
        assert result["core_inserted_count"] == 5

        mock_connection.autocommit.assert_called_once_with(False)

        assert mock_cursor.execute.call_count == 6

        executed_sql = [
            call.args[0]
            for call in mock_cursor.execute.call_args_list
        ]

        expected_stg_sql = extract_staging_insert(
            read_sql_file(STG_SQL_PATH)
        )

        expected_expire_sql = read_sql_file(EXPIRE_SQL_PATH)
        expected_core_insert_sql = read_sql_file(
            INSERT_CORE_SQL_PATH
        )

        assert executed_sql[0] == "USE DATABASE SKYPOINTS_DE"
        assert executed_sql[1] == "USE SCHEMA STG"
        assert executed_sql[2] == expected_stg_sql
        assert executed_sql[3] == "USE SCHEMA CORE"
        assert executed_sql[4] == expected_expire_sql
        assert executed_sql[5] == expected_core_insert_sql

        mock_connection.commit.assert_called_once()
        mock_connection.rollback.assert_not_called()

        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    print("\nMember processing success test passed!")


def test_member_processing_rollback():

    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connection.cursor.return_value = mock_cursor

    # Simulate failure during CORE INSERT.
    mock_cursor.rowcount = 5

    mock_cursor.execute.side_effect = [
        None,  # USE DATABASE
        None,  # USE SCHEMA STG
        None,  # STG INSERT
        None,  # USE SCHEMA CORE
        None,  # CORE expiration
        Exception("Simulated CORE INSERT failure")
    ]

    with patch(
        "python.transformations.member_processor.get_snowflake_connection",
        return_value=mock_connection
    ):

        try:
            run_member_processing()
            assert False, "Expected an exception but none was raised."

        except Exception as error:
            assert str(error) == "Simulated CORE INSERT failure"

        mock_connection.commit.assert_not_called()
        mock_connection.rollback.assert_called_once()

        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    print("\nMember processing rollback test passed!")


def test_core_sql_maps_derived_member_attributes():

    project_root = Path(__file__).resolve().parents[1]

    incremental_sql = read_sql_file(INSERT_CORE_SQL_PATH)

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


if __name__ == "__main__":
    test_member_processing_success()
    test_member_processing_rollback()
