import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from python.transformations.redemption_processor import (
    run_redemption_processing,
    STG_SQL_PATH,
    CORE_SQL_PATH
)


def test_redemption_processing_success():

    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connection.cursor.return_value = mock_cursor

    # Simulate Snowflake MERGE results.
    mock_cursor.fetchone.side_effect = [
        (1, 0),  # STG: 1 inserted, 0 updated
        (0, 1)   # CORE: 0 inserted, 1 updated
    ]

    with patch(
        "python.transformations.redemption_processor.get_snowflake_connection",
        return_value=mock_connection
    ):

        result = run_redemption_processing()

        assert result["status"] == "SUCCESS"
        assert result["stg_merge_results"] == (1, 0)
        assert result["core_merge_results"] == (0, 1)

        mock_connection.autocommit.assert_called_once_with(False)

        assert mock_cursor.execute.call_count == 5

        executed_sql = [
            call.args[0]
            for call in mock_cursor.execute.call_args_list
        ]

        assert executed_sql[0] == "USE DATABASE SKYPOINTS_DE"
        assert executed_sql[1] == "USE SCHEMA STG"

        assert executed_sql[2] == STG_SQL_PATH.read_text(
            encoding="utf-8"
        )

        assert executed_sql[3] == "USE SCHEMA CORE"

        assert executed_sql[4] == CORE_SQL_PATH.read_text(
            encoding="utf-8"
        )

        mock_connection.commit.assert_called_once()
        mock_connection.rollback.assert_not_called()

        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    print("\nRedemption processing success test passed!")


def test_redemption_processing_rollback():

    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connection.cursor.return_value = mock_cursor

    # Simulate failure during CORE MERGE.
    mock_cursor.execute.side_effect = [
        None,  # USE DATABASE
        None,  # USE SCHEMA STG
        None,  # STG MERGE
        None,  # USE SCHEMA CORE
        Exception("Simulated CORE MERGE failure")
    ]

    with patch(
        "python.transformations.redemption_processor.get_snowflake_connection",
        return_value=mock_connection
    ):

        try:
            run_redemption_processing()
            assert False, "Expected an exception but none was raised."

        except Exception as error:
            assert str(error) == "Simulated CORE MERGE failure"

        mock_connection.commit.assert_not_called()
        mock_connection.rollback.assert_called_once()

        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    print("\nRedemption processing rollback test passed!")


if __name__ == "__main__":
    test_redemption_processing_success()
    test_redemption_processing_rollback()