
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from python.transformations.core_processor import (
    run_core_incremental,
    EXPIRE_SQL_PATH,
    INSERT_SQL_PATH
)


def test_core_incremental_success():

    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connection.cursor.return_value = mock_cursor
    

    # Simulate Snowflake affected-row counts.
    mock_cursor.rowcount = 1

    with patch(
        "python.transformations.core_processor.get_snowflake_connection",
        return_value=mock_connection
    ):

        result = run_core_incremental()

        assert result["status"] == "SUCCESS"
        assert result["expired_count"] == 1
        assert result["inserted_count"] == 1

        mock_connection.autocommit.assert_called_once_with(False)

        assert mock_cursor.execute.call_count == 2

        executed_sql = [
            call.args[0]
            for call in mock_cursor.execute.call_args_list
        ]

        assert executed_sql[0] == EXPIRE_SQL_PATH.read_text(
            encoding="utf-8"
        )

        assert executed_sql[1] == INSERT_SQL_PATH.read_text(
            encoding="utf-8"
        )

        mock_connection.commit.assert_called_once()
        mock_connection.rollback.assert_not_called()

        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    print("\nCORE incremental success test passed!")


def test_core_incremental_rollback():

    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connection.cursor.return_value = mock_cursor
    

    # Simulate failure during INSERT.
    mock_cursor.rowcount = 0
    
    mock_cursor.execute.side_effect = [
        None,
        Exception("Simulated INSERT failure")
    ]

    with patch(
        "python.transformations.core_processor.get_snowflake_connection",
        return_value=mock_connection
    ):

        try:
            run_core_incremental()
            assert False, "Expected an exception but none was raised."

        except Exception as error:
            assert str(error) == "Simulated INSERT failure"

        mock_connection.commit.assert_not_called()
        mock_connection.rollback.assert_called_once()

        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    print("\nCORE incremental rollback test passed!")


if __name__ == "__main__":
    test_core_incremental_success()
    test_core_incremental_rollback()
