
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from python.transformations.redemption_processor import (
    run_redemption_processing,
    QUARANTINE_SQL_PATH,
    STG_SQL_PATH,
    CORE_SQL_PATH,
)

from python.utils.config_utils import (
    get_fully_qualified_name,
    get_snowflake_object,
)


def test_redemption_processing_success():

    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connection.cursor.return_value = mock_cursor

    mock_cursor.fetchone.side_effect = [
        (2, 0),  # Quarantine MERGE
        (1, 0),  # STG MERGE
        (0, 1),  # CORE MERGE
    ]

    with patch(
        "python.transformations.redemption_processor.get_snowflake_connection",
        return_value=mock_connection
    ):

        result = run_redemption_processing()

        # Verify processing result.
        assert result["status"] == "SUCCESS"
        assert result["quarantine_merge_results"] == (2, 0)
        assert result["stg_merge_results"] == (1, 0)
        assert result["core_merge_results"] == (0, 1)

        # Verify transaction configuration.
        mock_connection.autocommit.assert_called_once_with(False)

        # Verify SQL execution count.
        assert mock_cursor.execute.call_count == 6

        executed_sql = [
            call.args[0]
            for call in mock_cursor.execute.call_args_list
        ]

        # Resolve Snowflake objects from configuration.
        object_mapping = {
            "__REJECTED_REDEMPTION_TABLE__":
                get_fully_qualified_name("rejected_redemption_records"),

            "__REDEMPTION_RAW_TABLE__":
                get_fully_qualified_name("redemption_raw"),

            "__MEMBER_CORE_TABLE__":
                get_fully_qualified_name("member_core"),

            "__INGESTION_AUDIT_TABLE__":
                get_fully_qualified_name("ingestion_audit"),

            "__REDEMPTION_STAGING_TABLE__":
                get_fully_qualified_name("redemption_staging"),

            "__REDEMPTION_CORE_TABLE__":
                get_fully_qualified_name("redemption_core"),
        }

        staging_config = get_snowflake_object(
            "redemption_staging"
        )

        core_config = get_snowflake_object(
            "redemption_core"
        )

        database = staging_config["database"]
        staging_schema = staging_config["schema"]
        core_schema = core_config["schema"]

        # Verify Snowflake execution context.
        assert executed_sql[0] == f"USE DATABASE {database}"
        assert executed_sql[1] == f"USE SCHEMA {staging_schema}"

        # Verify configuration-driven quarantine SQL.
        original_quarantine_sql = QUARANTINE_SQL_PATH.read_text(
            encoding="utf-8"
        )

        expected_quarantine_sql = original_quarantine_sql

        for placeholder, object_name in object_mapping.items():
            expected_quarantine_sql = expected_quarantine_sql.replace(
                placeholder,
                object_name
            )

        assert executed_sql[2] == expected_quarantine_sql

        # Verify configuration-driven STG SQL.
        original_stg_sql = STG_SQL_PATH.read_text(
            encoding="utf-8"
        )

        expected_stg_sql = original_stg_sql

        for placeholder, object_name in object_mapping.items():
            expected_stg_sql = expected_stg_sql.replace(
                placeholder,
                object_name
            )

        assert executed_sql[3] == expected_stg_sql

        # Verify CORE schema switch.
        assert executed_sql[4] == f"USE SCHEMA {core_schema}"

        # Verify configuration-driven CORE SQL.
        original_core_sql = CORE_SQL_PATH.read_text(
            encoding="utf-8"
        )

        expected_core_sql = original_core_sql

        for placeholder, object_name in object_mapping.items():
            expected_core_sql = expected_core_sql.replace(
                placeholder,
                object_name
            )

        assert executed_sql[5] == expected_core_sql

        # Verify no unresolved placeholders reach Snowflake.
        for sql in (
            executed_sql[2],
            executed_sql[3],
            executed_sql[5],
        ):
            assert "__" not in sql

        # Verify transaction handling.
        mock_connection.commit.assert_called_once()
        mock_connection.rollback.assert_not_called()

        # Verify resource cleanup.
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    print("\nRedemption processing success test passed!")


def test_redemption_processing_rollback():

    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    mock_connection.cursor.return_value = mock_cursor

    # Simulate failure during CORE MERGE.
    mock_cursor.execute.side_effect = [
        None,       # USE DATABASE
        None,       # USE SCHEMA STG
        None,       # Quarantine MERGE
        None,       # STG MERGE
        None,       # USE SCHEMA CORE
        Exception("Simulated CORE MERGE failure"),
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

        # Verify rollback.
        mock_connection.commit.assert_not_called()
        mock_connection.rollback.assert_called_once()

        # Verify resource cleanup.
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    print("\nRedemption processing rollback test passed!")


if __name__ == "__main__":
    test_redemption_processing_success()
    test_redemption_processing_rollback()
