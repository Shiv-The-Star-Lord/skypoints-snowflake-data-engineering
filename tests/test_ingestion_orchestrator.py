
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from python.ingestion.ingestion_orchestrator import run_member_source


def test_member_source_orchestration():

    source_config = {
        "source_id": "IND_MEMBERS",
        "file_name": "IND.csv",
        "format": "CSV",
        "entity": "MEMBER_PROFILE",
        "target_table": "RAW_MEMBER_PROFILES",
        "enabled": True
    }

    test_file = PROJECT_ROOT / "data" / "test" / "IND.csv"

    assert test_file.exists(), "Test CSV file does not exist."

    with patch(
        "python.ingestion.ingestion_orchestrator.get_snowflake_connection"
    ) as mock_connection, patch(
        "python.ingestion.ingestion_orchestrator.load_member_data"
    ) as mock_loader, patch(
        "python.ingestion.ingestion_orchestrator.run_member_processing"
    ) as mock_processor:

        mock_connection.return_value = MagicMock()

        mock_loader.return_value = {
            "batch_id": "TEST_BATCH_001",
            "status": "SUCCESS",
            "records_loaded": 5
        }

        mock_processor.return_value = {
            "stg_inserted_count": 5,
            "expired_count": 2,
            "core_inserted_count": 5,
            "status": "SUCCESS"
        }

        result = run_member_source(
            source_config,
            file_path=test_file
        )

        assert result["status"] == "SUCCESS"

        assert result["raw_load"]["records_loaded"] == 5

        assert result["transformation"]["status"] == "SUCCESS"

        assert result["transformation"]["stg_inserted_count"] == 5

        mock_connection.assert_called_once()
        mock_loader.assert_called_once()
        mock_processor.assert_called_once()

        loader_arguments = mock_loader.call_args.kwargs

        assert loader_arguments["source_id"] == "IND_MEMBERS"
        assert loader_arguments["entity_type"] == "MEMBER_PROFILE"
        assert loader_arguments["file_path"] == test_file

        assert len(loader_arguments["df"]) == 5

        mock_connection.return_value.close.assert_called_once()

    print("\nMember end-to-end orchestration test passed!")


if __name__ == "__main__":
    test_member_source_orchestration()
