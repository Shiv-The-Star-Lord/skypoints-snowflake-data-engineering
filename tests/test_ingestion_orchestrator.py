
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd

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


def test_orchestrator_quarantines_all_identity_conflicts():

    source_config = {
        "source_id": "TEST_MEMBERS",
        "file_name": "IND.csv",
        "format": "CSV",
        "entity": "MEMBER_PROFILE",
        "target_table": "RAW_MEMBER_PROFILES",
        "enabled": True
    }

    test_file = PROJECT_ROOT / "data" / "test" / "IND.csv"

    assert test_file.exists(), "Test CSV file does not exist."

    # Both records have the same name but different member IDs.
    # The first record also has a normal validation error.
    all_records_df = pd.DataFrame([
        {
            "member_name": "MIKE",
            "member_id": 3,
            "enrollment_date": "invalid-date",
            "_source_row_number": 1
        },
        {
            "member_name": "MIKE",
            "member_id": 1,
            "enrollment_date": "2021-01-01",
            "_source_row_number": 2
        }
    ])

    # Normal validation accepts the second record.
    valid_df = all_records_df.iloc[[1]].copy()

    # Normal validation rejects the first record.
    existing_validation_error = {
        "check": "INVALID_DATE",
        "column": "enrollment_date",
        "message": "Invalid enrollment date"
    }

    rejected_df = all_records_df.iloc[[0]].copy()
    rejected_df["validation_errors"] = [
        [existing_validation_error]
    ]

    mocked_ingestion_result = {
        "data": all_records_df,
        "valid_data": valid_df,
        "rejected_data": rejected_df,
        "total_records": 2,
        "valid_records": 1,
        "rejected_records": 1,
        "is_valid": False,
        "errors": ["Invalid enrollment date"]
    }

    with patch(
        "python.ingestion.ingestion_orchestrator.run_member_ingestion",
        return_value=mocked_ingestion_result
    ) as mock_ingestion, patch(
        "python.ingestion.ingestion_orchestrator.get_snowflake_connection"
    ) as mock_connection, patch(
        "python.ingestion.ingestion_orchestrator.get_existing_member_identities",
        return_value={}
    ) as mock_identity_lookup, patch(
        "python.ingestion.ingestion_orchestrator.load_member_data"
    ) as mock_loader, patch(
        "python.ingestion.ingestion_orchestrator.run_member_processing"
    ) as mock_processor:

        mock_connection.return_value = MagicMock()

        mock_loader.return_value = {
            "batch_id": "TEST_IDENTITY_BATCH",
            "status": "SUCCESS",
            "records_loaded": 0
        }

        mock_processor.return_value = {
            "stg_inserted_count": 0,
            "expired_count": 0,
            "core_inserted_count": 0,
            "status": "SUCCESS"
        }

        result = run_member_source(
            source_config,
            file_path=test_file
        )

        # Both records must be rejected.
        assert result["ingestion"]["total_records"] == 2
        assert result["ingestion"]["valid_records"] == 0
        assert result["ingestion"]["rejected_records"] == 2
        assert result["ingestion"]["identity_conflicts"] == 2

        loader_arguments = mock_loader.call_args.kwargs

        # No identity-conflicting records should enter RAW.
        assert loader_arguments["df"].empty

        # Both records should be sent for quarantine.
        quarantined_df = loader_arguments["rejected_df"]

        assert len(quarantined_df) == 2
        assert set(
            quarantined_df["_source_row_number"]
        ) == {1, 2}

        # Each rejected row must have an identity conflict error.
        for _, row in quarantined_df.iterrows():
            errors = row["validation_errors"]

            assert any(
                error["check"] == "IDENTITY_CONFLICT"
                for error in errors
            )

        # The first row must retain its original validation error.
        first_row = quarantined_df[
            quarantined_df["_source_row_number"] == 1
        ].iloc[0]

        assert any(
            error["check"] == "INVALID_DATE"
            for error in first_row["validation_errors"]
        )

        # The second row must appear only once in quarantine.
        assert (
            quarantined_df["_source_row_number"] == 2
        ).sum() == 1

        mock_ingestion.assert_called_once()
        mock_identity_lookup.assert_called_once()
        mock_connection.assert_called_once()
        mock_loader.assert_called_once()
        mock_processor.assert_called_once()

        mock_connection.return_value.close.assert_called_once()

    print("\nIdentity conflict orchestration test passed!")


if __name__ == "__main__":
    test_member_source_orchestration()
    test_orchestrator_quarantines_all_identity_conflicts()
