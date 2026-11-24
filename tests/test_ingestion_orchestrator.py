
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from python.ingestion.ingestion_orchestrator import (
    run_member_source,
    run_all_member_sources
)


def test_member_source_orchestration():

    source_config = {
        "source_id": "IND_MEMBERS",
        "file_name": "IND.csv",
        "format": "CSV",
        "entity": "MEMBER_PROFILE",
        "target_table": "RAW_MEMBER_PROFILES",
        "enabled": True
    }

    test_file = PROJECT_ROOT / "data" / "test" / "members" / "IND.csv"

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

    test_file = PROJECT_ROOT / "data" / "test" / "members" / "IND.csv"

    assert test_file.exists(), "Test CSV file does not exist."

    # Both rows have the same composite identity.
    # Row 1 also has a normal validation error.
    all_records_df = pd.DataFrame([
        {
            "member_name": "MIKE",
            "country_code": "IND",
            "member_id": 3,
            "enrollment_date": "invalid-date",
            "_source_row_number": 1
        },
        {
            "member_name": "SAM",
            "country_code": "IND",
            "member_id": 3,
            "enrollment_date": "2021-01-01",
            "_source_row_number": 2
        }
    ])

    valid_df = all_records_df.iloc[[1]].copy()

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
        return_value=set()
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

        assert result["ingestion"]["total_records"] == 2
        assert result["ingestion"]["valid_records"] == 0
        assert result["ingestion"]["rejected_records"] == 2
        assert result["ingestion"]["identity_conflicts"] == 2

        loader_arguments = mock_loader.call_args.kwargs

        assert loader_arguments["df"].empty

        quarantined_df = loader_arguments["rejected_df"]

        assert len(quarantined_df) == 2

        assert set(
            quarantined_df["_source_row_number"]
        ) == {1, 2}

        for _, row in quarantined_df.iterrows():

            errors = row["validation_errors"]

            assert any(
                error["check"] == "DUPLICATE_MEMBER_IDENTITY"
                for error in errors
            )

        first_row = quarantined_df[
            quarantined_df["_source_row_number"] == 1
        ].iloc[0]

        assert any(
            error["check"] == "INVALID_DATE"
            for error in first_row["validation_errors"]
        )

        assert (
            quarantined_df["_source_row_number"] == 2
        ).sum() == 1

        mock_ingestion.assert_called_once()
        mock_identity_lookup.assert_called_once()
        mock_connection.assert_called_once()
        mock_loader.assert_called_once()
        mock_processor.assert_called_once()

        mock_connection.return_value.close.assert_called_once()

    print("\nComposite identity orchestration test passed!")


if __name__ == "__main__":
    test_member_source_orchestration()
    test_orchestrator_quarantines_all_identity_conflicts()


def test_production_file_discovery(tmp_path):

    raw_members = tmp_path / "raw" / "members"
    raw_redemptions = tmp_path / "raw" / "redemptions"
    sample_members = tmp_path / "sample" / "members"
    test_members = tmp_path / "test" / "members"

    for directory in [
        raw_members,
        raw_redemptions,
        sample_members,
        test_members
    ]:
        directory.mkdir(parents=True)

    # Two production files matching the configured pattern.
    (raw_members / "IND_01.csv").touch()
    (raw_members / "IND_02.csv").touch()

    # These must never be discovered as production files.
    (sample_members / "IND_sample.csv").touch()
    (test_members / "IND_test.csv").touch()

    # This source has no matching production files.
    # This source is disabled.
    (raw_members / "DISABLED.csv").touch()

    source_config = {
        "sources": [
            {
                "source_id": "IND_MEMBERS",
                "entity": "MEMBER_PROFILE",
                "landing_directory": str(raw_members),
                "file_pattern": "IND*.csv",
                "enabled": True
            },
            {
                "source_id": "REDEMPTION_TRANSACTIONS",
                "entity": "REDEMPTION",
                "landing_directory": str(raw_redemptions),
                "file_pattern": "*.json",
                "enabled": True
            },
            {
                "source_id": "DISABLED_MEMBERS",
                "entity": "MEMBER_PROFILE",
                "landing_directory": str(raw_members),
                "file_pattern": "DISABLED.csv",
                "enabled": False
            }
        ]
    }

    with patch(
        "python.ingestion.ingestion_orchestrator.load_source_config",
        return_value=source_config
    ), patch(
        "python.ingestion.ingestion_orchestrator.run_member_source"
    ) as mock_member, patch(
        "python.ingestion.ingestion_orchestrator.run_redemption_source"
    ) as mock_redemption:

        run_all_member_sources()

        discovered_files = [
            call.kwargs["file_path"]
            for call in mock_member.call_args_list
        ]

        assert len(mock_member.call_args_list) == 2
        assert set(discovered_files) == {
            raw_members / "IND_01.csv",
            raw_members / "IND_02.csv"
        }

        mock_redemption.assert_not_called()


def test_duplicate_member_file_skips_transformation():

    source_config = {
        "source_id": "IND_MEMBERS",
        "format": "CSV",
        "entity": "MEMBER_PROFILE",
        "enabled": True
    }

    test_file = (
        PROJECT_ROOT / "data" / "test" / "members" / "IND.csv"
    )

    empty_records = pd.DataFrame(columns=["_source_row_number"])

    mocked_ingestion_result = {
        "data": pd.DataFrame(columns=["member_name"]),
        "valid_data": empty_records.copy(),
        "rejected_data": pd.DataFrame(
            columns=["_source_row_number", "validation_errors"]
        ),
        "total_records": 0
    }

    with patch(
        "python.ingestion.ingestion_orchestrator.run_member_ingestion",
        return_value=mocked_ingestion_result
    ), patch(
        "python.ingestion.ingestion_orchestrator.get_snowflake_connection"
    ) as mock_connection, patch(
        "python.ingestion.ingestion_orchestrator.get_existing_member_identities",
        return_value={}
    ), patch(
        "python.ingestion.ingestion_orchestrator.validate_member_identity_records",
        return_value={"rejected_records": empty_records.copy()}
    ), patch(
        "python.ingestion.ingestion_orchestrator.load_member_data",
        return_value={
            "batch_id": "DUPLICATE_TEST",
            "status": "SKIPPED_DUPLICATE",
            "records_loaded": 0,
            "records_rejected": 0,
            "quarantine_entries": 0
        }
    ), patch(
        "python.ingestion.ingestion_orchestrator.run_member_processing"
    ) as mock_processor:

        mock_connection.return_value = MagicMock()

        result = run_member_source(
            source_config,
            file_path=test_file
        )

        assert result["status"] == "SKIPPED_DUPLICATE"
        assert result["transformation"] is None

        mock_processor.assert_not_called()
        mock_connection.assert_called_once()


def test_duplicate_redemption_file_skips_transformation():

    from python.ingestion.ingestion_orchestrator import run_redemption_source

    source_config = {
        "source_id": "REDEMPTION_TRANSACTIONS",
        "format": "JSON",
        "entity": "REDEMPTION",
        "enabled": True
    }

    test_file = (
        PROJECT_ROOT
        / "data"
        / "test"
        / "redemptions"
        / "redemption_incremental_01.json"
    )

    assert test_file.exists()

    with patch(
        "python.ingestion.ingestion_orchestrator.read_file",
        return_value={"member_id": "TEST", "feed_date": "20261123", "redemptions": []}
    ), patch(
        "python.ingestion.ingestion_orchestrator.validate_redemption_data",
        return_value={"is_valid": True, "errors": []}
    ), patch(
        "python.ingestion.ingestion_orchestrator.get_snowflake_connection"
    ) as mock_connection, patch(
        "python.ingestion.ingestion_orchestrator.load_redemption_data",
        return_value={
            "batch_id": "DUPLICATE_REDEMPTION_TEST",
            "status": "SKIPPED_DUPLICATE",
            "records_loaded": 0
        }
    ), patch(
        "python.ingestion.ingestion_orchestrator.run_redemption_processing"
    ) as mock_processor:

        mock_connection.return_value = MagicMock()

        result = run_redemption_source(
            source_config,
            file_path=test_file
        )

        assert result["status"] == "SKIPPED_DUPLICATE"
        assert result["transformation"] is None

        mock_processor.assert_not_called()
        mock_connection.assert_called_once()
