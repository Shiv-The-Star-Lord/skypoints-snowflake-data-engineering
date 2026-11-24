
-- ============================================================
-- SQL 13: CREATE MEMBER BATCH CONTROL TABLE
-- ============================================================
-- Creates the operational batch control table.
-- Object name is resolved from config/source_config.json.
-- ============================================================

CREATE TABLE IF NOT EXISTS __MEMBER_BATCH_CONTROL_TABLE__
(
    BATCH_ID            VARCHAR(100) NOT NULL,
    SOURCE_ID           VARCHAR(100),
    SOURCE_FILE         VARCHAR(500),

    STG_STATUS          VARCHAR(30) DEFAULT 'PENDING',
    STG_STARTED_AT      TIMESTAMP_NTZ,
    STG_COMPLETED_AT    TIMESTAMP_NTZ,
    STG_ERROR_MESSAGE   VARCHAR(5000),

    CORE_STATUS         VARCHAR(30) DEFAULT 'PENDING',
    CORE_STARTED_AT     TIMESTAMP_NTZ,
    CORE_COMPLETED_AT   TIMESTAMP_NTZ,
    CORE_ERROR_MESSAGE  VARCHAR(5000),

    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
