
-- ============================================================
-- SQL 16: CREATE REJECTED MEMBER RECORDS TABLE
-- ============================================================
-- Stores rejected source records and validation errors.
-- Object name is resolved from config/source_config.json.
-- ============================================================

CREATE TABLE IF NOT EXISTS __REJECTED_MEMBER_RECORDS_TABLE__
(
    REJECTED_RECORD_ID VARCHAR(100) DEFAULT UUID_STRING(),
    SOURCE_ID VARCHAR(100),
    SOURCE_FILE VARCHAR(500),
    SOURCE_ROW_NUMBER NUMBER,
    BATCH_ID VARCHAR(100),

    RECORD_DATA VARIANT,

    ERROR_CHECK VARCHAR(100),
    ERROR_COLUMN VARCHAR(100),
    ERROR_MESSAGE VARCHAR(5000),

    REJECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    PROCESSING_STATUS VARCHAR(30) DEFAULT 'REJECTED',
    RESOLVED_AT TIMESTAMP_NTZ,
    REPROCESS_BATCH_ID VARCHAR(100)
);
