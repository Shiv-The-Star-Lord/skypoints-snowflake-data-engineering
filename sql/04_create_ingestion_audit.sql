-- ============================================================
-- SQL 21: CREATE INGESTION AUDIT TABLE
-- ============================================================
-- Tracks source file ingestion, processing status and errors.
-- Database, schema and table names are resolved from config.
-- ============================================================

CREATE TABLE IF NOT EXISTS __INGESTION_AUDIT_TABLE__
(
    BATCH_ID          VARCHAR(100),
    SOURCE_ID         VARCHAR(100),
    SOURCE_FILE       VARCHAR(500),
    FILE_CHECKSUM     VARCHAR(64),
    ENTITY_TYPE       VARCHAR(100),
    FILE_SIZE_BYTES   NUMBER(38,0),
    RECORD_COUNT      NUMBER(38,0),
    STATUS            VARCHAR(30),
    STARTED_AT        TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT      TIMESTAMP_NTZ(9),
    ERROR_MESSAGE     VARCHAR(5000)
);
