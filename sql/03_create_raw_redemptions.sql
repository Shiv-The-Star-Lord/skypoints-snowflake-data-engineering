-- ============================================================
-- SQL 18: CREATE RAW REDEMPTIONS TABLE
-- ============================================================
-- Stores raw redemption transaction payloads.
-- Database, schema and table names are resolved from config.
-- ============================================================

CREATE TABLE IF NOT EXISTS __REDEMPTION_RAW_TABLE__
(
    MEMBER_ID            VARCHAR(100),
    FEED_DATE             DATE,
    REDEMPTION_DATA       VARIANT,
    SOURCE_FILE           VARCHAR(500),
    INGESTION_TIMESTAMP   TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID              VARCHAR(100)
);
