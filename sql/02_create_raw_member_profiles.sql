-- ============================================================
-- SQL 17: CREATE RAW MEMBER PROFILES TABLE
-- ============================================================
-- Creates the RAW member profiles ingestion table.
-- Database, schema and table names are resolved from config.
-- ============================================================

CREATE TABLE IF NOT EXISTS __MEMBER_RAW_TABLE__
(
    MEMBER_ID            VARCHAR(100),
    MEMBER_NAME          VARCHAR(255),
    DATE_OF_BIRTH        DATE,
    TIER_CODE            VARCHAR(50),
    ENROLLMENT_DATE      DATE,
    LAST_FLIGHT_DATE     DATE,
    AGENT_NAME           VARCHAR(255),
    STATE_CODE           VARCHAR(100),
    POSTAL_CODE          VARCHAR(50),
    IS_ACTIVE            VARCHAR(10),
    MEMBER_TYPE          VARCHAR(100),
    COUNTRY_CODE         VARCHAR(10),
    SOURCE_FILE          VARCHAR(500),
    SOURCE_ROW_NUMBER    NUMBER(38,0),
    INGESTION_TIMESTAMP  TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID             VARCHAR(100)
);
