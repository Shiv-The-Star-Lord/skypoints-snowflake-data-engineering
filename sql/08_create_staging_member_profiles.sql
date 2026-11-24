-- ============================================================
-- SQL 19: CREATE STAGING MEMBER PROFILES TABLE
-- ============================================================
-- Stores transformed member records before CORE processing.
-- Database, schema and table names are resolved from config.
-- ============================================================

CREATE TABLE IF NOT EXISTS __MEMBER_STAGING_TABLE__
(
    MEMBER_NAME           VARCHAR(255),
    MEMBER_ID             VARCHAR(100),
    DATE_OF_BIRTH         DATE,
    TIER_CODE             VARCHAR(50),
    ENROLLMENT_DATE       DATE,
    LAST_FLIGHT_DATE      DATE,
    AGENT_NAME            VARCHAR(255),
    STATE_CODE            VARCHAR(100),
    POSTAL_CODE           VARCHAR(50),
    IS_ACTIVE             VARCHAR(10),
    MEMBER_TYPE           VARCHAR(100),
    COUNTRY_CODE          VARCHAR(10),
    RECORD_HASH           VARCHAR(64),
    INGESTION_TIMESTAMP   TIMESTAMP_NTZ(9),
    SOURCE_FILE           VARCHAR(500),
    SOURCE_ROW_NUMBER     NUMBER(38,0),
    BATCH_ID              VARCHAR(100),
    STG_LOAD_TIMESTAMP    TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    AGE                   NUMBER(3,0),
    STALE_MEMBER          BOOLEAN
);
