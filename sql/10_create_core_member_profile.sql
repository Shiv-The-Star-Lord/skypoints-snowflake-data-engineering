-- ============================================================
-- SQL 20: CREATE CORE MEMBER PROFILE TABLE
-- ============================================================
-- Stores historical member profile versions using SCD Type 2.
-- Database, schema and table names are resolved from config.
-- ============================================================

CREATE TABLE IF NOT EXISTS __MEMBER_CORE_TABLE__
(
    MEMBER_SK_KEY       NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
    MEMBER_NAME         VARCHAR(255),
    MEMBER_ID           VARCHAR(100),
    DATE_OF_BIRTH       DATE,
    TIER_CODE           VARCHAR(50),
    ENROLLMENT_DATE     DATE,
    LAST_FLIGHT_DATE    DATE,
    AGENT_NAME          VARCHAR(255),
    STATE_CODE          VARCHAR(100),
    POSTAL_CODE         VARCHAR(50),
    IS_ACTIVE           VARCHAR(10),
    MEMBER_TYPE         VARCHAR(100),
    COUNTRY_CODE        VARCHAR(10),
    RECORD_HASH         VARCHAR(64),
    EFFECTIVE_FROM      TIMESTAMP_NTZ(9),
    EFFECTIVE_TO        TIMESTAMP_NTZ(9),
    IS_CURRENT          BOOLEAN,
    CREATED_TIMESTAMP   TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_BATCH_ID     VARCHAR(100),
    SOURCE_ROW_NUMBER   NUMBER(38,0),
    AGE                 NUMBER(3,0),
    STALE_MEMBER        BOOLEAN
);
