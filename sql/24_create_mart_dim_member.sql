-- ============================================================
-- SKYPOINTS MART: DIM_MEMBER
-- Grain: One row per member SCD2 version
-- ============================================================

CREATE TABLE IF NOT EXISTS __DIM_MEMBER_TABLE__ (
    MEMBER_KEY              NUMBER(38,0)
                            AUTOINCREMENT START 1 INCREMENT 1
                            NOT NULL,

    SOURCE_MEMBER_SK_KEY    NUMBER(38,0) NOT NULL,

    MEMBER_ID               VARCHAR(100),
    MEMBER_NAME             VARCHAR(255),
    DATE_OF_BIRTH           DATE,
    TIER_CODE               VARCHAR(50),
    ENROLLMENT_DATE         DATE,
    LAST_FLIGHT_DATE        DATE,
    AGENT_NAME              VARCHAR(255),
    STATE_CODE              VARCHAR(100),
    POSTAL_CODE             VARCHAR(50),
    IS_ACTIVE               VARCHAR(10),
    MEMBER_TYPE             VARCHAR(100),
    COUNTRY_CODE            VARCHAR(10),

    AGE                     NUMBER(3,0),
    STALE_MEMBER            BOOLEAN,

    EFFECTIVE_FROM          TIMESTAMP_NTZ(9),
    EFFECTIVE_TO            TIMESTAMP_NTZ(9),
    IS_CURRENT              BOOLEAN,

    CREATED_TIMESTAMP       TIMESTAMP_NTZ(9)
                            DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_MEMBER PRIMARY KEY (MEMBER_KEY)
);
