-- ============================================================
-- SKYPOINTS MART: FACT_REDEMPTION
-- Grain: One row per unique redemption transaction
-- ============================================================

CREATE TABLE IF NOT EXISTS __FACT_REDEMPTION_TABLE__ (
    REDEMPTION_KEY          NUMBER(38,0)
                            AUTOINCREMENT START 1 INCREMENT 1
                            NOT NULL,

    SOURCE_REDEMPTION_SK    NUMBER(38,0) NOT NULL,

    MEMBER_KEY              NUMBER(38,0) NOT NULL,
    DATE_KEY                NUMBER(8,0) NOT NULL,

    MEMBER_ID               VARCHAR(100) NOT NULL,
    FEED_DATE               DATE NOT NULL,
    TXN_ID                  VARCHAR(100) NOT NULL,
    TXN_DATE                DATE NOT NULL,

    PARTNER                 VARCHAR(255),
    MILES_REDEEMED          NUMBER(18,2),
    STATUS                  VARCHAR(50),

    TRANSACTION_INDEX       NUMBER(38,0),
    SOURCE_FILE             VARCHAR(500),
    BATCH_ID                VARCHAR(100),

    CREATED_TIMESTAMP       TIMESTAMP_NTZ(9)
                            DEFAULT CURRENT_TIMESTAMP(),

    UPDATED_TIMESTAMP       TIMESTAMP_NTZ(9),

    CONSTRAINT PK_FACT_REDEMPTION
        PRIMARY KEY (REDEMPTION_KEY)
);
