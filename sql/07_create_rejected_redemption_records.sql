-- ============================================================
-- SKYPOINTS OPS: REJECTED REDEMPTION RECORDS
-- Purpose: Store invalid and unresolved redemption transactions
--          for investigation and reprocessing
-- ============================================================

CREATE TABLE IF NOT EXISTS __REJECTED_REDEMPTION_TABLE__ (

    REJECTED_RECORD_ID    VARCHAR(100)
                          DEFAULT UUID_STRING(),

    SOURCE_ID             VARCHAR(100),

    MEMBER_ID             VARCHAR(100),
    TXN_ID                VARCHAR(100),
    FEED_DATE             DATE,
    TXN_DATE              DATE,

    PARTNER               VARCHAR(255),
    MILES_REDEEMED        NUMBER(18,2),
    STATUS                VARCHAR(50),

    SOURCE_FILE           VARCHAR(500),
    SOURCE_ROW_NUMBER     NUMBER(38,0),
    BATCH_ID              VARCHAR(100),

    RECORD_DATA           VARIANT,

    ERROR_CHECK           VARCHAR(100),
    ERROR_COLUMN          VARCHAR(100),
    ERROR_MESSAGE         VARCHAR(5000),

    REJECTED_AT           TIMESTAMP_NTZ(9)
                          DEFAULT CURRENT_TIMESTAMP(),

    PROCESSING_STATUS     VARCHAR(30)
                          DEFAULT 'REJECTED',

    RESOLVED_AT           TIMESTAMP_NTZ(9),
    REPROCESS_BATCH_ID    VARCHAR(100)

);
