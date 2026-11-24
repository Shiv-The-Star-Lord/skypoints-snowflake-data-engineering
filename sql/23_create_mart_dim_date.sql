-- ============================================================
-- SKYPOINTS MART: DIM_DATE
-- Grain: One row per calendar date
-- ============================================================

CREATE TABLE IF NOT EXISTS __DIM_DATE_TABLE__ (
    DATE_KEY                NUMBER(8,0) NOT NULL,
    FULL_DATE               DATE NOT NULL,

    DAY_NUMBER              NUMBER(2,0),
    DAY_NAME                VARCHAR(10),
    DAY_OF_WEEK             NUMBER(1,0),
    WEEK_NUMBER             NUMBER(2,0),

    MONTH_NUMBER            NUMBER(2,0),
    MONTH_NAME              VARCHAR(10),
    YEAR_MONTH              VARCHAR(7),

    QUARTER_NUMBER          NUMBER(1,0),
    YEAR_NUMBER             NUMBER(4,0),

    FISCAL_MONTH_NUMBER     NUMBER(2,0),
    FISCAL_QUARTER          VARCHAR(2),
    FISCAL_YEAR             NUMBER(4,0),

    IS_WEEKEND              BOOLEAN,

    CREATED_TIMESTAMP       TIMESTAMP_NTZ(9)
                            DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_DIM_DATE PRIMARY KEY (DATE_KEY)
);
