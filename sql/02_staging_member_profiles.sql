
-- ============================================================
-- PROJECT: SKYPOINTS
-- LAYER: STAGING (STG)
-- PURPOSE:
--   1. Create the STG member profile table.
--   2. Generate a hash for each member profile.
--   3. Identify duplicate and changed profile versions.
--   4. Preserve distinct profile versions for CORE SCD Type 2.
--   5. Prevent previously processed RAW records from loading again.
-- ============================================================


-- ============================================================
-- STEP 1: CREATE STG MEMBER PROFILE TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS SKYPOINTS_DE.STG.STG_MEMBER_PROFILES
(
    MEMBER_NAME          VARCHAR(255),
    MEMBER_ID            VARCHAR(100),
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

    RECORD_HASH          VARCHAR(64),

    INGESTION_TIMESTAMP  TIMESTAMP_NTZ,
    SOURCE_FILE          VARCHAR(500),
    SOURCE_ROW_NUMBER    NUMBER(38,0),
    BATCH_ID             VARCHAR(100),

    STG_LOAD_TIMESTAMP   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ============================================================
-- STEP 2: HASH, DETECT CHANGES, AND INSERT DISTINCT VERSIONS
-- ============================================================

INSERT INTO SKYPOINTS_DE.STG.STG_MEMBER_PROFILES
(
    MEMBER_NAME,
    MEMBER_ID,
    DATE_OF_BIRTH,
    TIER_CODE,
    ENROLLMENT_DATE,
    LAST_FLIGHT_DATE,
    AGENT_NAME,
    STATE_CODE,
    POSTAL_CODE,
    IS_ACTIVE,
    MEMBER_TYPE,
    COUNTRY_CODE,
    RECORD_HASH,
    INGESTION_TIMESTAMP,
    SOURCE_FILE,
    SOURCE_ROW_NUMBER,
    BATCH_ID
)

WITH HASHED_RAW AS
(
    SELECT
        *,
        SHA2(
            TO_JSON(
                ARRAY_CONSTRUCT(
                    MEMBER_NAME,
                    MEMBER_ID,
                    DATE_OF_BIRTH,
                    TIER_CODE,
                    ENROLLMENT_DATE,
                    LAST_FLIGHT_DATE,
                    AGENT_NAME,
                    STATE_CODE,
                    POSTAL_CODE,
                    IS_ACTIVE,
                    MEMBER_TYPE,
                    COUNTRY_CODE
                )
            ),
            256
        ) AS RECORD_HASH

    FROM SKYPOINTS_DE.RAW.RAW_MEMBER_PROFILES
),

ORDERED_RAW AS
(
    SELECT
        *,
        LAG(RECORD_HASH) OVER
        (
            PARTITION BY MEMBER_NAME
            ORDER BY
                INGESTION_TIMESTAMP,
                BATCH_ID,
                SOURCE_ROW_NUMBER
        ) AS PREVIOUS_RECORD_HASH

    FROM HASHED_RAW
),

DEDUPLICATED AS
(
    SELECT *
    FROM ORDERED_RAW
    WHERE
        PREVIOUS_RECORD_HASH IS NULL
        OR RECORD_HASH <> PREVIOUS_RECORD_HASH
)

SELECT
    R.MEMBER_NAME,
    R.MEMBER_ID,
    R.DATE_OF_BIRTH,
    R.TIER_CODE,
    R.ENROLLMENT_DATE,
    R.LAST_FLIGHT_DATE,
    R.AGENT_NAME,
    R.STATE_CODE,
    R.POSTAL_CODE,
    R.IS_ACTIVE,
    R.MEMBER_TYPE,
    R.COUNTRY_CODE,
    R.RECORD_HASH,
    R.INGESTION_TIMESTAMP,
    R.SOURCE_FILE,
    R.SOURCE_ROW_NUMBER,
    R.BATCH_ID

FROM DEDUPLICATED R

WHERE NOT EXISTS
(
    SELECT 1
    FROM SKYPOINTS_DE.STG.STG_MEMBER_PROFILES S
    WHERE S.BATCH_ID = R.BATCH_ID
      AND S.SOURCE_ROW_NUMBER = R.SOURCE_ROW_NUMBER
);


-- ============================================================
-- STEP 3: VALIDATE STG MEMBER PROFILE RECORDS
-- ============================================================

-- Check all distinct member profile versions.

SELECT
    MEMBER_NAME,
    MEMBER_ID,
    COUNTRY_CODE,
    RECORD_HASH,
    INGESTION_TIMESTAMP,
    SOURCE_FILE,
    BATCH_ID
FROM SKYPOINTS_DE.STG.STG_MEMBER_PROFILES
ORDER BY
    MEMBER_NAME,
    INGESTION_TIMESTAMP;


-- Check the number of profile versions per member.

SELECT
    MEMBER_NAME,
    COUNT(*) AS VERSION_COUNT,
    COUNT(DISTINCT RECORD_HASH) AS DISTINCT_VERSIONS
FROM SKYPOINTS_DE.STG.STG_MEMBER_PROFILES
GROUP BY MEMBER_NAME
ORDER BY MEMBER_NAME;


-- ============================================================
-- END OF STG TRANSFORMATION
-- ============================================================
