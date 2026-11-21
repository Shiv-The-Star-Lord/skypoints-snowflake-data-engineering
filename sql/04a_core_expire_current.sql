-- ============================================================
-- PROJECT: SKYPOINTS
-- LAYER: CORE
-- PURPOSE: Expire existing current records before inserting
--          new SCD Type 2 versions
-- ============================================================

UPDATE SKYPOINTS_DE.CORE.MEMBER_PROFILE C
SET
    EFFECTIVE_TO = P.FIRST_NEW_EFFECTIVE_FROM,
    IS_CURRENT = FALSE
FROM
(
    SELECT
        S.MEMBER_NAME,
        MIN(S.INGESTION_TIMESTAMP) AS FIRST_NEW_EFFECTIVE_FROM
    FROM SKYPOINTS_DE.STG.STG_MEMBER_PROFILES S
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM SKYPOINTS_DE.CORE.MEMBER_PROFILE C2
        WHERE C2.SOURCE_BATCH_ID = S.BATCH_ID
          AND C2.SOURCE_ROW_NUMBER = S.SOURCE_ROW_NUMBER
    )
    GROUP BY S.MEMBER_NAME
) P
WHERE C.MEMBER_NAME = P.MEMBER_NAME
  AND C.IS_CURRENT = TRUE;