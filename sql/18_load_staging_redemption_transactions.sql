
MERGE INTO __REDEMPTION_STAGING_TABLE__ AS target

USING (
    WITH FLATTENED_TRANSACTIONS AS (
        SELECT
            NULLIF(TRIM(r.MEMBER_ID), '') AS MEMBER_ID,
            r.FEED_DATE,

            NULLIF(TRIM(f.VALUE:txn_id::VARCHAR), '') AS TXN_ID,

            TRY_TO_DATE(
                NULLIF(TRIM(f.VALUE:txn_date::VARCHAR), ''),
                'YYYYMMDD'
            ) AS TXN_DATE,

            NULLIF(TRIM(f.VALUE:partner::VARCHAR), '') AS PARTNER,

            NULLIF(TRIM(f.VALUE:miles_redeemed::VARCHAR), '') AS RAW_MILES_REDEEMED,

            TRY_TO_DECIMAL(
                NULLIF(TRIM(f.VALUE:miles_redeemed::VARCHAR), ''),
                18,
                2
            ) AS MILES_REDEEMED,

            NULLIF(TRIM(f.VALUE:status::VARCHAR), '') AS STATUS,

            f.INDEX AS TRANSACTION_INDEX,
            r.SOURCE_FILE,
            r.INGESTION_TIMESTAMP,
            r.BATCH_ID

        FROM __REDEMPTION_RAW_TABLE__ r,
        LATERAL FLATTEN(INPUT => r.REDEMPTION_DATA) f
    ),

    VALID_TRANSACTIONS AS (
        SELECT *
        FROM FLATTENED_TRANSACTIONS
        WHERE MEMBER_ID IS NOT NULL
          AND FEED_DATE IS NOT NULL
          AND TXN_ID IS NOT NULL
          AND TXN_DATE IS NOT NULL
          AND (
              RAW_MILES_REDEEMED IS NULL
              OR MILES_REDEEMED IS NOT NULL
          )
          AND (
              MILES_REDEEMED IS NULL
              OR MILES_REDEEMED >= 0
          )
    ),

    MEMBER_MATCHES AS (
        SELECT
            MEMBER_ID,
            COUNT(DISTINCT NULLIF(TRIM(MEMBER_NAME), ''))
                AS MATCHING_MEMBER_COUNT
        FROM __MEMBER_CORE_TABLE__
        WHERE IS_CURRENT = TRUE
        GROUP BY MEMBER_ID
    ),

    ELIGIBLE_TRANSACTIONS AS (
        SELECT t.*
        FROM VALID_TRANSACTIONS t
        INNER JOIN MEMBER_MATCHES m
            ON t.MEMBER_ID = m.MEMBER_ID
        WHERE m.MATCHING_MEMBER_COUNT = 1
    ),

    LATEST_VALID_TRANSACTIONS AS (
        SELECT *
        FROM ELIGIBLE_TRANSACTIONS

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY MEMBER_ID, TXN_ID
            ORDER BY
                INGESTION_TIMESTAMP DESC,
                BATCH_ID DESC
        ) = 1
    )

    SELECT *
    FROM LATEST_VALID_TRANSACTIONS

) AS source

ON target.MEMBER_ID = source.MEMBER_ID
AND target.TXN_ID = source.TXN_ID

WHEN MATCHED AND (
       target.FEED_DATE IS DISTINCT FROM source.FEED_DATE
    OR target.TXN_DATE IS DISTINCT FROM source.TXN_DATE
    OR target.PARTNER IS DISTINCT FROM source.PARTNER
    OR target.MILES_REDEEMED IS DISTINCT FROM source.MILES_REDEEMED
    OR target.STATUS IS DISTINCT FROM source.STATUS
    OR target.TRANSACTION_INDEX IS DISTINCT FROM source.TRANSACTION_INDEX
)
THEN UPDATE SET
    target.FEED_DATE = source.FEED_DATE,
    target.TXN_DATE = source.TXN_DATE,
    target.PARTNER = source.PARTNER,
    target.MILES_REDEEMED = source.MILES_REDEEMED,
    target.STATUS = source.STATUS,
    target.TRANSACTION_INDEX = source.TRANSACTION_INDEX,
    target.SOURCE_FILE = source.SOURCE_FILE,
    target.INGESTION_TIMESTAMP = source.INGESTION_TIMESTAMP,
    target.BATCH_ID = source.BATCH_ID

WHEN NOT MATCHED THEN INSERT (
    MEMBER_ID,
    FEED_DATE,
    TXN_ID,
    TXN_DATE,
    PARTNER,
    MILES_REDEEMED,
    STATUS,
    TRANSACTION_INDEX,
    SOURCE_FILE,
    INGESTION_TIMESTAMP,
    BATCH_ID
)
VALUES (
    source.MEMBER_ID,
    source.FEED_DATE,
    source.TXN_ID,
    source.TXN_DATE,
    source.PARTNER,
    source.MILES_REDEEMED,
    source.STATUS,
    source.TRANSACTION_INDEX,
    source.SOURCE_FILE,
    source.INGESTION_TIMESTAMP,
    source.BATCH_ID
);
