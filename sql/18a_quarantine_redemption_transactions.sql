MERGE INTO __REJECTED_REDEMPTION_TABLE__ AS target

USING (
    WITH FLATTENED_TRANSACTIONS AS (
        SELECT
            NULLIF(TRIM(r.MEMBER_ID), '') AS MEMBER_ID,
            r.FEED_DATE,

            NULLIF(
                TRIM(f.VALUE:txn_id::VARCHAR),
                ''
            ) AS TXN_ID,

            NULLIF(
                TRIM(f.VALUE:txn_date::VARCHAR),
                ''
            ) AS RAW_TXN_DATE,

            TRY_TO_DATE(
                NULLIF(TRIM(f.VALUE:txn_date::VARCHAR), ''),
                'YYYYMMDD'
            ) AS TXN_DATE,

            NULLIF(
                TRIM(f.VALUE:partner::VARCHAR),
                ''
            ) AS PARTNER,

            NULLIF(
                TRIM(f.VALUE:miles_redeemed::VARCHAR),
                ''
            ) AS RAW_MILES_REDEEMED,

            TRY_TO_DECIMAL(
                NULLIF(TRIM(f.VALUE:miles_redeemed::VARCHAR), ''),
                18,
                2
            ) AS MILES_REDEEMED,

            NULLIF(
                TRIM(f.VALUE:status::VARCHAR),
                ''
            ) AS STATUS,

            f.INDEX + 1 AS SOURCE_ROW_NUMBER,

            f.VALUE AS RECORD_DATA,

            r.SOURCE_FILE,
            r.INGESTION_TIMESTAMP,
            r.BATCH_ID

        FROM __REDEMPTION_RAW_TABLE__ r,
        LATERAL FLATTEN(
            INPUT => r.REDEMPTION_DATA
        ) f
    ),

    LATEST_TRANSACTIONS AS (
        SELECT *
        FROM FLATTENED_TRANSACTIONS

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY
                MEMBER_ID,
                COALESCE(
                    TXN_ID,
                    CONCAT(
                        'MISSING_TXN_ID_',
                        BATCH_ID,
                        ':',
                        SOURCE_ROW_NUMBER
                    )
                )
            ORDER BY
                INGESTION_TIMESTAMP DESC,
                BATCH_ID DESC
        ) = 1
    ),

    MEMBER_MATCHES AS (
        SELECT
            MEMBER_ID,
            COUNT(
                DISTINCT NULLIF(TRIM(MEMBER_NAME), '')
            ) AS MATCHING_MEMBER_COUNT

        FROM __MEMBER_CORE_TABLE__

        WHERE IS_CURRENT = TRUE

        GROUP BY MEMBER_ID
    ),

    CLASSIFIED_TRANSACTIONS AS (
        SELECT
            t.*,

            a.SOURCE_ID,

            COALESCE(
                m.MATCHING_MEMBER_COUNT,
                0
            ) AS MATCHING_MEMBER_COUNT,

            CASE
                WHEN t.MEMBER_ID IS NULL
                    THEN 'REQUIRED_FIELD'

                WHEN t.FEED_DATE IS NULL
                    THEN 'REQUIRED_FIELD'

                WHEN t.TXN_ID IS NULL
                    THEN 'REQUIRED_FIELD'

                WHEN t.RAW_TXN_DATE IS NULL
                    THEN 'REQUIRED_FIELD'

                WHEN t.TXN_DATE IS NULL
                    THEN 'INVALID_DATE'

                WHEN t.RAW_MILES_REDEEMED IS NOT NULL
                     AND t.MILES_REDEEMED IS NULL
                    THEN 'INVALID_AMOUNT'

                WHEN t.MILES_REDEEMED < 0
                    THEN 'NEGATIVE_AMOUNT'

                WHEN COALESCE(
                    m.MATCHING_MEMBER_COUNT,
                    0
                ) = 0
                    THEN 'MEMBER_NOT_FOUND'

                WHEN m.MATCHING_MEMBER_COUNT > 1
                    THEN 'AMBIGUOUS_MEMBER_REFERENCE'

                ELSE NULL
            END AS ERROR_CHECK,

            CASE
                WHEN t.MEMBER_ID IS NULL
                    THEN 'MEMBER_ID'

                WHEN t.FEED_DATE IS NULL
                    THEN 'FEED_DATE'

                WHEN t.TXN_ID IS NULL
                    THEN 'TXN_ID'

                WHEN t.RAW_TXN_DATE IS NULL
                    OR t.TXN_DATE IS NULL
                    THEN 'TXN_DATE'

                WHEN t.RAW_MILES_REDEEMED IS NOT NULL
                     AND t.MILES_REDEEMED IS NULL
                    THEN 'MILES_REDEEMED'

                WHEN t.MILES_REDEEMED < 0
                    THEN 'MILES_REDEEMED'

                WHEN COALESCE(
                    m.MATCHING_MEMBER_COUNT,
                    0
                ) = 0
                    OR m.MATCHING_MEMBER_COUNT > 1
                    THEN 'MEMBER_ID'

                ELSE NULL
            END AS ERROR_COLUMN

        FROM LATEST_TRANSACTIONS t

        LEFT JOIN MEMBER_MATCHES m
            ON t.MEMBER_ID = m.MEMBER_ID

        LEFT JOIN __INGESTION_AUDIT_TABLE__ a
            ON t.BATCH_ID = a.BATCH_ID
    )

    SELECT
        *
    FROM CLASSIFIED_TRANSACTIONS
    WHERE ERROR_CHECK IS NOT NULL

) AS source

ON target.BATCH_ID = source.BATCH_ID
AND target.SOURCE_ROW_NUMBER = source.SOURCE_ROW_NUMBER
AND target.ERROR_CHECK = source.ERROR_CHECK

WHEN NOT MATCHED THEN INSERT (
    SOURCE_ID,
    MEMBER_ID,
    TXN_ID,
    FEED_DATE,
    TXN_DATE,
    PARTNER,
    MILES_REDEEMED,
    STATUS,
    SOURCE_FILE,
    SOURCE_ROW_NUMBER,
    BATCH_ID,
    RECORD_DATA,
    ERROR_CHECK,
    ERROR_COLUMN,
    ERROR_MESSAGE
)

VALUES (
    source.SOURCE_ID,
    source.MEMBER_ID,
    source.TXN_ID,
    source.FEED_DATE,
    source.TXN_DATE,
    source.PARTNER,
    source.MILES_REDEEMED,
    source.STATUS,
    source.SOURCE_FILE,
    source.SOURCE_ROW_NUMBER,
    source.BATCH_ID,
    source.RECORD_DATA,
    source.ERROR_CHECK,
    source.ERROR_COLUMN,

    CASE source.ERROR_CHECK
        WHEN 'REQUIRED_FIELD'
            THEN 'A mandatory transaction field is missing.'

        WHEN 'INVALID_DATE'
            THEN 'Transaction date is not a valid YYYYMMDD date.'

        WHEN 'INVALID_AMOUNT'
            THEN 'Miles redeemed is not a valid decimal amount.'

        WHEN 'NEGATIVE_AMOUNT'
            THEN 'Miles redeemed cannot be negative.'

        WHEN 'MEMBER_NOT_FOUND'
            THEN 'No current member matches the supplied MEMBER_ID.'

        WHEN 'AMBIGUOUS_MEMBER_REFERENCE'
            THEN 'MEMBER_ID matches multiple distinct current members.'

        ELSE 'Transaction failed validation.'
    END
);