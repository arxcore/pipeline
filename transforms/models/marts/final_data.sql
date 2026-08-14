{{ config (
    materialized = 'table'
) }}

WITH metrix AS (

    SELECT
        curr.id,
        curr.date,
        curr.year,
        curr.source,
        curr.code,
        curr.country,
        curr.category,
        curr.indicator,
        curr.value,
        curr.frequency,
        curr.method,
        curr.unit,
        curr.description,
        curr.processed,
        CASE
            WHEN curr.method IN (
                'net',
                'mom'
            ) THEN prev_mom.value
            WHEN curr.method = 'yoy' THEN prev_yoy.value
        END AS prev_value,
        prev_mom.value AS mom_value,
        curr.value - prev_mom.value AS net_change,
        (
            curr.value - prev_mom.value
        ) * 100.0 / NULLIF (
            prev_mom.value,
            0
        ) AS pct_change_mom,
        prev_yoy.value AS yoy_value,
        (
            curr.value - prev_yoy.value
        ) * 100.0 / NULLIF (
            prev_yoy.value,
            0
        ) AS pct_change_yoy
    FROM
        {{ source (
            'staging',
            'staging_indicators'
        ) }}
        curr
        LEFT JOIN {{ source (
            'staging',
            'staging_indicators'
        ) }}
        prev_mom
        ON curr.country = prev_mom.country
        AND curr.indicator = prev_mom.indicator
        AND curr.method = prev_mom.method
        AND DATE_TRUNC (
            'month',
            prev_mom.date
        ) = DATE_TRUNC (
            'month',
            curr.date - INTERVAL '1 month'
        )
        LEFT JOIN {{ source (
            'staging',
            'staging_indicators'
        ) }}
        prev_yoy
        ON curr.country = prev_yoy.country
        AND curr.indicator = prev_yoy.indicator
        AND curr.method = prev_yoy.method
        AND DATE_TRUNC (
            'month',
            prev_yoy.date
        ) = DATE_TRUNC (
            'month',
            curr.date - INTERVAL '1 year'
        )
)
SELECT
    id,
    DATE,
    YEAR,
    source,
    code,
    INDICATOR,
    country,
    category,
    VALUE,
    prev_value,
    CASE
        WHEN method = 'net' THEN net_change
        WHEN method = 'mom' THEN pct_change_mom
        WHEN method = 'yoy' THEN pct_change_yoy
        WHEN method = 'raw' THEN VALUE
        ELSE NULL
    END AS final_metric,
    frequency,
    method,
    unit,
    description,
    processed
FROM
    metrix
ORDER BY
    DATE DESC
