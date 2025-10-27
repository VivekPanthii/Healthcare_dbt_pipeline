{{
    config(
        materialized='incremental',
        unique_key='LOCATION_SK',
    )
}}

WITH source_data AS (
    SELECT
        BRANCH_ID,       
        ADDRESS,
        CITY,
        STATE,
        ZIP_CODE,
        PHONE,
        EMAIL,
        REGION,
        LATITUDE,
        LONGITUDE,
        MD5(
            CONCAT_WS('|',
                COALESCE(ADDRESS, 'null'),
                COALESCE(CITY, 'null'),
                COALESCE(STATE, 'null'),
                COALESCE(ZIP_CODE, 'null'),
                COALESCE(PHONE, 'null'),
                COALESCE(EMAIL, 'null')
            )
        ) AS hash_value
FROM {{ref("silver_hospital_branches")}}
),
existing AS (
    {% if is_incremental() %}
    SELECT *
    FROM {{ this }}
    WHERE IS_CURRENT = TRUE
    {% else %}
    SELECT NULL AS  LOCATION_SK, NULL AS BRANCH_ID, NULL AS ADDRESS,NULL AS CITY, NULL AS STATE,
           NULL AS ZIP_CODE, NULL AS PHONE, NULL AS EMAIL,
           NULL AS REGION, NULL AS LATITUDE, NULL AS LONGITUDE,
           NULL AS EFFECTIVE_FROM, NULL AS EFFECTIVE_TO, NULL AS IS_CURRENT, NULL AS hash_value
    {% endif %}
),
changed AS (
    SELECT s.*
    FROM source_data s 
    LEFT JOIN existing e
    ON s.BRANCH_ID=e.BRANCH_ID
    WHERE s.hash_value<>e.hash_value OR e.hash_value is NULL
),
expired AS (
    SELECT 
        e.LOCATION_SK,
        e.BRANCH_ID,
        e.ADDRESS,
        e.CITY,
        e.STATE,
        e.ZIP_CODE,
        e.PHONE,
        e.EMAIL,
        e.REGION,
        e.LATITUDE,
        e.LONGITUDE,
        e.EFFECTIVE_FROM,
        CURRENT_TIMESTAMP AS EFFECTIVE_TO,
        FALSE IS_CURRENT,
        e.hash_value
    FROM existing e
    INNER JOIN changed c
        ON e.BRANCH_ID=c.BRANCH_ID
),
new_versions AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY BRANCH_ID) AS BRANCH_SK, 
        c.BRANCH_ID,
        c.ADDRESS,
        c.CITY,
        c.STATE,
        c.ZIP_CODE,
        c.PHONE,
        c.EMAIL,
        c.REGION,
        c.LATITUDE,
        c.LONGITUDE,
        '2024-01-01'::DATE  AS EFFECTIVE_FROM,
        NULL AS EFFECTIVE_TO,
        TRUE AS IS_CURRENT,
        c.hash_value
    FROM changed c  
)

SELECT * FROM expired
UNION ALL
SELECT * FROM new_versions