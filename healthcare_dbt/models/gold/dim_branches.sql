{{
    config(
        materialized='incremental',
        unique_key='BRANCH_SK'
    )
}}

WITH source_data AS (
    SELECT
        BRANCH_ID,
        BRANCH_NAME,
        BRANCH_TYPE,
        MANAGER_NAME,
        TOTAL_BEDS,
        EMERGENCY_SERVICES,
        MENTAL_HEALTH_UNIT,
        OPERATING_HOURS,
        ACCREDITATION,
        STATUS,
        MD5(
            CONCAT_WS('|',
                COALESCE(MANAGER_NAME, 'null'),
                COALESCE(TOTAL_BEDS, 'null'),
                COALESCE(ACCREDITATION, 'null'),
                COALESCE(OPERATING_HOURS, 'null'),
                COALESCE(MENTAL_HEALTH_UNIT, 'null'),
                COALESCE(STATUS, 'null')
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
    SELECT NULL AS  BRANCH_SK, NULL AS BRANCH_ID, NULL AS BRANCH_NAME,NULL AS BRANCH_TYPE, NULL AS MANAGER_NAME,
           NULL AS TOTAL_BEDS, NULL AS EMERGENCY_SERVICES, NULL AS MENTAL_HEALTH_UNIT,
           NULL AS OPERATING_HOURS, NULL AS ACCREDITATION, NULL AS STATUS,
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
        e.BRANCH_SK,
        e.BRANCH_ID,
        e.BRANCH_NAME,
        e.BRANCH_TYPE,
        e.MANAGER_NAME,
        e.TOTAL_BEDS,
        e.EMERGENCY_SERVICES,
        e.MENTAL_HEALTH_UNIT,
        e.OPERATING_HOURS,
        e.ACCREDITATION,
        e.STATUS,
        e.EFFECTIVE_FROM,
        CURRENT_TIMESTAMP AS EFFECTIVE_TO,
        FALSE AS IS_CURRENT,
        e.hash_value
    FROM existing e
    INNER JOIN changed c
        ON e.BRANCH_ID=c.BRANCH_ID
),
new_versions AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY BRANCH_ID) AS BRANCH_SK, 
        c.BRANCH_ID,
        c.BRANCH_NAME,
        c.BRANCH_TYPE,
        c.MANAGER_NAME,
        c.TOTAL_BEDS,
        c.EMERGENCY_SERVICES,
        c.MENTAL_HEALTH_UNIT,
        c.OPERATING_HOURS,
        c.ACCREDITATION,
        c.STATUS,
        '2024-01-01'::DATE AS EFFECTIVE_FROM,
        '9999-12-31'::TIMESTAMP AS EFFECTIVE_TO,
        TRUE AS IS_CURRENT,
        c.hash_value

    FROM changed c  
)

SELECT * FROM expired
UNION ALL
SELECT * FROM new_versions