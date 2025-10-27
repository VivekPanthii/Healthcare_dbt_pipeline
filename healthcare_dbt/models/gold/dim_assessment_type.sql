{{
    config(
        materialized='incremental',
        unique_key='ASSESSMENT_TYPE_SK'
    )
}}

WITH TS AS (
    SELECT DISTINCT ASSESSMENT_TYPE
    FROM {{ref("silver_mental_health_assessments")}}
    WHERE ASSESSMENT_TYPE IS NOT NULL
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY ASSESSMENT_TYPE) ASSESSMENT_TYPE_SK,
    ASSESSMENT_TYPE
FROM TS
{% if is_incremental() %}
WHERE ASSESSMENT_TYPE NOT IN (
    SELECT ASSESSMENT_TYPE FROM {{this}}
)
{% endif %}