{{
    config(
        materialized='incremental',
        unique_key='DIAGNOSIS_CODE_SK'
    )
}}

WITH TS AS (
    SELECT DISTINCT DIAGNOSIS_CODE,
    SEVERITY_LEVEL
    FROM {{ref("silver_mental_health_assessments")}}
    WHERE DIAGNOSIS_CODE IS NOT NULL
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY DIAGNOSIS_CODE) DIAGNOSIS_CODE_SK,
    DIAGNOSIS_CODE,
    SEVERITY_LEVEL
FROM TS
{% if is_incremental() %}
WHERE DIAGNOSIS_CODE NOT IN (
    SELECT DIAGNOSIS_CODE FROM {{this}}
)
{% endif %}