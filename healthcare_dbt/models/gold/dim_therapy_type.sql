{{
    config(
        materialized='incremental',
        unique_key='THERAPY_TYPE_SK'
    )
}}

WITH TS AS (
    SELECT DISTINCT THERAPY_TYPE
    FROM {{ref("silver_treatment_plans")}}
    WHERE THERAPY_TYPE IS NOT NULL
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY THERAPY_TYPE) THERAPY_TYPE_SK,
    THERAPY_TYPE
FROM TS
{% if is_incremental() %}
WHERE THERAPY_TYPE NOT IN (
    SELECT THERAPY_TYPE FROM {{this}}
)
{% endif %}