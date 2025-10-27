{{
    config(
        materialized='incremental',
        unique_key='TREATMENT_TYPE_SK'
    )
}}

WITH TS AS (
    SELECT DISTINCT TREATMENT_TYPE
    FROM {{ref("silver_treatment_plans")}}
    WHERE TREATMENT_TYPE IS NOT NULL
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY TREATMENT_TYPE) TREATMENT_TYPE_SK,
    TREATMENT_TYPE
FROM TS
{% if is_incremental() %}
WHERE TREATMENT_TYPE NOT IN (
    SELECT TREATMENT_TYPE FROM {{this}}
)
{% endif %}