{{
    config(
        materialized='incremental',
        unique_key='MEDICATION_NAME_SK'
    )
}}

WITH TS AS (
    SELECT DISTINCT MEDICATION_NAME
    FROM {{ref("silver_treatment_plans")}}
    WHERE MEDICATION_NAME IS NOT NULL
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY MEDICATION_NAME) MEDICATION_NAME_SK,
    MEDICATION_NAME
FROM TS
{% if is_incremental() %}
WHERE MEDICATION_NAME NOT IN (
    SELECT MEDICATION_NAME FROM {{this}}
)
{% endif %}