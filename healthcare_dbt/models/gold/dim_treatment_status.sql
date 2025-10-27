{{
    config(
        materialized='incremental',
        unique_key='TREATMENT_STATUS_SK'
    )
}}

WITH TS AS (
    SELECT DISTINCT TREATMENT_STATUS
    FROM {{ref("silver_treatment_plans")}}
    WHERE TREATMENT_STATUS IS NOT NULL
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY TREATMENT_STATUS) TREATMENT_STATUS_SK,
    TREATMENT_STATUS
FROM TS
{% if is_incremental() %}
WHERE TREATMENT_STATUS NOT IN (
    SELECT TREATMENT_STATUS FROM {{this}}
)
{% endif %}