{{
    config(
        materialized='incremental',
        unique_key='APPOINTMENT_TYPE_SK'
    )
}}

WITH DIST AS (
    SELECT DISTINCT APPOINTMENT_TYPE 
    FROM  {{ref('silver_appointments_history')}}
    WHERE APPOINTMENT_TYPE IS NOT NULL
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY APPOINTMENT_TYPE) AS APPOINTMENT_TYPE_SK,
    APPOINTMENT_TYPE
FROM DIST
    {% if is_incremental()%}
    WHERE APPOINTMENT_TYPE NOT IN (
        SELECT APPOINTMENT_TYPE
        FROM {{this}}
    )
    {% endif %}
   