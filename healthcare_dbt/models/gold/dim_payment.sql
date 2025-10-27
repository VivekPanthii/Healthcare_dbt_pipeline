{{
    config(
        materialized='incremental',
        unique_key='PAYMENT_STATUS_SK'
    )
}}

WITH disp_payment AS(
    SELECT DISTINCT PAYMENT_STATUS
    FROM {{ref("silver_appointments_history")}}
    WHERE APPOINTMENT_TYPE IS NOT NULL
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY PAYMENT_STATUS) AS PAYMENT_STATUS_SK,
    PAYMENT_STATUS
    FROM disp_payment
{% if is_incremental()%}
WHERE PAYMENT_STATUS NOT IN (
    SELECT PAYMENT_STATUS
    FROM {{this}}
    )
{% endif %}