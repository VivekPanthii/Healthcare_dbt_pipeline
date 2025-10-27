{{
    config(
        materialized='incremental',
        unique_key='PRACTITIONER_SK',
    )
}}

WITH source_data AS (
SELECT
    DOCTOR_ID,
    FULL_NAME,
    SPECIALIZATION,
    QUALIFICATION,
    LICENSE_NUMBER,
    YEARS_OF_EXPERIENCE,
    PRIMARY_BRANCH_ID,
    SECONDARY_BRANCH_ID,
    PHONE,
    EMAIL,
    HIRE_DATE,
    EMPLOYMENT_STATUS,
    CONSULTATION_FEE,
    RATING,
    LANGUAGES_SPOKEN,
    WORKING_HOURS,
    EMERGENCY_AVAILABILITY,
    MD5(
        CONCAT_WS('|',


            COALESCE(QUALIFICATION, 'null'),
            COALESCE(YEARS_OF_EXPERIENCE::TEXT, 'null'),
            COALESCE(PRIMARY_BRANCH_ID, 'null'),
            COALESCE(SECONDARY_BRANCH_ID, 'null'),
            COALESCE(EMPLOYMENT_STATUS, 'null'),
            COALESCE(CONSULTATION_FEE::TEXT, 'null'),
            COALESCE(RATING::TEXT, 'null'),
            COALESCE(WORKING_HOURS, 'null'),
            COALESCE(
                EMERGENCY_AVAILABILITY,
                'null'
            )
        )
    ) AS hash_value


FROM {{ref("silver_medical_practioners")}}
),
existing AS (
    {% if is_incremental() %}
    SELECT *
    FROM {{ this }}
    WHERE IS_CURRENT = TRUE
    {% else %}
    SELECT NULL AS PRACTITIONER_SK, NULL AS DOCTOR_ID, NULL AS FULL_NAME, NULL AS SPECIALIZATION, NULL AS QUALIFICATION,
           NULL AS LICENSE_NUMBER, NULL AS YEARS_OF_EXPERIENCE, NULL AS PRIMARY_BRANCH_ID,
           NULL AS SECONDARY_BRANCH_ID, NULL AS PHONE, NULL AS EMAIL, NULL AS HIRE_DATE,
           NULL AS EMPLOYMENT_STATUS, NULL AS CONSULTATION_FEE, NULL AS RATING,
           NULL AS LANGUAGES_SPOKEN, NULL AS WORKING_HOURS, NULL AS EMERGENCY_AVAILABILITY,
           NULL AS EFFECTIVE_FROM, NULL AS EFFECTIVE_TO, NULL AS IS_CURRENT, NULL AS hash_value
    {% endif %}
),
changed AS (
    SELECT s.*
    FROM source_data s 
    LEFT JOIN existing e
    ON s.DOCTOR_ID=e.DOCTOR_ID
    WHERE s.hash_value<>e.hash_value OR e.hash_value is NULL
),
expired AS (
    SELECT 
        e.PRACTITIONER_SK,
        e.DOCTOR_ID,
        e.FULL_NAME,
        e.SPECIALIZATION,
        e.QUALIFICATION,
        e.LICENSE_NUMBER,
        e.YEARS_OF_EXPERIENCE,
        e.PRIMARY_BRANCH_ID,
        e.SECONDARY_BRANCH_ID,
        e.PHONE,
        e.EMAIL,
        e.HIRE_DATE,
        e.EMPLOYMENT_STATUS,
        e.CONSULTATION_FEE,
        e.RATING,
        e.LANGUAGES_SPOKEN,
        e.WORKING_HOURS,
        e.EMERGENCY_AVAILABILITY,
        e.EFFECTIVE_FROM,
        CURRENT_TIMESTAMP AS EFFECTIVE_TO,
        FALSE AS IS_CURRENT,
        e.hash_value
    FROM existing e
    INNER JOIN changed c
        ON e.DOCTOR_ID=c.DOCTOR_ID
),
new_versions AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY DOCTOR_ID) AS PRACTITIONER_SK, 
        c.DOCTOR_ID,
        c.FULL_NAME,
        c.SPECIALIZATION,
        c.QUALIFICATION,
        c.LICENSE_NUMBER,
        c.YEARS_OF_EXPERIENCE,
        c.PRIMARY_BRANCH_ID,
        c.SECONDARY_BRANCH_ID,
        c.PHONE,
        c.EMAIL,
        c.HIRE_DATE,
        c.EMPLOYMENT_STATUS,
        c.CONSULTATION_FEE,
        c.RATING,
        c.LANGUAGES_SPOKEN,
        c.WORKING_HOURS,
        c.EMERGENCY_AVAILABILITY,
        '2024-01-01'::DATE AS EFFECTIVE_FROM,
        '9999-12-31'::TIMESTAMP AS EFFECTIVE_TO,
        TRUE AS IS_CURRENT,
        c.hash_value
    FROM changed c  
)

SELECT * FROM expired
UNION ALL
SELECT * FROM new_versions

    
