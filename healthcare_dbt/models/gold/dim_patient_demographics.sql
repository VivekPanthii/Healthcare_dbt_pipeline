{{
    config(
        materialized= 'incremental',
        unique_key='PATIENT_SK',
    )
}}
{# Source data with cleaning and hash #}
WITH source_data as (
    SELECT 
        PATIENT_ID,
        FULL_NAME,
        DATE_OF_BIRTH,
        AGE,
        GENDER,
        MARITAL_STATUS,
        EDUCATION_LEVEL,
        OCCUPATION,
        INCOME_ANNUAL,
        PHONE_NUMBER,
        EMAIL,
        ADDRESS_LINE1,
        CITY,
        STATE,
        ZIP_CODE,
        COUNTRY,
        EMERGENCY_CONTACT_NAME,
        EMERGENCY_CONTACT_PHONE,
        INSURANCE_PROVIDER,
        INSURANCE_POLICY_NUMBER,
        REGISTRATION_DATE,
        BRANCH_ID,
        PRIMARY_DOCTOR_ID,
        BLOOD_GROUP,
        HEIGHT_CM,
        WEIGHT_KG,
        BMI,
        BMI_STATUS,
        SMOKING_STATUS,
        ALCOHOL_CONSUMPTION,
        LAST_UPDATED,
        {# hash for change detection #}
        MD5(
            CONCAT_WS('|',
                COALESCE(MARITAL_STATUS, ''),
                COALESCE(EDUCATION_LEVEL, ''),
                COALESCE(OCCUPATION, ''),
                COALESCE(INCOME_ANNUAL, ''),
                COALESCE(PHONE_NUMBER, ''),
                COALESCE(EMAIL, ''),
                COALESCE(ADDRESS_LINE1, ''),
                COALESCE(CITY, ''),
                COALESCE(STATE, ''),
                COALESCE(ZIP_CODE, ''),
                COALESCE(COUNTRY, ''),
                COALESCE(EMERGENCY_CONTACT_NAME, ''),
                COALESCE(EMERGENCY_CONTACT_PHONE, ''),
                COALESCE(INSURANCE_PROVIDER, ''),
                COALESCE(INSURANCE_POLICY_NUMBER, ''),
                COALESCE(BRANCH_ID, ''),
                COALESCE(PRIMARY_DOCTOR_ID, ''),
                COALESCE(HEIGHT_CM, ''),
                COALESCE(WEIGHT_KG, ''),
                COALESCE(BMI_STATUS, ''),
                COALESCE(SMOKING_STATUS, ''),
                COALESCE(ALCOHOL_CONSUMPTION, ''),
                COALESCE(LAST_UPDATED, '')
            )
        ) AS hash_value


    FROM 
        {{ref('silver_patient_demographics')}}
),
existing AS (
    {% if is_incremental() %}
    SELECT *
    FROM {{ this }}
    WHERE IS_CURRENT = TRUE
    {% else %}
    SELECT NULL AS PATIENT_SK,NULL AS PATIENT_ID, NULL AS FULL_NAME, NULL AS DATE_OF_BIRTH, NULL AS AGE, NULL AS GENDER,
           NULL AS MARITAL_STATUS, NULL AS EDUCATION_LEVEL, NULL AS OCCUPATION,
           NULL AS INCOME_ANNUAL, NULL AS PHONE_NUMBER, NULL AS EMAIL, NULL AS ADDRESS_LINE1,
           NULL AS CITY, NULL AS STATE, NULL AS ZIP_CODE,
           NULL AS COUNTRY, NULL AS EMERGENCY_CONTACT_NAME, NULL AS EMERGENCY_CONTACT_PHONE,
           NULL AS INSURANCE_PROVIDER,NULL AS INSURANCE_POLICY_NUMBER,NULL AS REGISTRATION_DATE, NULL AS BRANCH_ID,
           NULL AS PRIMARY_DOCTOR_ID, NULL AS BLOOD_GROUP, NULL AS HEIGHT_CM, NULL AS WEIGHT_KG,
           NULL AS BMI, NULL AS BMI_STATUS, NULL AS SMOKING_STATUS, NULL AS ALCOHOL_CONSUMPTION,
           NULL AS LAST_UPDATED,
           NULL AS EFFECTIVE_FROM, NULL AS EFFECTIVE_TO, NULL AS IS_CURRENT, NULL AS hash_value
    {% endif %}
),
changed AS (
    SELECT s.*
    FROM source_data s 
    LEFT JOIN existing e
    ON s.PATIENT_ID=e.PATIENT_ID
    WHERE s.hash_value<>e.hash_value OR e.hash_value is NULL
),
expired AS (
    SELECT 
        e.PATIENT_SK,
        e.PATIENT_ID,
        e.FULL_NAME,
        e.DATE_OF_BIRTH,
        e.AGE,
        e.GENDER,
        e.MARITAL_STATUS,
        e.EDUCATION_LEVEL,
        e.OCCUPATION,
        e.INCOME_ANNUAL,
        e.PHONE_NUMBER,
        e.EMAIL,
        e.ADDRESS_LINE1,
        e.CITY,
        e.STATE,
        e.ZIP_CODE,
        e.COUNTRY,
        e.EMERGENCY_CONTACT_NAME,
        e.EMERGENCY_CONTACT_PHONE,
        e.INSURANCE_PROVIDER,
        e.INSURANCE_POLICY_NUMBER,
        e.REGISTRATION_DATE,
        e.BRANCH_ID,
        e.PRIMARY_DOCTOR_ID,
        e.BLOOD_GROUP,
        e.HEIGHT_CM,
        e.WEIGHT_KG,
        e.BMI,
        e.BMI_STATUS,
        e.SMOKING_STATUS,
        e.ALCOHOL_CONSUMPTION,
        e.LAST_UPDATED,
        e.EFFECTIVE_FROM,
        CURRENT_TIMESTAMP AS EFFECTIVE_TO,
        FALSE AS IS_CURRENT,
        e.hash_value
    FROM existing e
    INNER JOIN changed c
        ON e.PATIENT_ID=c.PATIENT_ID
),
new_versions AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY PATIENT_ID) AS PATIENT_SK, 
        c.PATIENT_ID,
        c.FULL_NAME,
        c.DATE_OF_BIRTH,
        c.AGE,
        c.GENDER,
        c.MARITAL_STATUS,
        c.EDUCATION_LEVEL,
        c.OCCUPATION,
        c.INCOME_ANNUAL,
        c.PHONE_NUMBER,
        c.EMAIL,
        c.ADDRESS_LINE1,
        c.CITY,
        c.STATE,
        c.ZIP_CODE,
        c.COUNTRY,
        c.EMERGENCY_CONTACT_NAME,
        c.EMERGENCY_CONTACT_PHONE,
        c.INSURANCE_PROVIDER,
        c.INSURANCE_POLICY_NUMBER,
        c.REGISTRATION_DATE,
        c.BRANCH_ID,
        c.PRIMARY_DOCTOR_ID,
        c.BLOOD_GROUP,
        c.HEIGHT_CM,
        c.WEIGHT_KG,
        c.BMI,
        c.BMI_STATUS,
        c.SMOKING_STATUS,
        c.ALCOHOL_CONSUMPTION,
        c.LAST_UPDATED,
        '2024-01-01'::DATE AS EFFECTIVE_FROM,
        '9999-12-31'::TIMESTAMP AS EFFECTIVE_TO,
        TRUE AS IS_CURRENT,
        c.hash_value
    FROM changed c  
)


SELECT * FROM expired
UNION ALL
SELECT * FROM new_versions
