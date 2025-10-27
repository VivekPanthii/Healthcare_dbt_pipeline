    SELECT 
        PATIENT_ID,
        FULL_NAME,
        CASE 
            WHEN  DATEDIFF(YEAR,DATE_OF_BIRTH,CURRENT_DATE)<=100 THEN DATE_OF_BIRTH
            ELSE NULL
        END DATE_OF_BIRTH,
        DATEDIFF(YEAR,DATE_OF_BIRTH,CURRENT_DATE) AS AGE,
        GENDER,
        CASE
            WHEN LOWER(MARITAL_STATUS) IN ('single','s') THEN 'Single'
            WHEN LOWER(MARITAL_STATUS) IN ('married','m') THEN 'Married'
            WHEN LOWER(MARITAL_STATUS) IN ('divorced','d') THEN 'Divorced'
            ELSE NULL
        END MARITAL_STATUS,
        EDUCATION_LEVEL,
        OCCUPATION,
        INCOME_ANNUAL,
        PHONE_NUMBER,
        LOWER(TRIM(EMAIL)) AS EMAIL,
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
        CASE 
            WHEN BLOOD_GROUP IN ('B-','AB+','O+','A+','A-','O-','B+','AB-') THEN BLOOD_GROUP
            ELSE 'unknown'
        END AS BLOOD_GROUP,
        CASE 
            WHEN HEIGHT_CM BETWEEN 50 AND 250 THEN HEIGHT_CM
            ELSE NULL
        END HEIGHT_CM,
        CASE 
            WHEN WEIGHT_KG BETWEEN 30 AND 300 THEN WEIGHT_KG
            ELSE NULL
        END WEIGHT_KG,
        BMI,
        CASE 
            WHEN BMI BETWEEN 18.5 AND 24.9 THEN 'Healthy'
            WHEN BMI <18.5 THEN 'Underweight'
            WHEN BMI BETWEEN 25 AND 29.9 THEN 'Overweight'
            WHEN BMI >=30 THEN 'Obese'
        END::TEXT BMI_STATUS,
        CASE
            WHEN SMOKING_STATUS IN ('Non-smoker','non-smoker') THEN 'Non-Smoker'
            ELSE SMOKING_STATUS
        END SMOKING_STATUS,
        ALCOHOL_CONSUMPTION,
        LAST_UPDATED
    FROM 
        {{ref('bronze_patient_demographics')}}
    WHERE ID_VALIDATION='valid'
    AND DATE_OF_BIRTH<=CURRENT_DATE() 
    AND TIMESTAMPDIFF(YEAR,DATE_OF_BIRTH,CURRENT_DATE()) <= 100