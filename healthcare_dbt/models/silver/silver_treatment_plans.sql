    SELECT
        TREATMENT_ID,
        PATIENT_ID,
        ASSESSMENT_ID,
        DOCTOR_ID,
        BRANCH_ID,
        TREATMENT_START_DATE,
        DATEADD(DAYS,DURATION_WEEKS*7,TREATMENT_START_DATE) AS TREATMENT_END_DATE,
        DURATION_WEEKS,
        TREATMENT_STATUS,
        TREATMENT_TYPE,
        THERAPY_TYPE,
        CASE 
            WHEN MEDICATION_NAME IS NULL THEN 'Not Provided'
            ELSE 'Provided'
        END AS MEDICATION_STATUS,
        MEDICATION_NAME,
        DOSAGE_MG,
        FREQUENCY,
        CASE
            WHEN COST_USD<0 THEN 0
            ELSE COST_USD
        END AS COST_USD,
        CASE 
            WHEN INSURANCE_COVERED IN ('T','true','TRUE') THEN 'True'
            WHEN INSURANCE_COVERED IN ('F','false','FALSE') THEN 'False'
            ELSE 'null'
        END AS INSURANCE_COVERED,
        CASE
            WHEN PATIENT_PAYMENT<0 THEN 0
            ELSE PATIENT_PAYMENT
        END AS PATIENT_PAYMENT,
        CASE 
            WHEN SIDE_EFFECTS_REPORTED IS NULL THEN 'No Medications'
            ELSE SIDE_EFFECTS_REPORTED
        END AS SIDE_EFFECTS_REPORTED,
        CASE    
            WHEN EFFECTIVENESS_RATING<0 THEN 0
            ELSE EFFECTIVENESS_RATING
        END AS EFFECTIVENESS_RATING,
        NOTES
    FROM {{ref('bronze_treatment_plans')}}