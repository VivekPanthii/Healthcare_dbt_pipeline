    SELECT
        ASSESSMENT_ID,
        PATIENT_ID,
        CASE 
            WHEN ASSESSMENT_DATE <= CURRENT_DATE() THEN ASSESSMENT_DATE
            ELSE NULL
        END AS ASSESSMENT_DATE,
        DOCTOR_ID,
        BRANCH_ID,
        CASE 
            WHEN (0-DEPRESSION_SCORE) > 0 THEN ABS(DEPRESSION_SCORE)
            ELSE DEPRESSION_SCORE
        END AS DEPRESSION_SCORE,
        CASE 
            WHEN (0-ANXIETY_SCORE) > 0 THEN ABS(ANXIETY_SCORE)
            ELSE ANXIETY_SCORE
        END AS ANXIETY_SCORE,
        STRESS_LEVEL,
        SLEEP_QUALITY,
        CASE  
            WHEN LOWER(APPETITE_CHANGES) IN ('true','t') THEN 'True'
            WHEN LOWER(APPETITE_CHANGES) IN ('false','f') THEN 'False'
            ELSE 'null'
        END AS APPETITE_CHANGES,
        CASE  
            WHEN LOWER(SOCIAL_WITHDRAWAL) IN ('true','t') THEN 'True'
            WHEN LOWER(SOCIAL_WITHDRAWAL) IN ('false','f') THEN 'False'
            ELSE 'null'
        END AS SOCIAL_WITHDRAWAL,
        CASE  
            WHEN LOWER(CONCENTRATION_ISSUES) IN ('true','t') THEN 'True'
            WHEN LOWER(CONCENTRATION_ISSUES) IN ('false','f') THEN 'False'
            ELSE 'null'
        END AS CONCENTRATION_ISSUES,
        CASE  
            WHEN LOWER(SUICIDAL_THOUGHTS) IN ('true','t') THEN 'True'
            WHEN LOWER(SUICIDAL_THOUGHTS) IN ('false','f') THEN 'False'
            ELSE 'null'
        END AS SUICIDAL_THOUGHTS,
        COALESCE(MEDICATION_PRESCRIBED, 'Not Prescribed') AS MEDICATION_PRESCRIBED,
        THERAPY_SESSIONS_RECOMMENDED,
        DIAGNOSIS_CODE,
        SEVERITY_LEVEL,
        NOTES,
        FOLLOW_UP_DATE,
        ASSESSMENT_TYPE
    FROM {{ref("bronze_mental_health_assessments")}}