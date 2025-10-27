SELECT 
        BRANCH_ID,
        BRANCH_NAME,
        CASE 
            WHEN BRANCH_TYPE IN ('main hospital') THEN 'Main Hospital'
            ELSE BRANCH_TYPE
        END BRANCH_TYPE,
        ADDRESS,
        CITY,
        STATE,
        ZIP_CODE,
        CASE
            WHEN LENGTH(REPLACE(PHONE, '-', '')) = 10 THEN PHONE
            ELSE NULL
        END AS PHONE,
        EMAIL,
        CASE 
            WHEN MANAGER_NAME IS NULL THEN 'Unknown'
            ELSE MANAGER_NAME
        END AS MANAGER_NAME,
        CASE    
            WHEN OPENING_DATE <= CURRENT_DATE THEN OPENING_DATE
            ELSE NULL
        END AS OPENING_DATE,
        TOTAL_BEDS,
        CASE    
            WHEN LOWER(EMERGENCY_SERVICES) IN ('false','f') THEN 'False'
            WHEN LOWER(EMERGENCY_SERVICES) IN ('true','t') THEN 'True'
            ELSE NULL
        END AS EMERGENCY_SERVICES,
        CASE    
            WHEN LOWER(MENTAL_HEALTH_UNIT) IN ('false','f') THEN 'False'
            WHEN LOWER(MENTAL_HEALTH_UNIT) IN ('true','t') THEN 'True'
            ELSE NULL
        END AS MENTAL_HEALTH_UNIT,
        CASE    
            WHEN OPERATING_HOURS IN ('24/7','24-7') THEN '12AM-11:59PM'
            ELSE SPLIT_PART(OPERATING_HOURS, ' ', 2)
        END AS OPERATING_HOURS,
        CASE 
            WHEN OPERATING_HOURS IN ('24/7','24-7') THEN 'Sun-Sat'
            ELSE SPLIT_PART(OPERATING_HOURS, ' ', 1)
        END AS WORKING_HOURS,
        ACCREDITATION,
        REGION,
        LATITUDE,
        LONGITUDE,
        STATUS
    FROM {{ ref("bronze_hospital_branches") }}