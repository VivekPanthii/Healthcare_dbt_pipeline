SELECT 
    *,
    CASE 
        WHEN DOCTOR_ID IS NULL THEN 'invalid'
        ELSE 'valid'
    END AS id_validation
FROM 
    {{source('source','MEDICAL_PRACTITIONERS')}}