SELECT 
    *,
    CASE 
        WHEN PATIENT_ID IS NULL THEN 'invalid'
        ELSE 'valid'
    END AS id_validation
FROM 
    {{source('source','PATIENT_DEMOGRAPHICS')}}