SELECT
    *
FROM 
    {{ref('bronze_patient_demographics')}}
where id_validation='invalid'