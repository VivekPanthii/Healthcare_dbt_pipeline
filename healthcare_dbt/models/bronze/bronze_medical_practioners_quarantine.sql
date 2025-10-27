SELECT
    *
FROM
 {{ref("bronze_medical_practioners")}}
where id_validation='invalid'