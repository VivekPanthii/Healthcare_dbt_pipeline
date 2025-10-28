{{
    config(
        materialized='incremental',
        unique_key='TREATMENT_ID',
        on_scheam_change='merge'
    )
}}

SELECT 
    f.TREATMENT_ID,
    p.PATIENT_SK AS PATIENT_FK,
    a.ASSESSMENT_SK AS ASSESSMENT_FK,
    d.PRACTITIONER_SK AS PRACTITIONER_FK,
    b.BRANCH_SK as BRANCH_FK,
    f.TREATMENT_START_DATE,
    f.TREATMENT_END_DATE,
    f.DURATION_WEEKS,
    t.TREATMENT_STATUS_SK AS TREATMENT_STATUS_FK,
    tt.TREATMENT_TYPE_SK AS TREATMENT_TYPE_FK,
    tht.THERAPY_TYPE_SK AS THERAPY_TYPE_FK,
    m.MEDICATION_NAME_SK AS MEDICATION_NAME_FK,
    f.DOSAGE_MG,
    f.FREQUENCY,
    f.COST_USD,
    f.INSURANCE_COVERED,
    f.PATIENT_PAYMENT,
    f.SIDE_EFFECTS_REPORTED,
    f.EFFECTIVENESS_RATING,
    f.NOTES 
FROM {{ref("silver_treatment_plans")}} f
LEFT JOIN {{ref('dim_patient_demographics')}} p
    ON f.PATIENT_ID=p.PATIENT_ID
    AND f.TREATMENT_START_DATE>=p.EFFECTIVE_FROM
    AND f.TREATMENT_START_DATE<p.EFFECTIVE_TO
LEFT JOIN {{ref('fact_mental_health_assessment')}} a
    ON f.ASSESSMENT_ID=a.ASSESSMENT_ID
LEFT JOIN {{ref('dim_medical_practioners')}} d
    ON f.DOCTOR_ID=d.DOCTOR_ID
    AND f.TREATMENT_START_DATE>=d.EFFECTIVE_FROM
    AND f.TREATMENT_START_DATE<d.EFFECTIVE_TO
LEFT JOIN {{ref('dim_branches')}} b
    ON f.BRANCH_ID=b.BRANCH_ID
    AND f.TREATMENT_START_DATE>=b.EFFECTIVE_FROM
    AND f.TREATMENT_START_DATE<b.EFFECTIVE_TO
LEFT JOIN {{ref('dim_treatment_status')}} t
    ON f.TREATMENT_STATUS=t.TREATMENT_STATUS
LEFT JOIN {{ref('dim_treatment_type')}} tt
    ON f.TREATMENT_TYPE=tt.TREATMENT_TYPE
LEFT JOIN {{ref('dim_therapy_type')}} tht
    ON f.THERAPY_TYPE=tht.THERAPY_TYPE
LEFT JOIN {{ref('dim_medication_name')}} m
    ON f.MEDICATION_NAME=m.MEDICATION_NAME
{% if is_incremental() %}
  WHERE f.TREATMENT_ID NOT IN (SELECT TREATMENT_ID FROM {{ this }})
{% endif %}