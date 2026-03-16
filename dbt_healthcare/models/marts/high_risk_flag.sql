WITH patients AS (
    SELECT * FROM {{ ref('dim_patients') }}
),

active_conditions AS (
    SELECT
        patient_id,
        COUNT(DISTINCT condition_code)                                   AS active_condition_count,
        STRING_AGG(DISTINCT condition_description, '; ' ORDER BY condition_description)
                                                                         AS active_conditions
    FROM {{ ref('fct_clinical_events') }}
    WHERE condition_is_active = TRUE
    GROUP BY 1
),

latest_vitals AS (
    SELECT DISTINCT ON (patient_id)
        patient_id,
        systolic_bp             AS latest_systolic_bp,
        diastolic_bp            AS latest_diastolic_bp,
        bmi                     AS latest_bmi,
        glucose_mgdl            AS latest_glucose,
        encounter_start         AS vitals_recorded_at
    FROM {{ ref('fct_clinical_events') }}
    WHERE systolic_bp IS NOT NULL
       OR bmi IS NOT NULL
       OR glucose_mgdl IS NOT NULL
    ORDER BY patient_id, encounter_start DESC
),

latest_encounter AS (
    SELECT DISTINCT ON (patient_id)
        patient_id,
        encounter_class         AS recent_encounter_type,
        encounter_start         AS last_encounter_at
    FROM {{ ref('fct_clinical_events') }}
    ORDER BY patient_id, encounter_start DESC
),

current_meds AS (
    SELECT
        patient                                                           AS patient_id,
        STRING_AGG(description, '; ' ORDER BY start DESC)
            FILTER (WHERE stop IS NULL OR stop > CURRENT_DATE::TEXT)      AS current_medications
    FROM {{ source('raw', 'medications') }}
    GROUP BY 1
),

combined AS (
    SELECT
        p.patient_id,
        p.full_name,
        p.age,
        p.gender,
        p.is_deceased,
        p.days_since_last_encounter,

        ac.active_condition_count,
        ac.active_conditions,

        lv.latest_systolic_bp,
        lv.latest_diastolic_bp,
        lv.latest_bmi,
        lv.latest_glucose,

        le.recent_encounter_type,

        cm.current_medications

    FROM patients p
    LEFT JOIN active_conditions ac USING (patient_id)
    LEFT JOIN latest_vitals      lv USING (patient_id)
    LEFT JOIN latest_encounter   le USING (patient_id)
    LEFT JOIN current_meds       cm USING (patient_id)
),

flagged AS (
    SELECT
        *,
        CASE
            WHEN active_condition_count >= 3                        THEN TRUE  
            WHEN latest_systolic_bp > 160                           THEN TRUE 
            WHEN latest_glucose > 300                               THEN TRUE
            WHEN latest_bmi > 40                                    THEN TRUE 
            WHEN days_since_last_encounter > 365 AND active_condition_count >= 1 THEN TRUE 
            ELSE FALSE
        END AS is_high_risk
    FROM combined
    WHERE is_deceased = FALSE        
)

SELECT *
FROM flagged
WHERE is_high_risk = TRUE
