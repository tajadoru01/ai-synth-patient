WITH patients AS (
    SELECT * FROM {{ ref('stg_patients') }}
),

encounter_stats AS (
    SELECT
        patient_id,
        COUNT(*)                            AS total_encounters,
        MAX(encounter_start)                AS last_encounter_at,
        MIN(encounter_start)               AS first_encounter_at,
        SUM(total_claim_cost)               AS total_claim_cost,
        SUM(patient_out_of_pocket)          AS total_out_of_pocket
    FROM {{ ref('stg_encounters') }}
    GROUP BY 1
),

final AS (
    SELECT
        p.patient_id,
        p.full_name,
        p.first_name,
        p.last_name,
        p.birthdate,
        p.deathdate,
        p.age,
        p.gender,
        p.race,
        p.ethnicity,
        p.marital,
        p.city,
        p.state,
        p.zip,
        p.lat,
        p.lon,
        p.is_deceased,
        p.healthcare_expenses,
        p.healthcare_coverage,
        COALESCE(e.total_encounters, 0)         AS total_encounters,
        e.last_encounter_at,
        e.first_encounter_at,
        CURRENT_DATE - e.last_encounter_at::DATE AS days_since_last_encounter,
        COALESCE(e.total_claim_cost, 0)          AS total_claim_cost,
        COALESCE(e.total_out_of_pocket, 0)       AS total_out_of_pocket
    FROM patients p
    LEFT JOIN encounter_stats e USING (patient_id)
)

SELECT * FROM final
