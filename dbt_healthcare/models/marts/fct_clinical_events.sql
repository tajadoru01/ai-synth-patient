WITH encounters AS (
    SELECT * FROM {{ ref('stg_encounters') }}
),

conditions AS (
    SELECT DISTINCT
        encounter                   AS encounter_id,
        patient                     AS patient_id,
        start::DATE                 AS condition_start,
        stop::DATE                  AS condition_stop,
        code                        AS condition_code,
        description                 AS condition_description,
        CASE
            WHEN stop IS NULL THEN TRUE ELSE FALSE
        END                         AS is_active
    FROM {{ source('raw', 'conditions') }}
),

observations_wide AS (
    SELECT
        encounter                   AS encounter_id,
        patient                     AS patient_id,
        MAX(CASE WHEN code = '55284-4' THEN value::NUMERIC END)  AS systolic_bp,
        MAX(CASE WHEN code = '8462-4'  THEN value::NUMERIC END)  AS diastolic_bp,
        MAX(CASE WHEN code = '39156-5' THEN value::NUMERIC END)  AS bmi,
        MAX(CASE WHEN code = '2339-0'  THEN value::NUMERIC END)  AS glucose_mgdl,
        MAX(CASE WHEN code = '8867-4'  THEN value::NUMERIC END)  AS heart_rate,
        MAX(CASE WHEN code = '8310-5'  THEN value::NUMERIC END)  AS body_temp_c
    FROM {{ source('raw', 'observations') }}
    GROUP BY 1, 2
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['e.encounter_id']) }} AS event_id,
        e.encounter_id,
        e.patient_id,
        e.encounter_start,
        e.encounter_stop,
        e.encounter_class,
        e.encounter_description,
        e.reasondescription,
        e.total_claim_cost,
        e.patient_out_of_pocket,
        c.condition_code,
        c.condition_description,
        COALESCE(c.is_active, FALSE)            AS condition_is_active,
        o.systolic_bp,
        o.diastolic_bp,
        o.bmi,
        o.glucose_mgdl,
        o.heart_rate,
        o.body_temp_c,
        ROW_NUMBER() OVER (PARTITION BY e.encounter_id ORDER BY c.condition_code NULLS LAST) AS rn
    FROM encounters e
    LEFT JOIN conditions c
        ON  c.encounter_id = e.encounter_id
    LEFT JOIN observations_wide o
        ON  o.encounter_id = e.encounter_id
)

SELECT
    event_id,
    encounter_id,
    patient_id,
    encounter_start,
    encounter_stop,
    encounter_class,
    encounter_description,
    reasondescription,
    total_claim_cost,
    patient_out_of_pocket,
    condition_code,
    condition_description,
    condition_is_active,
    systolic_bp,
    diastolic_bp,
    bmi,
    glucose_mgdl,
    heart_rate,
    body_temp_c
FROM final
WHERE rn = 1
