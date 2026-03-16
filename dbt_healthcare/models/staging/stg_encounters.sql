WITH source AS (
    SELECT * FROM {{ source('raw', 'encounters') }}
),

cleaned AS (
    SELECT
        id                          AS encounter_id,
        patient                     AS patient_id,
        organization,
        provider,
        payer,
        encounterclass              AS encounter_class,
        code                        AS encounter_code,
        description                 AS encounter_description,
        reasoncode,
        reasondescription,
        start::TIMESTAMP            AS encounter_start,
        stop::TIMESTAMP             AS encounter_stop,
        EXTRACT(
            EPOCH FROM (stop::TIMESTAMP - start::TIMESTAMP)
        ) / 60.0                    AS duration_minutes,
        base_encounter_cost,
        total_claim_cost,
        payer_coverage,
        (total_claim_cost - payer_coverage) AS patient_out_of_pocket,
        loaded_at

    FROM source
    WHERE id IS NOT NULL
      AND patient IS NOT NULL
      AND start IS NOT NULL
)

SELECT * FROM cleaned
