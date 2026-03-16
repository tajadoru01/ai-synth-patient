WITH source AS (
    SELECT * FROM {{ source('raw', 'patients') }}
),

cleaned AS (
    SELECT
        id                                                      AS patient_id,
        LOWER(first)                                            AS first_name,
        LOWER(last)                                             AS last_name,
        INITCAP(LOWER(first)) || ' ' || INITCAP(LOWER(last))   AS full_name,
        birthdate,
        deathdate,
        CASE
            WHEN deathdate IS NOT NULL THEN
                EXTRACT(YEAR FROM AGE(deathdate, birthdate))
            ELSE
                EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate))
        END::INTEGER                                            AS age,
        gender,
        race,
        ethnicity,
        marital,
        city,
        state,
        zip,
        lat,
        lon,
        healthcare_expenses,
        healthcare_coverage,
        CASE WHEN deathdate IS NOT NULL THEN TRUE ELSE FALSE END AS is_deceased,
        loaded_at

    FROM source
    WHERE id IS NOT NULL
)

SELECT * FROM cleaned
