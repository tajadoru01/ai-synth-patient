\connect healthcare_db

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.patients (
    id                  TEXT PRIMARY KEY,
    birthdate           DATE,
    deathdate           DATE,
    ssn                 TEXT,
    drivers             TEXT,
    passport            TEXT,
    prefix              TEXT,
    first               TEXT,
    last                TEXT,
    suffix              TEXT,
    maiden              TEXT,
    marital             TEXT,
    race                TEXT,
    ethnicity           TEXT,
    gender              TEXT,
    birthplace          TEXT,
    address             TEXT,
    city                TEXT,
    state               TEXT,
    county              TEXT,
    zip                 TEXT,
    lat                 NUMERIC,
    lon                 NUMERIC,
    healthcare_expenses NUMERIC,
    healthcare_coverage NUMERIC,
    loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.encounters (
    id                  TEXT PRIMARY KEY,
    start               TIMESTAMP,
    stop                TIMESTAMP,
    patient             TEXT,
    organization        TEXT,
    provider            TEXT,
    payer               TEXT,
    encounterclass      TEXT,
    code                TEXT,
    description         TEXT,
    base_encounter_cost NUMERIC,
    total_claim_cost    NUMERIC,
    payer_coverage      NUMERIC,
    reasoncode          TEXT,
    reasondescription   TEXT,
    loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.conditions (
    start       DATE,
    stop        DATE,
    patient     TEXT,
    encounter   TEXT,
    code        TEXT,
    description TEXT,
    loaded_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.observations (
    date        TIMESTAMP,
    patient     TEXT,
    encounter   TEXT,
    code        TEXT,
    description TEXT,
    value       TEXT,
    units       TEXT,
    type        TEXT,
    loaded_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.medications (
    start                TEXT,
    stop                 TEXT,
    patient              TEXT,
    payer                TEXT,
    encounter            TEXT,
    code                 TEXT,
    description          TEXT,
    base_cost            NUMERIC,
    payer_coverage       NUMERIC,
    dispenses            INTEGER,
    totalcost            NUMERIC,
    reasoncode           TEXT,
    reasondescription    TEXT,
    loaded_at            TIMESTAMP DEFAULT NOW()
);

CREATE SCHEMA IF NOT EXISTS analytics_analytics;

CREATE TABLE IF NOT EXISTS analytics_analytics.ai_triage_summaries (
    id              SERIAL PRIMARY KEY,
    patient_id      TEXT NOT NULL,
    run_date        DATE NOT NULL DEFAULT CURRENT_DATE,
    risk_score      INTEGER,       
    risk_label      TEXT,          
    summary         TEXT,
    model_used      TEXT,
    prompt_tokens   INTEGER,
    completion_tokens INTEGER,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_triage_patient ON analytics_analytics.ai_triage_summaries (patient_id);
CREATE INDEX IF NOT EXISTS idx_triage_run_date ON analytics_analytics.ai_triage_summaries (run_date);