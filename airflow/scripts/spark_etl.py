import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

OBS_CODES = {
    "55284-4": "systolic_bp",
    "8462-4":  "diastolic_bp",
    "39156-5": "bmi",
    "2339-0":  "glucose_mgdl",
    "8867-4":  "heart_rate",
    "8310-5":  "body_temp_c",
}

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("healthcare-etl")
        # In prod: configure executor memory, JDBC driver jar, S3 creds here
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

def extract(spark: SparkSession, data_dir: Path) -> dict[str, DataFrame]:
    """Read raw CSVs. Returns a dict of DataFrames keyed by table name."""
    files = {
        "patients":     "patients.csv",
        "encounters":   "encounters.csv",
        "conditions":   "conditions.csv",
        "observations": "observations.csv",
        "medications":  "medications.csv",
    }
    dfs = {}
    for name, filename in files.items():
        path = data_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Required file missing: {path}")
        log.info("Extracting %s", path)
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(str(path))
        )
        df = df.toDF(*[c.strip().lower() for c in df.columns])
        log.info("  %s: %d rows, %d cols", name, df.count(), len(df.columns))
        dfs[name] = df
    return dfs

def transform_patients(patients: DataFrame) -> DataFrame:
    """
    Mirrors stg_patients.sql:
      - name casing
      - age from birthdate / deathdate
      - is_deceased flag
      - null id rows dropped
    """
    return (
        patients
        .filter(F.col("id").isNotNull())
        .withColumn("patient_id",  F.col("id"))
        .withColumn("first_name",  F.lower(F.col("first")))
        .withColumn("last_name",   F.lower(F.col("last")))
        .withColumn("full_name",
            F.concat_ws(" ",
                F.initcap(F.col("first")),
                F.initcap(F.col("last")),
            )
        )
        .withColumn("is_deceased", F.col("deathdate").isNotNull())
        .withColumn("age",
            F.when(
                F.col("deathdate").isNotNull(),
                F.floor(
                    F.datediff(
                        F.to_date(F.col("deathdate")),
                        F.to_date(F.col("birthdate")),
                    ) / 365.25
                )
            ).otherwise(
                F.floor(
                    F.datediff(F.current_date(), F.to_date(F.col("birthdate"))) / 365.25
                )
            ).cast("integer")
        )
        .select(
            "patient_id", "full_name", "first_name", "last_name",
            "birthdate", "deathdate", "age", "gender",
            "race", "ethnicity", "marital", "city", "state", "zip",
            "lat", "lon", "healthcare_expenses", "healthcare_coverage",
            "is_deceased",
        )
    )

def transform_encounters(encounters: DataFrame) -> DataFrame:
    """
    Mirrors stg_encounters.sql:
      - column renames + type casting
      - duration in minutes
      - patient out-of-pocket calc
      - null id / patient / start rows dropped
    """
    return (
        encounters
        .filter(
            F.col("id").isNotNull() &
            F.col("patient").isNotNull() &
            F.col("start").isNotNull()
        )
        .withColumn("encounter_id",          F.col("id"))
        .withColumn("patient_id",            F.col("patient"))
        .withColumn("encounter_class",       F.col("encounterclass"))
        .withColumn("encounter_code",        F.col("code"))
        .withColumn("encounter_description", F.col("description"))
        .withColumn("encounter_start",       F.to_timestamp(F.col("start")))
        .withColumn("encounter_stop",        F.to_timestamp(F.col("stop")))
        .withColumn("duration_minutes",
            (
                F.unix_timestamp(F.col("encounter_stop")) -
                F.unix_timestamp(F.col("encounter_start"))
            ) / 60.0
        )
        .withColumn("patient_out_of_pocket",
            F.col("total_claim_cost") - F.col("payer_coverage")
        )
        .select(
            "encounter_id", "patient_id", "organization", "provider", "payer",
            "encounter_class", "encounter_code", "encounter_description",
            "reasoncode", "reasondescription",
            "encounter_start", "encounter_stop", "duration_minutes",
            "base_encounter_cost", "total_claim_cost", "payer_coverage",
            "patient_out_of_pocket",
        )
    )

def transform_dim_patients(
    stg_patients: DataFrame,
    stg_encounters: DataFrame,
) -> DataFrame:
    """
    Mirrors dim_patients.sql:
      - encounter stats aggregated per patient
      - days_since_last_encounter computed
      - left-joined onto patients
    """
    encounter_stats = (
        stg_encounters
        .groupBy("patient_id")
        .agg(
            F.count("*").alias("total_encounters"),
            F.max("encounter_start").alias("last_encounter_at"),
            F.min("encounter_start").alias("first_encounter_at"),
            F.sum("total_claim_cost").alias("total_claim_cost"),
            F.sum("patient_out_of_pocket").alias("total_out_of_pocket"),
        )
        .withColumn("days_since_last_encounter",
            F.datediff(F.current_date(), F.to_date(F.col("last_encounter_at")))
        )
    )

    return (
        stg_patients
        .join(encounter_stats, on="patient_id", how="left")
        .withColumn("total_encounters",   F.coalesce(F.col("total_encounters"),   F.lit(0)))
        .withColumn("total_claim_cost",   F.coalesce(F.col("total_claim_cost"),   F.lit(0.0)))
        .withColumn("total_out_of_pocket",F.coalesce(F.col("total_out_of_pocket"),F.lit(0.0)))
    )

def transform_fct_clinical_events(
    stg_encounters: DataFrame,
    conditions: DataFrame,
    observations: DataFrame,
) -> DataFrame:
    cond = (
        conditions
        .filter(F.col("encounter").isNotNull() & F.col("patient").isNotNull())
        .withColumn("encounter_id",         F.col("encounter"))
        .withColumn("patient_id",           F.col("patient"))
        .withColumn("condition_start",      F.to_date(F.col("start")))
        .withColumn("condition_stop",       F.to_date(F.col("stop")))
        .withColumn("condition_code",       F.col("code"))
        .withColumn("condition_description",F.col("description"))
        .withColumn("is_active",            F.col("stop").isNull())
        .select(
            "encounter_id", "patient_id",
            "condition_start", "condition_stop",
            "condition_code", "condition_description", "is_active",
        )
        .dropDuplicates(["encounter_id", "patient_id", "condition_code"])
    )

    obs_base = (
        observations
        .filter(F.col("encounter").isNotNull())
        .withColumn("encounter_id", F.col("encounter"))
        .withColumn("patient_id",   F.col("patient"))
        .withColumn("value_num",    F.col("value").cast("double"))
    )

    obs_aggs = [
        F.max(
            F.when(F.col("code") == loinc, F.col("value_num"))
        ).alias(col_name)
        for loinc, col_name in OBS_CODES.items()
    ]

    obs_wide = (
        obs_base
        .groupBy("encounter_id", "patient_id")
        .agg(*obs_aggs)
    )

    window = Window.partitionBy("encounter_id").orderBy(
        F.col("condition_code").asc_nulls_last()
    )

    return (
        stg_encounters
        .join(cond,     on="encounter_id", how="left")
        .join(obs_wide, on="encounter_id", how="left")
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("event_id", F.md5(F.col("encounter_id")))
        .select(
            "event_id", "encounter_id", "patient_id",
            "encounter_start", "encounter_stop",
            "encounter_class", "encounter_description", "reasondescription",
            "total_claim_cost", "patient_out_of_pocket",
            "condition_code", "condition_description", "is_active",
            *OBS_CODES.values(),
        )
    )

def transform_high_risk_flag(
    dim_patients: DataFrame,
    fct_events: DataFrame,
    medications: DataFrame,
) -> DataFrame:

    active_conditions = (
        fct_events
        .filter(F.col("is_active") == True)  
        .groupBy("patient_id")
        .agg(
            F.countDistinct("condition_code").alias("active_condition_count"),
            F.collect_set("condition_description").alias("condition_set"),
        )
        .withColumn("active_conditions",
            F.array_join(F.sort_array(F.col("condition_set")), "; ")
        )
        .drop("condition_set")
    )

    vitals_present = (
        F.col("systolic_bp").isNotNull() |
        F.col("bmi").isNotNull() |
        F.col("glucose_mgdl").isNotNull()
    )
    vitals_window = Window.partitionBy("patient_id").orderBy(F.col("encounter_start").desc())

    latest_vitals = (
        fct_events
        .filter(vitals_present)
        .withColumn("rn", F.row_number().over(vitals_window))
        .filter(F.col("rn") == 1)
        .select(
            "patient_id",
            F.col("systolic_bp").alias("latest_systolic_bp"),
            F.col("diastolic_bp").alias("latest_diastolic_bp"),
            F.col("bmi").alias("latest_bmi"),
            F.col("glucose_mgdl").alias("latest_glucose"),
        )
    )

    enc_window = Window.partitionBy("patient_id").orderBy(F.col("encounter_start").desc())
    latest_encounter = (
        fct_events
        .withColumn("rn", F.row_number().over(enc_window))
        .filter(F.col("rn") == 1)
        .select(
            "patient_id",
            F.col("encounter_class").alias("recent_encounter_type"),
        )
    )

    today_str = F.current_date().cast("string")
    current_meds = (
        medications
        .filter(F.col("patient").isNotNull())
        .withColumn("patient_id", F.col("patient"))
        .filter(F.col("stop").isNull() | (F.col("stop") > today_str))
        .groupBy("patient_id")
        .agg(
            F.concat_ws("; ",
                F.collect_list(F.col("description"))
            ).alias("current_medications")
        )
    )

    is_high_risk = (
        (F.col("active_condition_count") >= 3) |
        (F.col("latest_systolic_bp") > 160) |
        (F.col("latest_glucose") > 300) |
        (F.col("latest_bmi") > 40) |
        (
            (F.col("days_since_last_encounter") > 365) &
            (F.col("active_condition_count") >= 1)
        )
    )

    return (
        dim_patients
        .filter(F.col("is_deceased") == False)  # noqa: E712
        .join(active_conditions, on="patient_id", how="left")
        .join(latest_vitals,     on="patient_id", how="left")
        .join(latest_encounter,  on="patient_id", how="left")
        .join(current_meds,      on="patient_id", how="left")
        .withColumn("active_condition_count",
            F.coalesce(F.col("active_condition_count"), F.lit(0))
        )
        .withColumn("is_high_risk", is_high_risk)
        .filter(F.col("is_high_risk") == True)  # noqa: E712
    )

def get_jdbc_url() -> str:
    return (
        f"jdbc:postgresql://{os.environ['DB_HOST']}:{os.environ['DB_PORT']}"
        f"/{os.environ['DB_NAME']}"
    )

def get_jdbc_props() -> dict:
    return {
        "user":   os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "driver": "org.postgresql.Driver",
        "sslmode": os.environ.get("DB_SSLMODE", "prefer"),  # "require" in prod
    }

def load_to_postgres(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    """
    Write a transformed DataFrame to Postgres via JDBC.

    mode="overwrite" truncates and rewrites — safe for analytics tables that
    are rebuilt from scratch on each run.

    For append-only tables (audit logs, etc.) use mode="append".
    """
    log.info("Loading %d rows → %s (mode=%s)", df.count(), table, mode)
    (
        df.write
        .format("jdbc")
        .option("url", get_jdbc_url())
        .option("dbtable", table)
        .option("mode", mode)
        .options(**get_jdbc_props())
        .save()
    )
    log.info("  Done → %s", table)

def run_etl(**kwargs) -> str:
    """
    Airflow-compatible entry point.

    ETL flow:
        CSVs
          └─ extract()
               ├─ transform_patients()       ─┐
               ├─ transform_encounters()      ├─ transform_dim_patients()
               ├─ transform_fct_clinical_events()                          ─┐
               └─ transform_high_risk_flag()  ◄─────────────────────────────┘
                    └─ load_to_postgres() × 3 tables
    """
    data_dir = Path(os.environ.get("DATA_DIR", "data/raw"))
    spark = get_spark()

    try:
        raw = extract(spark, data_dir)

        log.info("Transforming patients...")
        stg_patients   = transform_patients(raw["patients"])

        log.info("Transforming encounters...")
        stg_encounters = transform_encounters(raw["encounters"])

        log.info("Building dim_patients...")
        dim_patients   = transform_dim_patients(stg_patients, stg_encounters)

        log.info("Building fct_clinical_events...")
        fct_events     = transform_fct_clinical_events(
            stg_encounters, raw["conditions"], raw["observations"]
        )

        log.info("Applying high-risk flag...")
        high_risk      = transform_high_risk_flag(
            dim_patients, fct_events, raw["medications"]
        )

        load_to_postgres(dim_patients, "analytics.dim_patients")
        load_to_postgres(fct_events,   "analytics.fct_clinical_events")
        load_to_postgres(high_risk,    "analytics.high_risk_patients")

        summary = (
            f"ETL complete — "
            f"dim_patients={dim_patients.count()}, "
            f"fct_events={fct_events.count()}, "
            f"high_risk={high_risk.count()}"
        )
        log.info(summary)
        return summary

    finally:
        spark.stop()

if __name__ == "__main__":
    print(run_etl())