import argparse
import random
import uuid
from datetime import date, timedelta, datetime
from pathlib import Path

import pandas as pd

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

OUT_DIR = Path("data/raw")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def rand_ts(start: date, end: date) -> datetime:
    d = rand_date(start, end)
    return datetime(d.year, d.month, d.day, random.randint(6, 22), random.randint(0, 59))

CONDITION_POOL = [
    ("44054006",  "Diabetes mellitus type 2"),
    ("38341003",  "Hypertension"),
    ("195967001", "Asthma"),
    ("13645005",  "Chronic obstructive pulmonary disease"),
    ("44054006",  "Coronary artery disease"),
    ("73211009",  "Diabetes mellitus"),
    ("414545008", "Ischemic heart disease"),
    ("84114007",  "Heart failure"),
    ("40930008",  "Hypothyroidism"),
    ("230690007", "Cerebral infarction"),
]

MEDICATION_POOL = [
    ("860975", "Metformin 500 MG Oral Tablet"),
    ("860215", "Lisinopril 10 MG Oral Tablet"),
    ("617314", "Atorvastatin 40 MG Oral Tablet"),
    ("308460", "Albuterol 0.083 MG/ML Inhalation Solution"),
    ("197361", "Amlodipine 5 MG Oral Tablet"),
    ("197380", "Omeprazole 20 MG Oral Capsule"),
    ("310798", "Levothyroxine 50 MCG Oral Tablet"),
]

ENCOUNTER_CLASSES = ["ambulatory", "inpatient", "emergency", "wellness", "urgentcare"]

def _maybe_deathdate(bdate: date) -> str:
    """Return a plausible death date string, or '' if the patient is alive.
    Only 5% of patients die. Death must occur at least 50 years after birth
    and no later than 2024-01-01 — if that window doesn't exist, keep alive.
    """
    if random.random() > 0.05:
        return ""
    earliest = bdate + timedelta(days=365 * 50)
    latest   = date(2024, 1, 1)
    if earliest >= latest:
        return "" 
    return rand_date(earliest, latest).isoformat()


def gen_patients(n: int) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        bdate = rand_date(date(1930, 1, 1), date(2005, 12, 31))
        rows.append({
            "Id":                  str(uuid.uuid4()),
            "BIRTHDATE":           bdate.isoformat(),
            "DEATHDATE":           _maybe_deathdate(bdate),
            "SSN":                 f"999-{random.randint(10,99)}-{random.randint(1000,9999)}",
            "DRIVERS":             "",
            "PASSPORT":            "",
            "PREFIX":              random.choice(["Mr.", "Ms.", "Mrs.", ""]),
            "FIRST":               random.choice(["James","Mary","John","Patricia","Robert","Linda","Michael","Barbara","William","Elizabeth"]),
            "LAST":                random.choice(["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Wilson","Taylor"]),
            "SUFFIX":              "",
            "MAIDEN":              "",
            "MARITAL":             random.choice(["M","S","D","W"]),
            "RACE":                random.choice(["white","black","asian","hispanic","other"]),
            "ETHNICITY":           random.choice(["nonhispanic","hispanic"]),
            "GENDER":              random.choice(["M","F"]),
            "BIRTHPLACE":          "Springfield  Massachusetts  US",
            "ADDRESS":             f"{random.randint(1,999)} Main St",
            "CITY":                random.choice(["Boston","Springfield","Worcester","Cambridge"]),
            "STATE":               "Massachusetts",
            "COUNTY":              "Suffolk County",
            "ZIP":                 f"0{random.randint(1000,9999)}",
            "LAT":                 round(random.uniform(41.5, 42.5), 6),
            "LON":                 round(random.uniform(-72.0, -70.5), 6),
            "HEALTHCARE_EXPENSES": round(random.uniform(1000, 200000), 2),
            "HEALTHCARE_COVERAGE": round(random.uniform(500, 100000), 2),
        })
    return pd.DataFrame(rows)


def gen_encounters(patients_df: pd.DataFrame, n: int) -> pd.DataFrame:
    rows = []
    pids = patients_df["Id"].tolist()
    for _ in range(n):
        pid = random.choice(pids)
        ts  = rand_ts(date(2015, 1, 1), date(2024, 6, 1))
        stop = ts + timedelta(minutes=random.randint(15, 300))
        cost = round(random.uniform(50, 8000), 2)
        cov  = round(cost * random.uniform(0.4, 0.9), 2)
        rows.append({
            "Id":                   str(uuid.uuid4()),
            "START":                ts.isoformat(),
            "STOP":                 stop.isoformat(),
            "PATIENT":              pid,
            "ORGANIZATION":         str(uuid.uuid4()),
            "PROVIDER":             str(uuid.uuid4()),
            "PAYER":                str(uuid.uuid4()),
            "ENCOUNTERCLASS":       random.choice(ENCOUNTER_CLASSES),
            "CODE":                 "185349003",
            "DESCRIPTION":          "Encounter for check up",
            "BASE_ENCOUNTER_COST":  round(random.uniform(50, 500), 2),
            "TOTAL_CLAIM_COST":     cost,
            "PAYER_COVERAGE":       cov,
            "REASONCODE":           "",
            "REASONDESCRIPTION":    "",
        })
    return pd.DataFrame(rows)


def gen_conditions(encounters_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, enc in encounters_df.sample(frac=0.6).iterrows():
        cond = random.choice(CONDITION_POOL)
        start = enc["START"][:10]
        rows.append({
            "START":       start,
            "STOP":        "" if random.random() > 0.4 else (date.fromisoformat(start) + timedelta(days=random.randint(30, 1500))).isoformat(),
            "PATIENT":     enc["PATIENT"],
            "ENCOUNTER":   enc["Id"],
            "CODE":        cond[0],
            "DESCRIPTION": cond[1],
        })
    return pd.DataFrame(rows)


def gen_observations(encounters_df: pd.DataFrame) -> pd.DataFrame:
    obs_defs = [
        ("55284-4", "Blood Pressure",       lambda: str(random.randint(100, 200)), "mmHg",   "numeric"),
        ("8462-4",  "Diastolic Blood Pressure", lambda: str(random.randint(60, 120)), "mmHg","numeric"),
        ("39156-5", "Body Mass Index",       lambda: str(round(random.uniform(18, 50), 1)), "kg/m2", "numeric"),
        ("2339-0",  "Glucose",               lambda: str(random.randint(70, 400)), "mg/dL",  "numeric"),
        ("8867-4",  "Heart rate",            lambda: str(random.randint(50, 110)), "/min",   "numeric"),
        ("8310-5",  "Body temperature",      lambda: str(round(random.uniform(36.0, 39.5), 1)), "Cel", "numeric"),
    ]
    rows = []
    for _, enc in encounters_df.iterrows():
        for code, desc, val_fn, unit, typ in random.sample(obs_defs, k=random.randint(2, len(obs_defs))):
            rows.append({
                "DATE":        enc["START"],
                "PATIENT":     enc["PATIENT"],
                "ENCOUNTER":   enc["Id"],
                "CODE":        code,
                "DESCRIPTION": desc,
                "VALUE":       val_fn(),
                "UNITS":       unit,
                "TYPE":        typ,
            })
    return pd.DataFrame(rows)


def gen_medications(encounters_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, enc in encounters_df.sample(frac=0.4).iterrows():
        med = random.choice(MEDICATION_POOL)
        start = enc["START"][:10]
        rows.append({
            "START":           start,
            "STOP":            "" if random.random() > 0.3 else (date.fromisoformat(start) + timedelta(days=random.randint(30, 730))).isoformat(),
            "PATIENT":         enc["PATIENT"],
            "PAYER":           str(uuid.uuid4()),
            "ENCOUNTER":       enc["Id"],
            "CODE":            med[0],
            "DESCRIPTION":     med[1],
            "BASE_COST":       round(random.uniform(5, 300), 2),
            "PAYER_COVERAGE":  round(random.uniform(0, 200), 2),
            "DISPENSES":       random.randint(1, 12),
            "TOTALCOST":       round(random.uniform(5, 1200), 2),
            "REASONCODE":      "",
            "REASONDESCRIPTION": "",
        })
    return pd.DataFrame(rows)

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic healthcare CSVs")
    parser.add_argument("--patients",   type=int, default=200)
    parser.add_argument("--encounters", type=int, default=1000)
    args = parser.parse_args()

    print(f"Generating {args.patients} patients and ~{args.encounters} encounters …")

    patients   = gen_patients(args.patients)
    encounters = gen_encounters(patients, args.encounters)
    conditions = gen_conditions(encounters)
    observations = gen_observations(encounters)
    medications  = gen_medications(encounters)

    patients.to_csv(OUT_DIR / "patients.csv",     index=False)
    encounters.to_csv(OUT_DIR / "encounters.csv",  index=False)
    conditions.to_csv(OUT_DIR / "conditions.csv",  index=False)
    observations.to_csv(OUT_DIR / "observations.csv", index=False)
    medications.to_csv(OUT_DIR / "medications.csv",   index=False)

    print(f"\n Wrote to {OUT_DIR}/")
    print(f"   patients.csv       {len(patients)} rows")
    print(f"   encounters.csv     {len(encounters)} rows")
    print(f"   conditions.csv     {len(conditions)} rows")
    print(f"   observations.csv   {len(observations)} rows")
    print(f"   medications.csv    {len(medications)} rows")


if __name__ == "__main__":
    main()