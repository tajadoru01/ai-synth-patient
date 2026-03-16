import os
import io
import logging
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

FILE_TABLE_MAP = {
    "patients.csv":     "raw.patients",
    "encounters.csv":   "raw.encounters",
    "conditions.csv":   "raw.conditions",
    "observations.csv": "raw.observations",
    "medications.csv":  "raw.medications",
}

PK_COLS = {"patients": "id", "encounters": "id"}


def get_conn():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ["DB_NAME"],
    )


def load_csv_to_raw(csv_path: Path, table: str, conn) -> int:
    """Load a single CSV into a raw.* table using COPY for speed."""
    log.info("Loading %s → %s", csv_path.name, table)

    df = pd.read_csv(csv_path, low_memory=False)
    df.columns = [c.strip().lower() for c in df.columns]

    schema, tbl = table.split(".")

    if tbl in PK_COLS:
        pk = PK_COLS[tbl]
        with conn.cursor() as cur:
            cur.execute(f"SELECT {pk} FROM {table}") 
            existing_ids = {row[0] for row in cur.fetchall()}
        before = len(df)
        df = df[~df[pk].isin(existing_ids)]
        log.info("  Dedup: %d → %d rows (skipped %d)", before, len(df), before - len(df))

    if df.empty:
        log.info("  Nothing new to load for %s.", table)
        return 0

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)

    cols = ", ".join(df.columns)
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '')",  
            buf,
        )
    conn.commit()

    log.info("  Inserted %d rows into %s.", len(df), table)
    return len(df)


def run_load(**kwargs):
    """Airflow-compatible callable."""
    data_dir = Path(os.environ.get("DATA_DIR", "data/raw"))
    conn = get_conn()

    total = 0
    missing = []
    try:
        for filename, table in FILE_TABLE_MAP.items():
            csv_path = data_dir / filename
            if not csv_path.exists():
                log.warning("File not found, skipping: %s", csv_path)
                missing.append(filename)
                continue
            total += load_csv_to_raw(csv_path, table, conn)
    finally:
        conn.close()

    if missing:
        log.warning("Missing files: %s", ", ".join(missing))

    log.info("Load complete. Total rows inserted: %d", total)
    return total


if __name__ == "__main__":
    rows = run_load()
    print(f"\n Done — {rows} total rows loaded.")