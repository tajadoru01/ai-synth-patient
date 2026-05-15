#Note: this isn't really an ai agent by definition. it just produces an ai risk summary of the patients. future efforts should compile pre-defined functions and then approach it from an agentic lens

import os
import json
import logging
from typing import Any

import psycopg2
import psycopg2.extras
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

MODEL = "gpt-4o-mini"   #can switch models around and test as needed

SYSTEM_PROMPT = """\
You are a clinical triage assistant. You receive structured patient data and
must produce a concise clinical summary and a numeric risk score.

Your response MUST be valid JSON with exactly these keys:
{
  "risk_score": <integer 1-5>,
  "risk_label": <"LOW"|"MODERATE"|"HIGH"|"VERY HIGH"|"CRITICAL">,
  "summary": "<2-4 sentence plain-English clinical summary>"
}

Risk score guide:
  1 = LOW       – stable, no acute concerns
  2 = MODERATE  – monitor; at least one chronic condition
  3 = HIGH      – multiple chronic conditions or recent acute event
  4 = VERY HIGH – complex multi-morbidity or recent hospitalisation
  5 = CRITICAL  – life-threatening conditions or imminent risk

Do not include any text outside the JSON object.
"""

def get_conn():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ["DB_NAME"],
    )


def fetch_high_risk_patients(conn) -> list[dict]:
    query = """
        SELECT
            patient_id,
            full_name,
            age,
            gender,
            active_conditions,
            recent_encounter_type,
            days_since_last_encounter,
            current_medications,
            latest_systolic_bp,
            latest_diastolic_bp,
            latest_bmi,
            latest_glucose
        FROM analytics_analytics.high_risk_flag
        ORDER BY age DESC
        LIMIT 50;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def already_processed_today(conn, patient_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM analytics_analytics.ai_triage_summaries "
            "WHERE patient_id = %s AND run_date = CURRENT_DATE",
            (patient_id,),
        )
        return cur.fetchone() is not None


def write_summary(conn, patient_id: str, result: dict[str, Any], model: str,
                  prompt_tokens: int, completion_tokens: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO analytics_analytics.ai_triage_summaries
                (patient_id, run_date, risk_score, risk_label, summary,
                 model_used, prompt_tokens, completion_tokens)
            VALUES (%s, CURRENT_DATE, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                patient_id,
                result["risk_score"],
                result["risk_label"],
                result["summary"],
                model,
                prompt_tokens,
                completion_tokens,
            ),
        )
    conn.commit()

def build_user_prompt(patient: dict) -> str:
    meds = patient.get("current_medications") or "None documented"
    conditions = patient.get("active_conditions") or "None documented"
    return f"""\
Patient profile:
- Name: {patient.get('full_name', 'Unknown')}
- Age: {patient.get('age', 'Unknown')} | Gender: {patient.get('gender', 'Unknown')}
- Active conditions: {conditions}
- Current medications: {meds}
- Recent encounter type: {patient.get('recent_encounter_type', 'N/A')}
- Days since last encounter: {patient.get('days_since_last_encounter', 'N/A')}

Latest vitals:
- Blood pressure: {patient.get('latest_systolic_bp', '?')}/{patient.get('latest_diastolic_bp', '?')} mmHg
- BMI: {patient.get('latest_bmi', '?')}
- Glucose: {patient.get('latest_glucose', '?')} mg/dL

Provide a clinical triage assessment.
"""

def run_triage_agent(**kwargs):
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    conn = get_conn()

    patients = fetch_high_risk_patients(conn)
    log.info("Fetched %d high-risk patients to assess.", len(patients))

    processed = 0
    skipped = 0
    errors = 0

    for patient in patients:
        pid = patient["patient_id"]

        if already_processed_today(conn, pid):
            log.debug("Skipping %s – already processed today.", pid)
            skipped += 1
            continue

        user_prompt = build_user_prompt(patient)

        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": user_prompt},
                ],
                temperature=0.2,
                max_tokens=400,
            )

            raw_text = response.choices[0].message.content.strip()
            result = json.loads(raw_text)

            assert "risk_score" in result and "risk_label" in result and "summary" in result

            write_summary(
                conn,
                pid,
                result,
                model=response.model,
                prompt_tokens=response.usage.prompt_tokens,
                completion_tokens=response.usage.completion_tokens,
            )
            log.info(
                "%s → risk %s (%s) | %d+%d tokens",
                pid,
                result["risk_score"],
                result["risk_label"],
                response.usage.prompt_tokens,
                response.usage.completion_tokens,
            )
            processed += 1

        except json.JSONDecodeError as exc:
            log.error("JSON parse error for patient %s: %s | raw=%r", pid, exc, raw_text)
            errors += 1
        except Exception as exc:
            log.error("Unexpected error for patient %s: %s", pid, exc)
            errors += 1

    conn.close()
    summary = f"Triage complete. Processed={processed}, Skipped={skipped}, Errors={errors}"
    log.info(summary)
    return summary


if __name__ == "__main__":
    print(run_triage_agent())
