# Healthcare Data Engineering & AI Triage Pipeline

An end-to-end ELT pipeline that ingests synthetic Electronic Health Records (EHRs),
transforms them with dbt, and runs an AI agent to generate clinical triage
summaries for high-risk patients. Orchestrated with Apache Airflow and deployed
via Docker Compose.

---

## Architecture

```
Synthea CSVs  -->  load_raw_data.py  -->  PostgreSQL (raw.*)
                                               |
                                        dbt deps + dbt run / test
                                               |
                                    PostgreSQL (analytics_analytics.*)
                                    ├── stg_patients (view)
                                    ├── stg_encounters (view)
                                    ├── dim_patients (table)
                                    ├── fct_clinical_events (table)
                                    └── high_risk_flag (table) --> ai_triage_agent.py
                                                                         |
                                            analytics_analytics.ai_triage_summaries
```

**Orchestration:** Airflow DAG `healthcare_pipeline` runs daily at 02:00 UTC
with tasks: `extract_load -> dbt_deps -> dbt_run -> dbt_test -> ai_triage`

---

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Database | PostgreSQL 16 |
| Transformation | dbt-core 1.8 + dbt-postgres + dbt-utils |
| Language | Python 3.10+ |
| LLM API | OpenAI (gpt-4o-mini) |
| Containerisation | Docker & Docker Compose |

---

## Project Structure

```
ai-synth-patient/
├── airflow/
│   ├── dags/
│   │   └── healthcare_pipeline_dag.py      # Airflow DAG definition
│   └── scripts/
│       ├── load_raw_data.py                # CSV loader to PostgreSQL
│       └── ai_triage_agent.py              # GPT-4o-mini triage engine
├── dbt_healthcare/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml                 # Raw data source definitions
│   │   │   ├── stg_patients.sql            # Patient cleaning layer
│   │   │   └── stg_encounters.sql          # Encounter cleaning layer
│   │   └── marts/
│   │       ├── schema.yml                  # Tests & documentation
│   │       ├── dim_patients.sql            # Patient dimension
│   │       ├── fct_clinical_events.sql     # Event fact table
│   │       └── high_risk_flag.sql          # High-risk patients view
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── packages.yml                        # dbt-utils dependency
├── data/
│   └── raw/                                # CSV data (patients, encounters, etc.)
├── docker/
│   └── 00_init.sql                         # PostgreSQL init script
├── docker-compose.yml
├── healthcare_db_setup.sql                 # Schema & table definitions
├── requirements.txt                        # Python dependencies
├── Dockerfile
├── .gitignore
└── README.md
```

---

## Quick Start (Docker — Recommended)

### Prerequisites
- Docker Desktop (Windows/Mac) or Docker Engine + Compose v2 (Linux)
- OpenAI API key

### Setup & Run

1. Clone repository and configure:
```bash
git clone <repo-url>
cd ai-synth-patient
```

2. Clean up and rebuild (first time or after issues):
```powershell
docker-compose down -v
docker rmi healthcare-airflow:latest
docker-compose build --no-cache
docker-compose up airflow-init
docker-compose up -d
```

3. Access services:
- Airflow UI: http://localhost:8080 (user: admin, password: admin)
- PostgreSQL: localhost:5432 (user: postgres, password: postgres)

4. Trigger pipeline:
In Airflow UI, find `healthcare_pipeline` DAG and trigger manually, or wait for 02:00 UTC daily schedule.

---

## Manual Setup (No Docker)

### 1. Prerequisites
- PostgreSQL 14+
- Python 3.10+
- pip / virtual environment

### 2. Initialize database

```bash
createdb -U postgres healthcare_db
psql -U postgres -d healthcare_db -f healthcare_db_setup.sql
```

### 3. Set up Python environment

```bash
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Set environment variables

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=postgres
export DB_PASSWORD=postgres
export DB_NAME=healthcare_db
export OPENAI_API_KEY=<your-key>
```

### 5. Load sample data

```bash
python airflow/scripts/load_raw_data.py
```

### 6. Run dbt pipeline

```bash
cd dbt_healthcare
dbt deps            
dbt debug     
dbt run 
dbt test             
cd ..
```

### 7. Run AI triage agent

```bash
python airflow/scripts/ai_triage_agent.py
```

---

## Data Flow

### Raw Data
Synthetic EHR data loaded into `raw.*` schema:
- `raw.patients` - Patient demographics
- `raw.encounters` - Clinical encounters/visits
- `raw.conditions` - Active/historical diagnoses
- `raw.observations` - Vital signs and lab values
- `raw.medications` - Medication records

### Transformation Layer (dbt staging)
- `stg_patients` - Cleaned, typed patient records
- `stg_encounters` - Standardized encounter records with duration

### Analytics Layer (dbt marts)
- `dim_patients` - Patient dimension with encounter statistics
- `fct_clinical_events` - Fact table joining encounters, conditions, and vitals (one row per encounter)
- `high_risk_flag` - Materialized high-risk patient list (262 patients in demo)

### Output
- `ai_triage_summaries` - Risk assessments and clinical summaries from GPT-4o-mini

---

## AI Triage Agent

The `ai_triage_agent.py` script performs clinical triage:

1. **Query**: Fetches patients from `analytics_analytics.high_risk_flag`
2. **Build Prompt**: Constructs structured patient profile with vitals, conditions, medications
3. **Call LLM**: Sends to gpt-4o-mini with clinical triage system prompt
4. **Parse**: Extracts JSON response with risk_score (1-5), risk_label, summary
5. **Store**: Writes results to `analytics_analytics.ai_triage_summaries`

**High-risk criteria (any one qualifies):**
- 3+ active chronic conditions
- Systolic BP > 160 mmHg
- Glucose > 300 mg/dL
- BMI > 40
- >1 year since last encounter with active conditions

**Token Usage:** ~2,000-10,000 tokens per full pipeline run (typically <$0.01)

---

## DAG Tasks

| Task | Type | Purpose |
|---|---|---|
| `extract_load` | Python | Loads CSVs from `data/raw/` into PostgreSQL raw schema |
| `dbt_deps` | Bash | Installs dbt packages (dbt-utils) |
| `dbt_run` | Bash | Creates staging views and mart tables |
| `dbt_test` | Bash | Runs data quality tests |
| `ai_triage` | Python | Queries high-risk patients and generates assessments |

---

## Database Access

Query results directly or via GUI tools:

**Command line:**
```powershell
docker-compose exec postgres psql -U postgres -d healthcare_db \
  -c "SELECT patient_id, risk_score, risk_label FROM analytics_analytics.ai_triage_summaries LIMIT 10;"
```

**DBeaver (free GUI):**
1. Download: https://dbeaver.io/download/
2. New Connection -> PostgreSQL
3. Host: localhost, Port: 5432, DB: healthcare_db
4. User: postgres, Password: postgres
5. Browse to `analytics_analytics.ai_triage_summaries`

---

## Troubleshooting

**Issue: `dbt_utils` is undefined**
- Solution: Ensure `dbt_healthcare/packages.yml` exists and run `dbt deps` before `dbt run`

**Issue: `high_risk_flag` table not found**
- Cause: `dbt_run` failed or was skipped
- Solution: Check Airflow logs for `dbt_run` task; fix SQL errors and rebuild

**Issue: AI triage skips all patients**
- Cause: Patients already processed today (idempotent design)
- Solution: Clear `ai_triage_summaries` table and re-run

**Issue: OpenAI API errors**
- Check: OPENAI_API_KEY environment variable is set
- Check: API key is valid and has available credits

---

## Future Enhancements

- Add Metabase/Apache Superset dashboards
- Implement Great Expectations data quality framework
- Integration with Slack/PagerDuty for critical alerts
- Batch processing optimization for 10k+ patients
- Real-time streaming pipeline (Kafka/Spark)
- FHIR API wrapper for external EHR systems
- Prometheus + Grafana monitoring
