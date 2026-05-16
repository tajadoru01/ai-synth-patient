# Healthcare Data Engineering & AI Triage Pipeline

An end-to-end ELT pipeline that ingests synthetic Electronic Health Records (EHRs),
transforms them with dbt, and runs an AI agent to generate clinical triage
summaries for high-risk patients. Orchestrated with Apache Airflow and deployed
via Docker Compose.

---

## Branches

| Branch | Approach | Transform Layer | Load Target |
|---|---|---|---|
| `main` | ELT | dbt (SQL, post-load) | `raw.*` → `analytics.*` |
| `etl_branch` | ETL | Apache Spark (Python, pre-load) | `analytics.*` directly |

**`main`** is the stable branch. Raw CSVs land in Postgres first, then dbt
transforms them in-place. Readable by analysts, easy to iterate on SQL logic.

**`etl_branch`** replaces the load + dbt steps with a single PySpark job that
transforms data before it touches the database. The `raw.*` schema is gone —
Postgres only ever receives clean, analytics-ready tables. Better suited for
hospital-scale data volumes; requires Java and a Spark cluster.

The AI triage agent (`ai_triage_agent.py`) and Airflow orchestration are shared
across both branches. Only the ingestion and transformation layers differ.

---

## Architecture

### main (ELT)

```
Synthea CSVs  -->  load_raw_data.py  -->  PostgreSQL (raw.*)
                                               |
                                        dbt deps + dbt run / test
                                               |
                                    PostgreSQL (analytics.*)
                                    ├── stg_patients (view)
                                    ├── stg_encounters (view)
                                    ├── dim_patients (table)
                                    ├── fct_clinical_events (table)
                                    └── high_risk_flag (table) --> ai_triage_agent.py
                                                                         |
                                                          analytics.ai_triage_summaries
```

**DAG:** `extract_load -> dbt_deps -> dbt_run -> dbt_test -> ai_triage`

### etl_branch (ETL)

```
Synthea CSVs  -->  spark_etl.py (Extract + Transform)
                        ├── transform_patients()
                        ├── transform_encounters()
                        ├── transform_dim_patients()
                        ├── transform_fct_clinical_events()
                        └── transform_high_risk_flag()
                                    |
                            PostgreSQL (analytics.*)
                            ├── dim_patients (table)
                            ├── fct_clinical_events (table)
                            └── high_risk_patients (table) --> ai_triage_agent.py
                                                                       |
                                                        analytics.ai_triage_summaries
```

**DAG:** `spark_etl -> ai_triage`

---

## Tech Stack

| Component | `main` | `etl_branch` |
|---|---|---|
| Orchestration | Apache Airflow 2.9 | Apache Airflow 2.9 |
| Database | PostgreSQL 16 | PostgreSQL 16 |
| Transformation | dbt-core 1.8 + dbt-utils | Apache Spark 3.5 (PySpark) |
| Language | Python 3.10+ | Python 3.10+ + Java 17 |
| LLM API | OpenAI (gpt-4o-mini) | OpenAI (gpt-4o-mini) |
| Containerisation | Docker & Docker Compose | Docker & Docker Compose |

---

## Project Structure

```
ai-synth-patient/
├── airflow/
│   ├── dags/
│   │   └── healthcare_pipeline_dag.py      # Airflow DAG definition
│   └── scripts/
│       ├── load_raw_data.py                # [main] CSV loader to PostgreSQL
│       ├── spark_etl.py                    # [etl_branch] PySpark ETL job
│       └── ai_triage_agent.py              # GPT-4o-mini triage engine (shared)
├── dbt_healthcare/                         # [main only]
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── stg_patients.sql
│   │   │   └── stg_encounters.sql
│   │   └── marts/
│   │       ├── schema.yml
│   │       ├── dim_patients.sql
│   │       ├── fct_clinical_events.sql
│   │       └── high_risk_flag.sql
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── packages.yml
├── tests/
│   ├── test_ai_triage_agent.py             # Unit tests (shared)
│   ├── test_load_raw_data.py               # Unit tests (main)
│   └── test_spark_etl.py                   # Unit tests (etl_branch)
├── data/
│   └── raw/                                # CSV data (patients, encounters, etc.)
├── docker/
│   └── 00_init.sql                         # PostgreSQL init script
├── docker-compose.yml
├── healthcare_db_setup.sql
├── requirements.txt
├── requirements-test.txt
├── requirements-test.in
├── Dockerfile
├── .github/
│   └── workflows/
│       └── ci.yml                          # GitHub Actions CI (branch-aware)
└── README.md
```

---

## Quick Start (Docker — Recommended)

### Prerequisites
- Docker Desktop (Windows/Mac) or Docker Engine + Compose v2 (Linux)
- OpenAI API key
- **`etl_branch` only:** Java 17 and a Spark connection configured in Airflow

### Setup & Run

1. Clone repository and configure:
```bash
git clone <repo-url>
cd ai-synth-patient

# to use the ETL branch:
git checkout etl_branch
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
- **`etl_branch` only:** Java 17 (`brew install --cask temurin@17` on macOS)

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

### 5. Run the pipeline

**`main` (ELT):**
```bash
python airflow/scripts/load_raw_data.py

cd dbt_healthcare
dbt deps
dbt run
dbt test
cd ..

python airflow/scripts/ai_triage_agent.py
```

**`etl_branch` (ETL):**
```bash
python airflow/scripts/spark_etl.py
python airflow/scripts/ai_triage_agent.py
```

---

## Data Flow

### main — Raw Schema (ELT)
Synthetic EHR data loaded into `raw.*` first, then transformed by dbt:
- `raw.patients` — patient demographics
- `raw.encounters` — clinical encounters/visits
- `raw.conditions` — active/historical diagnoses
- `raw.observations` — vital signs and lab values
- `raw.medications` — medication records

### etl_branch — No Raw Schema (ETL)
Spark transforms data before it reaches Postgres. No `raw.*` schema exists.

### Analytics Layer (both branches)
- `dim_patients` — patient dimension with encounter statistics
- `fct_clinical_events` — encounters × conditions × vitals (one row per encounter)
- `high_risk_flag` / `high_risk_patients` — materialized high-risk patient list

### Output (both branches)
- `ai_triage_summaries` — risk assessments and clinical summaries from GPT-4o-mini

---

## AI Triage Agent

The `ai_triage_agent.py` script performs clinical triage:

1. **Query**: Fetches patients from the high-risk table
2. **Build Prompt**: Constructs structured patient profile with vitals, conditions, medications
3. **Call LLM**: Sends to gpt-4o-mini with clinical triage system prompt
4. **Parse**: Extracts JSON response with risk_score (1-5), risk_label, summary
5. **Store**: Writes results to `ai_triage_summaries`

**High-risk criteria (any one qualifies):**
- 3+ active chronic conditions
- Systolic BP > 160 mmHg
- Glucose > 300 mg/dL
- BMI > 40
- >1 year since last encounter with active conditions

**Token Usage:** ~2,000-10,000 tokens per full pipeline run (typically <$0.01)

---

## DAG Tasks

### main
| Task | Type | Purpose |
|---|---|---|
| `extract_load` | Python | Loads CSVs into PostgreSQL raw schema |
| `dbt_deps` | Bash | Installs dbt packages |
| `dbt_run` | Bash | Creates staging views and mart tables |
| `dbt_test` | Bash | Runs data quality tests |
| `ai_triage` | Python | Generates triage assessments |

### etl_branch
| Task | Type | Purpose |
|---|---|---|
| `spark_etl` | SparkSubmit | Extracts CSVs, transforms, loads to analytics schema |
| `ai_triage` | Python | Generates triage assessments |

---

## Running Tests

```bash
pip install -r requirements-test.txt

# main — no Java needed
pytest tests/ --ignore=tests/test_spark_etl.py -v

# etl_branch — requires Java 17
pytest tests/test_spark_etl.py -v
```

Test dependencies are pinned in `requirements-test.txt` and managed via
`pip-tools`. To regenerate after adding a dependency:

```bash
echo "new-package" >> requirements-test.in
pip-compile requirements-test.in --output-file requirements-test.txt
pip install -r requirements-test.txt
```

---

## CI/CD

CI runs automatically on every push and pull request via GitHub Actions
(`.github/workflows/ci.yml`). The pipeline is branch-aware — Spark tests
only run on `etl_branch` since they require Java.

### Jobs

| Job | Branches | What it does |
|---|---|---|
| `test` | `main`, `master` | Runs pandas-based unit tests with coverage; uploads report to Codecov |
| `test-spark` | `etl_branch` | Installs Java 17, runs PySpark unit tests with coverage |
| `lint` | all | Runs `ruff check` over `airflow/scripts/`; fails on unused imports, style violations |
| `docker-build` | all | Builds the Docker image tagged with the commit SHA; acts as a deployment gate |

`docker-build` only runs after `lint` passes. No broken code can produce a
deployable image.

### Coverage

Test coverage is uploaded to [Codecov](https://codecov.io) on every run.
The build does not fail if Codecov is unavailable.

### Adding a CD step

When a target environment exists, extend the `docker-build` job to push the
image and trigger a rollout:

```yaml
- name: Push to ECR
  if: github.ref == 'refs/heads/main'
  run: |
    aws ecr get-login-password --region us-east-1 \
      | docker login --username AWS --password-stdin $ECR_REGISTRY
    docker tag healthcare-airflow:${{ github.sha }} $ECR_REGISTRY/healthcare-airflow:latest
    docker push $ECR_REGISTRY/healthcare-airflow:latest
```

Store `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `ECR_REGISTRY` as
GitHub repository secrets under Settings -> Secrets and variables -> Actions.

---

## Database Access

**Command line:**
```powershell
docker-compose exec postgres psql -U postgres -d healthcare_db \
  -c "SELECT patient_id, risk_score, risk_label FROM analytics.ai_triage_summaries LIMIT 10;"
```

**DBeaver (free GUI):**
1. Download: https://dbeaver.io/download/
2. New Connection -> PostgreSQL
3. Host: localhost, Port: 5432, DB: healthcare_db
4. User: postgres, Password: postgres

---

## Troubleshooting

**Issue: `dbt_utils` is undefined** (`main` only)
- Solution: Ensure `dbt_healthcare/packages.yml` exists and run `dbt deps` before `dbt run`

**Issue: `high_risk_flag` table not found** (`main` only)
- Cause: `dbt_run` failed or was skipped
- Solution: Check Airflow logs for `dbt_run` task; fix SQL errors and rebuild

**Issue: PySpark job fails with `java.lang.UnsupportedClassVersionError`** (`etl_branch` only)
- Cause: Wrong Java version
- Solution: Ensure Java 17 is installed and `JAVA_HOME` is set correctly

**Issue: AI triage skips all patients**
- Cause: Patients already processed today (idempotent design)
- Solution: Clear `ai_triage_summaries` table and re-run

**Issue: OpenAI API errors**
- Check: `OPENAI_API_KEY` environment variable is set
- Check: API key is valid and has available credits

---

## Future Enhancements

- Add Metabase/Apache Superset dashboards
- Implement Great Expectations data quality framework
- Integration with Slack/PagerDuty for critical alerts
- Batch processing optimization for 10k+ patients
- Real-time streaming pipeline (Kafka/Spark Structured Streaming)
- FHIR API wrapper for external EHR systems
- Prometheus + Grafana monitoring