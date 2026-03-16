CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
CREATE DATABASE healthcare_db OWNER postgres;
GRANT CONNECT ON DATABASE healthcare_db TO airflow;