FROM apache/airflow:2.9.1-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
        python3-dev \
        libc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir psycopg2-binary==2.9.9

RUN pip install --no-cache-dir \
        dbt-core==1.8.0 \
        dbt-postgres==1.8.0

RUN pip install --no-cache-dir \
        snowplow-tracker>=0.10.0 \
        agate>=1.7.0 \
        click>=8.0.0 \
        colorama>=0.3.9 \
        hologram>=0.0.14 \
        isodate>=0.6.0 \
        jinja2>=3.1.2 \
        logbook>=1.0 \
        mashumaro>=3.9 \
        minimal-snowplow-tracker>=0.0.2 \
        msgpack \
        networkx>=2.3 \
        packaging>=22.0 \
        pathspec>=0.9.0 \
        protobuf>=4.0.0 \
        pytz>=2015.7 \
        pyyaml>=6.0 \
        requests>=2.26.0 \
        sqlparse>=0.2.3 \
        typing-extensions>=3.7.4 \
        urllib3>=1.26.0 \
        more-itertools \
        pytimeparse \
        python-slugify \
        leather \
        parsedatetime \
        openai==1.30.0 \
        pandas==2.2.2 \
        python-dotenv==1.0.1