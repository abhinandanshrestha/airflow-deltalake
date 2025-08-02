FROM apache/airflow:slim-latest-python3.12

USER root

RUN apt-get update && apt-get install -y gnupg2 software-properties-common curl
RUN apt-get install -y openjdk-17-jre-headless

USER airflow

COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN pip install psycopg2-binary asyncpg
RUN pip install apache-airflow-providers-openlineage