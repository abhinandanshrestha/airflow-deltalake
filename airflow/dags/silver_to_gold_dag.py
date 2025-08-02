import os
import json
import logging
from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import duckdb
from deltalake import DeltaTable, write_deltalake

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

silver_path = "s3://datalake/silver"
gold_path = "s3://datalake/gold"

# S3 config for MinIO
s3_endpoint = os.getenv('S3_ENDPOINT', 'minio:9000')
s3_access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
s3_secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
s3_region = os.getenv("S3_REGION", "us-east-1")

storage_options = {
    "AWS_ACCESS_KEY_ID": s3_access_key,
    "AWS_SECRET_ACCESS_KEY": s3_secret_key,
    "AWS_REGION": s3_region,
    "AWS_ENDPOINT_URL": f"http://{s3_endpoint}",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_EC2_METADATA_DISABLED": "true"
}


trigger_gold=Dataset("s3://trigger_gold")
trigger_embed=Dataset("s3://trigger_embed")

@task(outlets=[trigger_embed])
def silver_to_gold_sync():
    try:
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL delta; LOAD delta;")

        con.execute(f"""
            CREATE SECRET delta_s4 (
                TYPE s3,
                KEY_ID '{s3_access_key}',
                SECRET '{s3_secret_key}',
                REGION '{s3_region}',
                ENDPOINT '{s3_endpoint}',
                USE_SSL false,
                URL_STYLE 'path',
                SCOPE 's3://datalake'
            );
        """)

        # Create views
        con.execute(f"CREATE OR REPLACE VIEW silver AS SELECT * FROM delta_scan('{silver_path}');")

        try:
            con.execute(f"CREATE OR REPLACE VIEW gold AS SELECT id FROM delta_scan('{gold_path}');")
        except:
            con.execute("CREATE OR REPLACE VIEW gold AS SELECT * FROM silver WHERE false;")

        gold_df = con.execute("""
        WITH new_data AS (
            SELECT * FROM silver
            WHERE id NOT IN (SELECT id FROM gold)
        )
        SELECT
            id,
            text,
            JSON_OBJECT(
                'url', url,
                'title', title,
                'chunk_number', chunk_number,
                'published_on_utc', published_on_utc,
                'summary_of_url', summary_of_url
            ) AS metadata
        FROM new_data;
        """).fetchdf()

        # Collect URLs to delete from gold
        urls_to_delete = set()
        for row_metadata in gold_df["metadata"]:
            try:
                metadata_dict = json.loads(row_metadata)
                url = metadata_dict.get("url")
                if url:
                    urls_to_delete.add(url)
            except json.JSONDecodeError:
                continue

        if not gold_df.empty:
            try:
                gold_table = DeltaTable(gold_path, storage_options=storage_options)
                for url in urls_to_delete:
                    logger.info(f"Deleting rows in Gold where URL = {url}")
                    gold_table.delete(predicate=f"metadata ILIKE '%\"url\": \"{url}\"%'")
            except Exception as e:
                logger.warning(f"Failed to delete from Gold: {e}")

            write_deltalake(
                gold_path,
                gold_df,
                mode="append",
                storage_options=storage_options
            )
            logger.info(f"Appended {len(gold_df)} new records to Gold.")
        else:
            logger.info("No new data to insert into Gold.")

    except Exception as e:
        logger.error(f"Error during Silver to Gold sync: {e}")
        raise

# DAG configuration
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='silver_to_gold_dag',
    schedule=[trigger_gold],
    default_args=default_args,
    catchup=False,
) as dag:
    silver_to_gold_task = silver_to_gold_sync()
    silver_to_gold_task
