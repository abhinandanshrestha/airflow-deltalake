import os
import logging
from airflow.decorators import task
from airflow import DAG, Dataset
from datetime import datetime
import pandas as pd
import duckdb
from deltalake import DeltaTable, write_deltalake
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

bronze_path = "s3://datalake/bronze"
silver_path = "s3://datalake/silver"

trigger_silver=Dataset("s3://trigger_silver")
trigger_gold=Dataset("s3://trigger_gold")

# Get S3 endpoint - ensure consistency
s3_endpoint = os.getenv('S3_ENDPOINT', 'minio:9000')
s3_access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
s3_secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
s3_region = os.getenv("S3_REGION", "us-east-1")

# Delta Lake storage options - ensure proper endpoint format
storage_options = {
    "AWS_ACCESS_KEY_ID": s3_access_key,
    "AWS_SECRET_ACCESS_KEY": s3_secret_key,
    "AWS_REGION": s3_region,
    "AWS_ENDPOINT_URL": f"http://{s3_endpoint}",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",  # Often needed for MinIO
    "AWS_EC2_METADATA_DISABLED": "true"
}

con = duckdb.connect()

@task(outlets=[trigger_gold])
def bronze_to_silver_sync():
    try:
        # Load necessary extensions
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
        con.execute(f"""
            CREATE OR REPLACE VIEW bronze AS 
            SELECT * FROM delta_scan('{bronze_path}');
        """)

        try:
            con.execute(f"""
                CREATE OR REPLACE VIEW silver AS 
                SELECT * FROM delta_scan('{silver_path}');
            """)
        except Exception:
            con.execute("CREATE OR REPLACE VIEW silver AS SELECT * FROM bronze WHERE false;")

        new_data_df = con.execute("""
        WITH new_or_updated AS (
            SELECT * FROM bronze
            WHERE id NOT IN (SELECT id FROM silver)
        )
        SELECT
            id,
            text,
            url,
            chunk_number,
            title,
            STRPTIME(published, '%a, %d %b %Y %H:%M:%S GMT')::timestamp AS published_on_utc,
            summary_of_url
        FROM new_or_updated;
        """).fetchdf()

        if not new_data_df.empty:
            try:
                silver_table = DeltaTable(silver_path, storage_options=storage_options)
                for row_url in new_data_df["url"].unique():
                    logger.info(f"Deleting rows in Silver with url = {row_url}")
                    silver_table.delete(predicate=f"url = '{row_url}'")
            except Exception as e:
                logger.warning(f"Could not delete old rows from Silver: {e}")

            write_deltalake(
                silver_path,
                new_data_df,
                mode="append",
                storage_options=storage_options
            )
            logger.info(f"Appended {len(new_data_df)} new/updated records to Silver.")
        else:
            logger.info("No new or updated records to process.")
    except Exception as e:
        logger.error(f"Error during Bronze to Silver sync: {e}")
        raise
        
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='bronze_to_silver_dag',
    schedule=[trigger_silver],
    default_args=default_args,
    catchup=False,
) as dag:
    bronze_to_silver_task = bronze_to_silver_sync()

    bronze_to_silver_task