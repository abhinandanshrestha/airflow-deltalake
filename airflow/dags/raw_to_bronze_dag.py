import boto3
import json
import os
import logging
from airflow.decorators import task
from airflow import DAG
from datetime import datetime
from langchain.text_splitter import RecursiveCharacterTextSplitter
import duckdb
from deltalake import write_deltalake
import pandas as pd
from airflow import Dataset

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
BUCKET_NAME = os.getenv("BUCKET_NAME", "datalake")

meta_data=Dataset(f"s3://{BUCKET_NAME}/raw/metadata.json")
trigger_silver=Dataset("s3://trigger_silver")

# boto3 S3 client with endpoint override for MinIO or S3-compatible storage
s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

def read_text_files_from_s3(bucket, prefix, filenames_without_extension):
    texts = []
    filenames = []

    for base_filename in filenames_without_extension:
        key = f"{prefix}{base_filename}.txt"
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            texts.append(content)
            filenames.append(f"{base_filename}.txt")
            logger.info(f"Read text file from S3: {key}")
        except s3.exceptions.NoSuchKey:
            logger.warning(f"File not found in S3: {key}")
        except Exception as e:
            logger.error(f"Error reading file {key}: {e}")

    return texts, filenames

def read_metadata_from_s3(bucket, metadata_key):
    response = s3.get_object(Bucket=bucket, Key=metadata_key)
    metadata_json = json.loads(response["Body"].read().decode("utf-8"))
    logger.info(f"Read metadata file from S3: {metadata_key}")
    return metadata_json

@task
def process_s3_text_to_chunks():
    
    filename_to_metadata = read_metadata_from_s3(BUCKET_NAME, 'raw/metadata.json')
    logger.info(f"filename in metadata: {filename_to_metadata}")
    texts, filenames = read_text_files_from_s3(BUCKET_NAME, 'raw/text/', list(filename_to_metadata.keys()))
    logger.info(f"Textf from raw: {len(texts), filenames}")
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=500,
        chunk_overlap=100,
        length_function=len
    )

    all_chunks = []

    for filename, text in zip(filenames, texts):
        chunks = text_splitter.split_text(text)

        metadata = filename_to_metadata.get(filename.split('.')[0], {})
        url = metadata.get('url', 'unknown_url')
        title = metadata.get('title', 'unknown_title')
        published = metadata.get('published', 'unknown_published')
        summary = metadata.get('summary', 'unknown_summary')

        for i, chunk in enumerate(chunks):
            all_chunks.append({
                'chunk_number': i + 1,
                'text': chunk,
                'url': url,
                'title': title,
                'published': published,
                'summary_of_url': summary
            })

    logger.info(f"Total chunks processed: {len(all_chunks)}")
    if all_chunks:
        logger.info(f"Sample chunk: {all_chunks[0]}")

    return all_chunks

@task(outlets=[trigger_silver])
def save_chunks_to_deltalake(all_chunks):
    # Create a DataFrame from the chunks
    if all_chunks:
        df=pd.DataFrame(all_chunks)

        # Create in-memory DuckDB connection
        con = duckdb.connect()

        # Register your DataFrame as a DuckDB table
        con.register("raw", df)

        # Transform: Add `id` using md5(url || text)
        df_with_id = con.execute("""
        SELECT
            md5(url || text) AS id,
            *
        FROM raw;
        """).fetchdf()

        delta_path = "s3://datalake/bronze"

        # ✅ Pass storage_options directly
        storage_options = {
            "AWS_ACCESS_KEY_ID": os.getenv("S3_ACCESS_KEY", "minioadmin"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("S3_SECRET_KEY", "minioadmin"),
            "AWS_REGION": "us-east-1",
            "AWS_ENDPOINT_URL": f"http://{os.getenv('S3_ENDPOINT', 'minio:9000')}",
            "AWS_ALLOW_HTTP": "true"  # ✅ This line is essential
        }

        write_deltalake(
            delta_path,
            df_with_id,
            mode="append",
            storage_options=storage_options
        )

        logger.info(f"Wrote {len(df_with_id)} records to Delta Lake at {delta_path}")

        logger.info(f"Wrote {len(df_with_id)} rows to Delta Lake at {delta_path}")
    else:
        logging.info("No need further processing")
        
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 30),
    'retries': 1,
}

with DAG(
    dag_id='raw_to_bronze_dag',
    schedule=[meta_data],
    default_args=default_args,
    catchup=False,
) as dag:

    process_task = process_s3_text_to_chunks()
    save_task = save_chunks_to_deltalake(process_task)
    process_task >> save_task