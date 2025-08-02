from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, json, re, feedparser, requests, html2text
import boto3
from botocore.exceptions import NoCredentialsError
from airflow.decorators import task
import time
import logging
from uuid import uuid4
from airflow import Dataset

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# MinIO/S3 config
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
BUCKET_NAME = os.getenv("BUCKET_NAME", "datalake")

meta_data=Dataset(f"s3://{BUCKET_NAME}/raw/metadata.json")

# S3 client
s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

# Utility functions
def extract_body_html(raw_html: str) -> str:
    match = re.search(r"<article[^>]*>(.*?)</article>", raw_html, re.DOTALL | re.IGNORECASE)
    return match.group(1).strip() if match else ""

def convert_to_markdown(body_html: str) -> str:
    text_maker = html2text.HTML2Text()
    text_maker.ignore_links = False
    text_maker.bypass_tables = False
    text_maker.ignore_images = True
    text_maker.ignore_emphasis = False
    return text_maker.handle(body_html)

def upload_to_s3(key: str, content: str, content_type: str = "text/plain"):
    try:
        # Ensure bucket exists, or create it
        try:
            s3.head_bucket(Bucket=BUCKET_NAME)
        except s3.exceptions.ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                logger.info(f"Bucket '{BUCKET_NAME}' does not exist. Creating...")
                s3.create_bucket(Bucket=BUCKET_NAME)
            else:
                raise

        # Upload the object
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=content.encode("utf-8"), ContentType=content_type)
        logger.info(f"✅ Uploaded: {key}")

    except NoCredentialsError:
        logger.error("❌ Failed to authenticate with MinIO. Check credentials.")
    except Exception as e:
        logger.error(f"❌ Failed to upload {key}: {e}")

def read_metadata_from_s3(bucket, metadata_key):
    try:
        response = s3.get_object(Bucket=bucket, Key=metadata_key)
        metadata_json = json.loads(response["Body"].read().decode("utf-8"))
        logger.info(f"Read metadata file from S3: {metadata_key}")
        return metadata_json
    except Exception as e:
        return {} # Return Empty dictionary incase of no json if found indicating that it's full load
    
@task(outlets=[meta_data])
def process_rss_feed():
    feed = feedparser.parse("https://feeds.bbci.co.uk/news/technology/rss.xml")

    filename_to_metadata = {
        f"bbc_article_{str(uuid4())}": {
            "url": entry.link,
            "title": entry.title,
            "published": entry.published,
            "summary": entry.summary,
        }
        for i, entry in enumerate(feed.entries)
    }
    logger.info(f"filename_to_metadata:{filename_to_metadata}")
    existing_values = list(read_metadata_from_s3('datalake', 'raw/metadata.json').values())
    incoming_values = list(filename_to_metadata.values())

    # Find new entries in incoming that don't exist in existing
    new_entries = [item for item in incoming_values if item not in existing_values]
    logger.info(f"urls to crawl:{new_entries}")
    filename_to_metadata_to_save={}

    for entry in new_entries:
        base_name = f"bbc_article_{str(uuid4())}"  
        
        try:
            response = requests.get(entry['url'], timeout=10)
            if response.status_code != 200:
                logger.error(f"❌ Failed to fetch {entry['url']}")
                continue

            html = response.text
            body_html = extract_body_html(html)
            markdown_text = convert_to_markdown(body_html)

            # Upload files to MinIO
            upload_to_s3(f"raw/html/{base_name}.html", html, "text/html")
            upload_to_s3(f"raw/html/{base_name}_body.html", body_html, "text/html")
            upload_to_s3(f"raw/text/{base_name}.txt", markdown_text, "text/plain")

            logger.info(f"✅ Uploaded article {base_name}: {entry['url']}")
            filename_to_metadata_to_save[base_name]=entry
        except Exception as e:
            logger.warning(f"⚠️ Error processing {entry['url']}: {e}")
        time.sleep(1)

    # Upload metadata JSON
    try:
        metadata_json = json.dumps(filename_to_metadata_to_save, ensure_ascii=False, indent=2)
        upload_to_s3("raw/metadata.json", metadata_json, "application/json")
        logger.info("✅ Uploaded metadata.json")
    except Exception as e:
        logger.warning(f"⚠️ Failed to upload metadata.json: {e}")

# DAG Definition
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "crawl_and_land_to_minio",
    default_args=default_args,
    description="Extract articles from BBC RSS and upload to MinIO",
    schedule=None,
    catchup=False,
) as dag:

    fetch_and_upload = process_rss_feed()

    fetch_and_upload
