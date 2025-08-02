from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime
import logging, duckdb, json, os
from deltalake import DeltaTable
# from chromadb import PersistentClient
from sentence_transformers import SentenceTransformer
from chromadb import HttpClient



# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# gold_path = "/mnt/datalake/gold"

# # Initialize persistent Chroma client
# client = PersistentClient(path="/mnt/datalake/embed/chromastore")

# Setup DuckDB and load gold table
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

trigger_embed=Dataset("s3://trigger_embed")


@task
def embed_gold():
    # If you're running inside docker-compose, use service name like "chromadb"
    client = HttpClient(host="chromadb", port=8000) 
    
    logger.info("Loading embedding model...")
    model = SentenceTransformer('/content/local_model', device='cuda')  # adjust path if needed

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

    # Step 2: Get or create the collection
    collection = client.get_or_create_collection(name="gold_collection", metadata={"source": "gold_delta"})

    con.execute(f"CREATE OR REPLACE VIEW gold AS SELECT * FROM delta_scan('{gold_path}')")

    # Step 4: Get existing IDs in Chroma collection for deduplication
    existing_ids = set(collection.get(ids=None)['ids'])

    # Step 5: Query Gold for new or updated rows
    if existing_ids:
        query = f"""
        SELECT id, text, metadata
        FROM gold
        WHERE id NOT IN ({','.join(f"'{eid}'" for eid in existing_ids)})
        """
    else:
        query = "SELECT id, text, metadata FROM gold"

    new_rows = con.execute(query).fetchall()

    if not new_rows:
        logger.info("No new rows to add.")
        return

    # Step 6: Prepare data
    ids_to_add = []
    texts_to_embed = []
    metadatas_to_add = []

    for row_id, text, metadata_json in new_rows:
        ids_to_add.append(row_id)
        texts_to_embed.append(text or "")
        try:
            metadatas_to_add.append(json.loads(metadata_json))
        except Exception as e:
            logger.warning(f"Failed to parse metadata for ID {row_id}: {e}")
            metadatas_to_add.append({})

    # Step 8: Generate embeddings
    logger.info("Generating embeddings...")
    embeddings = model.encode(texts_to_embed, show_progress_bar=True)

    # Step 9: Add to Chroma collection
    collection.add(
        ids=ids_to_add,
        documents=texts_to_embed,
        metadatas=metadatas_to_add,
        embeddings=embeddings.tolist()
    )

    logger.info(f"✅ Added {len(ids_to_add)} new documents to Chroma collection.")

# DAG Definition
default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='embed_gold_to_chroma',
    schedule=[trigger_embed],  # Run manually or trigger via API
    default_args=default_args,
    catchup=False,
    tags=["embedding", "chroma", "deltalake"],
) as dag:
    
    embed_task=embed_gold()
    embed_task