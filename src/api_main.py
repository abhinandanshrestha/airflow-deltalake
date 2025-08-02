from fastapi import FastAPI, Query, Request
from models import RAGResponse
from sentence_transformers import SentenceTransformer
from chromadb import PersistentClient
import numpy as np
import logging, os
import requests
from service import generate_response, extract_urls
from chromadb import HttpClient
import torch, json
from datetime import datetime

# If you're running inside docker-compose, use service name like "chromadb"
client = HttpClient(host="chromadb", port=8000) 

# Initialize logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

device = 'cuda' if torch.cuda.is_available() else 'cpu'
logging.info(f"Device: {device}")

print(f"Device: {device}")

# Load model and Chroma client
model = SentenceTransformer('/content/local_model', device=device)
collection = client.get_or_create_collection(name="gold_collection")

# FastAPI app
app = FastAPI(title="RAG API")

# /rag endpoint
@app.get("/ask", response_model=RAGResponse)
def rag_search(query: str):
    logger.info(f"Received RAG query: {query}")

    # Embed the query
    query_embedding = model.encode(query)

    # Perform similarity search
    results = collection.query(
        query_embeddings=[query_embedding.tolist()],
        n_results=int(os.getenv('TOP_K')),
    )

    matched_docs = []
    for doc, meta, doc_id, distance in zip(
        results["documents"][0],
        results["metadatas"][0],
        results["ids"][0],
        results["distances"][0],
    ):
        matched_docs.append({
            "id": doc_id,
            "text": doc,
            "metadata": meta,
            "distance": distance,
        })

    prompt = f"""
    You are an AI assistant that provides answers based on the provided context. Answer as short as possible in a sentence.
    Context: {[row['text'] for row in matched_docs]}
    Question: {query}
    Answer:
    """
    response=generate_response(prompt=prompt)
    sources=set(extract_urls(matched_docs))
    return RAGResponse(query=query, response=response['choices'][0]['text'],sources=sources,retriever=matched_docs, )

@app.delete("/clear-collection")
def clear_gold_collection():
    """
    Delete all records from the 'gold_collection' ChromaDB collection.
    """
    # Fetch all document IDs in the collection
    all_ids = collection.get(include=[])["ids"]

    if not all_ids:
        return {"status": "success", "message": "No records to delete in 'gold_collection'"}

    # Delete them
    collection.delete(ids=all_ids)
    logger.info("Cleared all records from 'gold_collection'")
    return {"status": "success", "message": f"Deleted {len(all_ids)} records from 'gold_collection'"}


FILE_PATH = "./openlineage_events.jsonl"
os.makedirs(os.path.dirname(FILE_PATH), exist_ok=True) if os.path.dirname(FILE_PATH) else None

@app.post("/api/v1/lineage")
async def receive_lineage_event(request: Request):
    event = await request.json()
    line = json.dumps(event) + "\n"
    with open(FILE_PATH, "a") as f:
        f.write(line)
    return {"status": "event logged", "timestamp": datetime.utcnow().isoformat()}