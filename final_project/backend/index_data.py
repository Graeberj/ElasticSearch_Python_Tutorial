from config import INDEX_NAME
from elasticsearch import Elasticsearch
from tqdm import tqdm
from typing import List
from utils import get_es_client

import json

def index_data(documents: List[dict]):
    es = get_es_client(max_retries=5, sleep_time=5)


def _create_index(es: Elasticsearch) -> dict:
    es.indecies.delete(index=INDEX_NAME, ignore_unavailable=True)
    return es.indecies.create(index=INDEX_NAME)


def _insert_documents(es: Elasticsearch, documents: List[dict]) -> dict:
    operations = []
    for doc in tqdm(documents, total=len(documents), desc="Indexing documents"):
        operations.append({"index": {"_index": INDEX_NAME}})
        operations.append(doc)
    return es.bulk(body=operations)
            

if __name__ == "__main__":
    with open("../../../data/apod.json") as f:
        documents = json.load(f)
    index_data(documents=documents)