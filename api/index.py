import json
import logging
import os
import time
from functools import lru_cache

import pyarrow.dataset as ds
from fastapi import FastAPI, HTTPException, Query
from huggingface_hub import HfFileSystem

# Source data: ONLY the ten Parquet files directly inside tg/processed/.
HF_BUCKET = os.getenv(
    "HF_BUCKET",
    "buckets/Chatpataprani/Hitek_ImCR_API-bucket",
).rstrip("/")
SOURCE_PREFIX = f"{HF_BUCKET}/tg/processed"
INDEX_PREFIX = f"{SOURCE_PREFIX}/account-id-index"
HF_TOKEN = os.getenv("HF_TOKEN") or None
SHARD_COUNT = int(os.getenv("INDEX_SHARDS", "1024"))
CACHE_SIZE = int(os.getenv("CACHE_SIZE", "2048"))

app = FastAPI(
    title="India DB Indexer",
    version="2.0.0",
    description="Exact account_id lookup backed by a Parquet hash-shard index.",
)
logger = logging.getLogger("india-db-indexer")
fs = HfFileSystem(token=HF_TOKEN)

try:
    with fs.open(f"{INDEX_PREFIX}/manifest.json", "rb") as stream:
        MANIFEST = json.loads(stream.read().decode("utf-8"))
    if MANIFEST.get("version") != 1 or int(MANIFEST.get("shard_count", 0)) != SHARD_COUNT:
        raise RuntimeError("Index manifest version or shard count does not match API settings.")
    if not isinstance(MANIFEST.get("files"), dict):
        raise RuntimeError("Index manifest is missing its shard file map.")
    INDEX_READY = True
except FileNotFoundError:
    MANIFEST = {}
    INDEX_READY = False
except Exception:
    logger.exception("Index manifest could not be loaded.")
    MANIFEST = {}
    INDEX_READY = False


def clean(value):
    if value is None:
        return None
    value = str(value).strip()
    return value if value and value.lower() not in {"null", "none", "nan", "n/a"} else None


@lru_cache(maxsize=CACHE_SIZE)
def lookup_cached(account_id):
    shard = str(int(account_id) % SHARD_COUNT)
    relative_files = MANIFEST["files"].get(shard, [])
    if not relative_files:
        return None

    paths = [f"{HF_BUCKET}/{path.lstrip('/')}" for path in relative_files]
    dataset = ds.dataset(paths, filesystem=fs, format="parquet")
    scanner = dataset.scanner(
        filter=ds.field("account_id") == account_id,
        columns=["account_id", "phone"],
        batch_size=2048,
        use_threads=True,
    )
    for batch in scanner.to_batches():
        for row in batch.to_pylist():
            if clean(row.get("account_id")) == account_id:
                return {
                    "account_id": account_id,
                    "phone": clean(row.get("phone")),
                }
    return None


@app.get("/")
def root():
    return {
        "status": "online" if INDEX_READY else "index_not_built",
        "name": "India DB Indexer",
        "lookup": "/search?account_id=<numeric_id>",
        "health": "/health",
        "docs": "/docs",
    }


@app.get("/health")
def health():
    return {
        "status": "ok" if INDEX_READY else "index_not_ready",
        "index_ready": INDEX_READY,
        "shards": SHARD_COUNT if INDEX_READY else 0,
    }


@app.get("/search")
def search(account_id: str = Query(..., min_length=1, max_length=32)):
    key = account_id.strip()
    if not key.isdigit():
        raise HTTPException(status_code=400, detail="account_id must contain digits only.")
    if not INDEX_READY:
        raise HTTPException(status_code=503, detail="Index has not been built yet.")

    try:
        started = time.perf_counter_ns()
        result = lookup_cached(key)
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    except Exception as exc:
        logger.exception("Indexed lookup failed.")
        raise HTTPException(status_code=503, detail="Index lookup temporarily failed.") from exc

    if result is None:
        raise HTTPException(status_code=404, detail="account_id not found.")

    # Deliberately return only the two requested database fields.
    response = {"account_id": result["account_id"], "phone": result["phone"]}
    return response
