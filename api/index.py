from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import json
import time

app = FastAPI(
    title="India DB Indexer — Synthetic Demo",
    version="1.0.0",
    description="A lookup and latency-testing API backed only by bundled synthetic fixtures."
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "synthetic_demo.json"

def load_records():
    try:
        return json.loads(DATA_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}

# Small in-memory fixture index: constant-time dictionary lookup.
INDEX = load_records()

@app.get("/")
def root():
    return {
        "name": "India DB Indexer — Synthetic Demo",
        "status": "ok",
        "dataset": "bundled synthetic fixtures only",
        "routes": ["/search?account_id=TEST100001", "/health"]
    }

@app.get("/health")
def health():
    return {"status": "ok", "records": len(INDEX), "mode": "synthetic-demo"}

@app.get("/search")
def search(account_id: str = Query(..., min_length=1, max_length=64)):
    started = time.perf_counter_ns()
    key = account_id.strip()
    record = INDEX.get(key)
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000

    if record is None:
        raise HTTPException(status_code=404, detail="Synthetic test account_id not found")

    return {
        "account_id": key,
        "phone": record["phone"],
        "lookup_ms": round(elapsed_ms, 6),
        "source": "synthetic-demo"
    }
