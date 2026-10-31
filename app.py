import hashlib
import hmac
import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query

INDEX_DIR = Path(os.getenv("INDEX_DIR", "index"))
SHARD_COUNT = int(os.getenv("SHARD_COUNT", "256"))
API_KEY = os.getenv("API_KEY", "")

app = FastAPI(title="India DB Indexer", version="1.0.0")


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    # Normalize common India prefixes while retaining international numbers.
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    return digits


def shard_for(phone: str) -> int:
    digest = hashlib.sha256(phone.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % SHARD_COUNT


def authorize(x_api_key: str | None) -> None:
    if not API_KEY:
        raise HTTPException(status_code=503, detail="API_KEY is not configured")
    if not x_api_key or not hmac.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="Invalid API key")


@app.get("/health")
def health():
    return {
        "ok": True,
        "index_ready": (INDEX_DIR / "manifest.json").exists(),
        "shards": SHARD_COUNT,
    }


@app.get("/search")
def search(
    phone: str = Query(..., min_length=5, max_length=20),
    x_api_key: str | None = Header(default=None),
):
    authorize(x_api_key)
    normalized = normalize_phone(phone)
    if len(normalized) < 5:
        raise HTTPException(status_code=400, detail="Invalid phone number")

    shard = INDEX_DIR / f"part-{shard_for(normalized):03d}.sqlite"
    if not shard.exists():
        return {"success": True, "phone": normalized, "total_records": 0, "results": []}

    try:
        with sqlite3.connect(f"file:{shard.resolve()}?mode=ro", uri=True, timeout=5) as con:
            rows = con.execute(
                "SELECT table_name, row_data FROM records WHERE phone = ? LIMIT 1000",
                (normalized,),
            ).fetchall()
    except sqlite3.Error as exc:
        raise HTTPException(status_code=503, detail="Index shard unavailable") from exc

    results = []
    for table_name, row_data in rows:
        try:
            record = json.loads(row_data)
        except (TypeError, json.JSONDecodeError):
            record = {}
        results.append({"table": table_name, "record": record})

    return {
        "success": True,
        "phone": normalized,
        "total_records": len(results),
        "results": results,
        "truncated": len(results) == 1000,
    }
