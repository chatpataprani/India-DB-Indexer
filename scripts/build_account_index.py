#!/usr/bin/env python3
"""Build an exact account_id hash-shard index from tg/processed/*.parquet.

Source files are opened read-only. Output is written only under
 tg/processed/account-id-index/ in the same Hugging Face bucket.
"""
import json
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.dataset as ds
from huggingface_hub import HfFileSystem, batch_bucket_files

HF_BUCKET = os.getenv("HF_BUCKET", "buckets/Chatpataprani/Hitek_ImCR_API-bucket").rstrip("/")
SOURCE_PREFIX = f"{HF_BUCKET}/tg/processed"
BUCKET_ID = HF_BUCKET.removeprefix("buckets/")
TOKEN = os.environ["HF_TOKEN"]
SHARD_COUNT = int(os.getenv("INDEX_SHARDS", "1024"))
REMOTE_PREFIX = "tg/processed/account-id-index"
OUTPUT_DIR = Path("/tmp/india-db-account-index")
TEMP_DIR = Path("/tmp/india-db-duckdb")

if SHARD_COUNT < 1:
    raise ValueError("INDEX_SHARDS must be at least 1")

shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
shutil.rmtree(TEMP_DIR, ignore_errors=True)
OUTPUT_DIR.mkdir(parents=True)
TEMP_DIR.mkdir(parents=True)
(TEMP_DIR / "spill").mkdir(parents=True)

fs = HfFileSystem(token=TOKEN)
# This glob is intentionally non-recursive: it reads only the ten original
# Parquet files directly in tg/processed and excludes our output subfolder.
source_files = sorted(fs.glob(f"{SOURCE_PREFIX}/*.parquet"))
if not source_files:
    raise RuntimeError(f"No source Parquet files found in {SOURCE_PREFIX}")

print(f"Found {len(source_files)} source Parquet files.", flush=True)
source = ds.dataset(source_files, filesystem=fs, format="parquet")
required = {"account_id", "phone"}
missing = required.difference(source.schema.names)
if missing:
    raise RuntimeError(f"Missing required columns: {sorted(missing)}")

scanner = source.scanner(
    columns=["account_id", "phone"],
    batch_size=131072,
    use_threads=True,
)
con = duckdb.connect(str(TEMP_DIR / "build.duckdb"))
con.execute("SET memory_limit='5GB'")
con.execute(f"SET temp_directory='{(TEMP_DIR / 'spill').as_posix()}'")
con.execute("SET threads=2")
con.register("source_rows", scanner.to_reader())

# Hash partitioning guarantees every account_id maps to one shard.
# Sorting inside each shard makes Parquet row-group statistics useful.
sql = f"""
COPY (
    SELECT
        CAST(account_id AS VARCHAR) AS account_id,
        CAST(phone AS VARCHAR) AS phone,
        CAST(TRY_CAST(account_id AS UBIGINT) % {SHARD_COUNT} AS INTEGER) AS shard
    FROM source_rows
    WHERE account_id IS NOT NULL
      AND TRY_CAST(account_id AS UBIGINT) IS NOT NULL
    ORDER BY shard, account_id
)
TO '{(OUTPUT_DIR / "shards").as_posix()}'
(FORMAT PARQUET, PARTITION_BY (shard), ROW_GROUP_SIZE 4096, COMPRESSION ZSTD)
"""
print("Scanning, partitioning and sorting source rows. This may take a while.", flush=True)
con.execute(sql)
con.close()

files_by_shard = {}
uploads = []
for path in sorted((OUTPUT_DIR / "shards").rglob("*.parquet")):
    shard_dir = next(part for part in path.parts if part.startswith("shard="))
    shard = shard_dir.split("=", 1)[1]
    relative = path.relative_to(OUTPUT_DIR / "shards").as_posix()
    remote = f"{REMOTE_PREFIX}/shards/{relative}"
    files_by_shard.setdefault(shard, []).append(remote)
    uploads.append((str(path), remote))

if not uploads:
    raise RuntimeError("No index shards were produced.")

print(f"Built {len(uploads)} Parquet shard files across {len(files_by_shard)} buckets.", flush=True)
for offset in range(0, len(uploads), 40):
    batch = uploads[offset:offset + 40]
    batch_bucket_files(BUCKET_ID, add=batch)
    print(f"Uploaded {min(offset + len(batch), len(uploads))}/{len(uploads)} shard files.", flush=True)

manifest = {
    "version": 1,
    "shard_count": SHARD_COUNT,
    "files": files_by_shard,
}
manifest_path = OUTPUT_DIR / "manifest.json"
manifest_path.write_text(json.dumps(manifest, separators=(",", ":")), encoding="utf-8")
# Publish manifest last so the API never treats a partial index as ready.
batch_bucket_files(
    BUCKET_ID,
    add=[(str(manifest_path), f"{REMOTE_PREFIX}/manifest.json")],
)
print("Index build complete; manifest published.", flush=True)
