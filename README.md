# India DB Indexer

A resumable, row-batched exact-phone indexer for a **verified synthetic SQLite dataset**. It detects phone-like column names, preserves every column from each matching row, and writes exact-match records into hash-partitioned SQLite shards.

## Important storage and access requirement

The source bucket is reported as approximately 438 GB. Batch processing limits memory use and lets the job resume after interruption, but it does **not** remove the need for the machine to access the complete SQLite database. A standard GitHub-hosted runner does not have enough disk to download and index a database of this size.

Do not split a SQLite `.db` file into arbitrary byte chunks. SQLite tables must be read through SQLite itself. The current builder pages through rowid ranges. Tables declared `WITHOUT ROWID` are detected and stop the build safely; they need a primary-key pagination implementation before indexing.

## Build or resume

Make the source database available locally as `users_data.db`, then:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
DATASET_IS_SYNTHETIC=1 SOURCE_DB=users_data.db INDEX_DIR=index BATCH_SIZE=2000 python scripts/build_index.py
```

The script:
- Opens the source database read-only.
- Reads at most `BATCH_SIZE` rows at a time (default 2,000).
- Detects columns named like phone, mobile, msisdn, contact, or cell.
- Preserves all source columns in each matching row's JSON payload.
- Writes 256 hash-partitioned SQLite files under `index/`.
- Commits shard writes before atomically saving `index/progress.json`.
- Resumes from each table's last committed rowid when rerun.
- Uses a unique source-row identity so a crash between shard commit and checkpoint does not duplicate entries.

Keep both the `index/` directory and `index/progress.json` between runs. Do not delete the progress file if you want to resume. To start a completely fresh build, move the entire old `index/` directory aside first. The new index format is not compatible with shards created by the earlier non-resumable script.

If the phone column uses a different name, update `PHONE_NAME` in `scripts/build_index.py`.

## Run the API

Keep the generated `index/` directory available to the API server:

```bash
export API_KEY='replace-with-a-long-random-secret'
export INDEX_DIR=index
uvicorn app:app --host 0.0.0.0 --port 8000
```

- `GET /health` — readiness check.
- `GET /search?phone=9876543210` — exact phone lookup; requires header `X-API-Key: <API_KEY>`.

The API returns the full indexed source row for each matching synthetic record, up to 1,000 results per request. Keep the API authenticated.

## Current limitations

- This is exact-match search, not fuzzy search.
- The source database is never modified.
- Index files may be large because complete matching rows are preserved.
- The current builder supports ordinary rowid tables; it intentionally stops on `WITHOUT ROWID` tables rather than silently skipping them.
- This repository currently contains the indexer and API scaffolding; it does not yet download the bucket, upload generated shards, or run the full indexing job. Those steps require a verified bucket access/upload method and storage with enough capacity.
- Test with a small synthetic database first and compare results against the source before attempting the full dataset.
