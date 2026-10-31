# India DB Indexer

Phone-number index for a **verified synthetic SQLite dataset**. The indexer detects phone-like column names, preserves every column from each matching row, and writes exact-match records into hash-partitioned SQLite shards.

## Important storage requirement

The source bucket is reported as approximately 438 GB. A standard GitHub-hosted runner does not have enough disk to download and index a database of that size. Run the indexing script on a machine/runner with the full source database available and sufficient additional disk for the generated index. The GitHub Actions workflow in this repository only validates the code; it does not download the database or run the 438 GB indexing job.

## Build the index

Make the synthetic SQLite database available locally as `users_data.db`, then:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
DATASET_IS_SYNTHETIC=1 SOURCE_DB=users_data.db INDEX_DIR=index python scripts/build_index.py
```

The script:
- Opens the source database read-only.
- Detects columns named like phone, mobile, msisdn, contact, or cell.
- Preserves all columns from each matching row.
- Writes 256 hash-partitioned index files under `index/`.
- Writes `index/manifest.json` with detected schema and build counts.

If your phone column uses a different name, update `PHONE_NAME` in `scripts/build_index.py` before running.

## Run the API

Keep the generated `index/` directory available to the API server:

```bash
export API_KEY='replace-with-a-long-random-secret'
export INDEX_DIR=index
uvicorn app:app --host 0.0.0.0 --port 8000
```

- `GET /health` — readiness check.
- `GET /search?phone=9876543210` — exact phone lookup; requires header `X-API-Key: <API_KEY>`.

The API returns all indexed source columns for each matching synthetic record, up to 1,000 matches per request. Keep the API authenticated and do not load real people's personal records into a public lookup service.

## Notes

- This is an exact-match index, not fuzzy search.
- The source DB is never modified.
- The index can be large because it stores the complete matching row payloads.
- Test on a small synthetic sample first and compare returned rows with the source database before processing the full dataset.
