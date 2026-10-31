# India DB Indexer

Fast exact-match API for `account_id` using your Hugging Face Parquet dataset.

## Data source

The builder reads only the ten Parquet files directly inside:

`buckets/Chatpataprani/Hitek_ImCR_API-bucket/tg/processed/*.parquet`

It selects only `account_id` and `phone`. It does not modify, rename, or delete any source Parquet file.

## How it works

1. GitHub Actions reads the ten source Parquet files through `HfFileSystem`.
2. DuckDB scans the dataset in batches, assigns each numeric account ID to one of 1,024 hash shards, and sorts records by shard and account ID.
3. The generated Parquet index is uploaded under `tg/processed/account-id-index/` in the same Hugging Face bucket.
4. The manifest is uploaded last. The API considers the index ready only after that manifest exists.
5. The API computes the shard for an incoming ID, reads only that shard, filters by exact `account_id`, and returns the two requested fields.

The original source files remain read-only. Rebuilding replaces only files in the generated `account-id-index` output folder.

## Build the index

Add a Hugging Face **write** token as a GitHub Actions repository secret named `HF_TOKEN`. The token is used only by the workflow to read the source and write the generated index.

Then open **GitHub → Actions → Build account ID Parquet index → Run workflow**.

The workflow uses GitHub-hosted compute and may take a while to scan the full dataset. It does not run automatically on every push.

## API

After the index build finishes and the API is deployed:

- `GET /` — service status
- `GET /health` — index readiness
- `GET /search?account_id=123456789` — exact lookup
- `GET /docs` — interactive API documentation

Successful response:

```json
{
  "account_id": "123456789",
  "phone": "+910000000000"
}
```

The API returns only `account_id` and `phone`. It returns HTTP 404 when the ID is not found and HTTP 503 if the index is not ready.

## Vercel environment variables

Set these in the Vercel project:

- `HF_TOKEN` — Hugging Face read token for the bucket
- `HF_BUCKET` — optional; defaults to `buckets/Chatpataprani/Hitek_ImCR_API-bucket`
- `INDEX_SHARDS` — optional; must match the builder, default `1024`
- `CACHE_SIZE` — optional in-process lookup cache size, default `2048`

Deploy this repository to Vercel using the repository root. The FastAPI entry point is `api/index.py`.

## Performance notes

Hash partitioning prevents a request from scanning all ten source files. A cold lookup still needs to access the relevant shard on Hugging Face; warm repeated IDs can be served from the process cache. Actual end-to-end latency depends on Vercel cold starts, network distance, Hugging Face storage, and shard size. Measure it after the index is built; millisecond response times are not guaranteed.

## Development

```bash
pip install -r requirements.txt
uvicorn api.index:app --host 0.0.0.0 --port 8000
```
