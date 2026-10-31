#!/usr/bin/env python3
"""Build a sharded exact-phone index from a local synthetic SQLite database.

The source database is read-only. Every source column is preserved in row_data.
Run this on a machine with enough free disk for the source plus the generated index.
"""
import hashlib
import json
import os
import re
import sqlite3
import sys
from pathlib import Path

SOURCE_DB = Path(os.getenv("SOURCE_DB", "users_data.db"))
INDEX_DIR = Path(os.getenv("INDEX_DIR", "index"))
SHARD_COUNT = int(os.getenv("SHARD_COUNT", "256"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
PHONE_NAME = re.compile(r"(phone|mobile|msisdn|contact|cell)", re.I)


def normalize(value):
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    return digits if len(digits) >= 5 else ""


def shard_for(phone):
    digest = hashlib.sha256(phone.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % SHARD_COUNT


def quote_ident(value):
    return '"' + value.replace('"', '""') + '"'


def main():
    if os.getenv("DATASET_IS_SYNTHETIC") != "1":
        sys.exit("Refusing to index: set DATASET_IS_SYNTHETIC=1 only after verifying the dataset is synthetic.")
    if not SOURCE_DB.is_file():
        sys.exit(f"Source database not found: {SOURCE_DB}")
    if SHARD_COUNT < 1:
        sys.exit("SHARD_COUNT must be at least 1")

    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    source = sqlite3.connect(f"file:{SOURCE_DB.resolve()}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row

    shard_connections = {}
    indexed_rows = 0
    indexed_matches = 0
    schema = []

    def get_shard(phone):
        shard_id = shard_for(phone)
        if shard_id not in shard_connections:
            path = INDEX_DIR / f"part-{shard_id:03d}.sqlite"
            con = sqlite3.connect(path)
            con.execute("""
                CREATE TABLE IF NOT EXISTS records (
                    phone TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    row_data TEXT NOT NULL
                )
            """)
            con.execute("CREATE INDEX IF NOT EXISTS idx_records_phone ON records(phone)")
            shard_connections[shard_id] = con
        return shard_connections[shard_id]

    try:
        tables = source.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """).fetchall()

        for (table,) in tables:
            columns = source.execute(f"PRAGMA table_info({quote_ident(table)})").fetchall()
            phone_columns = [col[1] for col in columns if PHONE_NAME.search(col[1])]
            schema.append({"table": table, "columns": [col[1] for col in columns],
                           "phone_columns": phone_columns})
            if not phone_columns:
                print(f"Skipping {table}: no phone-like column name detected", flush=True)
                continue

            print(f"Indexing {table}: {len(phone_columns)} candidate phone column(s)", flush=True)
            cursor = source.execute(f"SELECT * FROM {quote_ident(table)}")
            names = [d[0] for d in cursor.description]

            while True:
                batch = cursor.fetchmany(BATCH_SIZE)
                if not batch:
                    break
                for row in batch:
                    indexed_rows += 1
                    record = {name: row[name] for name in names}
                    payload = json.dumps(record, ensure_ascii=False, default=str, separators=(",", ":"))
                    seen = set()
                    for column in phone_columns:
                        phone = normalize(row[column])
                        if not phone or phone in seen:
                            continue
                        seen.add(phone)
                        get_shard(phone).execute(
                            "INSERT INTO records(phone, table_name, row_data) VALUES (?, ?, ?)",
                            (phone, table, payload),
                        )
                        indexed_matches += 1

                if indexed_rows % (BATCH_SIZE * 10) == 0:
                    for con in shard_connections.values():
                        con.commit()
                    print(f"Scanned {indexed_rows:,} rows; indexed {indexed_matches:,} phone matches", flush=True)

        for con in shard_connections.values():
            con.commit()
            con.close()

        manifest = {
            "format_version": 1,
            "shard_count": SHARD_COUNT,
            "source_file": SOURCE_DB.name,
            "tables": schema,
            "rows_scanned": indexed_rows,
            "phone_matches_indexed": indexed_matches,
            "phone_normalization": "digits-only; strips leading India country code 91 or trunk 0",
        }
        (INDEX_DIR / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(json.dumps(manifest, indent=2), flush=True)
    finally:
        source.close()


if __name__ == "__main__":
    main()
