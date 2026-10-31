#!/usr/bin/env python3
"""Resumable, row-batched index builder for a verified synthetic SQLite DB.

Never split a SQLite database into byte ranges. This script pages through rowid
ranges, commits output shards, then atomically checkpoints the last committed
rowid for each table. Re-running the command resumes safely.
"""
import hashlib
import json
import os
import re
import sqlite3
import sys
import tempfile
from pathlib import Path

SOURCE_DB = Path(os.getenv("SOURCE_DB", "users_data.db"))
INDEX_DIR = Path(os.getenv("INDEX_DIR", "index"))
STATE_FILE = Path(os.getenv("STATE_FILE", str(INDEX_DIR / "progress.json")))
SHARD_COUNT = int(os.getenv("SHARD_COUNT", "256"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))
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


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix="progress-", suffix=".tmp", dir=STATE_FILE.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, STATE_FILE)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def load_state():
    if not STATE_FILE.exists():
        return {"format_version": 2, "tables": {}, "rows_scanned": 0, "phone_matches_indexed": 0}
    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    if state.get("format_version") != 2:
        raise RuntimeError("Unsupported progress file. Move index/progress.json aside to start a fresh build.")
    return state


def main():
    if os.getenv("DATASET_IS_SYNTHETIC") != "1":
        sys.exit("Refusing to index: set DATASET_IS_SYNTHETIC=1 only after verifying the dataset is synthetic.")
    if not SOURCE_DB.is_file():
        sys.exit(f"Source database not found: {SOURCE_DB}")
    if SHARD_COUNT < 1 or BATCH_SIZE < 1:
        sys.exit("SHARD_COUNT and BATCH_SIZE must both be at least 1.")

    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    state = load_state()
    source = sqlite3.connect(f"file:{SOURCE_DB.resolve()}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    shard_connections = {}
    schema = []

    def get_shard(phone):
        shard_id = shard_for(phone)
        if shard_id not in shard_connections:
            path = INDEX_DIR / f"part-{shard_id:03d}.sqlite"
            con = sqlite3.connect(path)
            existing = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='records'"
            ).fetchone()
            if existing:
                columns = {row[1] for row in con.execute("PRAGMA table_info(records)")}
                if "source_id" not in columns:
                    con.close()
                    raise RuntimeError(
                        f"{path} uses the old index format. Move/delete the old index directory "
                        "before starting this resumable build."
                    )
            con.execute("""
                CREATE TABLE IF NOT EXISTS records (
                    phone TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    row_data TEXT NOT NULL,
                    UNIQUE(phone, table_name, source_id)
                )
            """)
            con.execute("CREATE INDEX IF NOT EXISTS idx_records_phone ON records(phone)")
            shard_connections[shard_id] = con
        return shard_connections[shard_id]

    try:
        tables = source.execute("""
            SELECT name, sql FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """).fetchall()

        for table_row in tables:
            table, create_sql = table_row["name"], table_row["sql"] or ""
            columns = source.execute(f"PRAGMA table_info({quote_ident(table)})").fetchall()
            phone_columns = [col["name"] for col in columns if PHONE_NAME.search(col["name"])]
            schema.append({
                "table": table,
                "columns": [col["name"] for col in columns],
                "phone_columns": phone_columns,
            })
            if not phone_columns:
                print(f"Skipping {table}: no phone-like column name detected", flush=True)
                continue
            if "WITHOUT ROWID" in create_sql.upper():
                raise RuntimeError(
                    f"Table {table!r} is WITHOUT ROWID. This version safely checkpoints rowid tables; "
                    "it will stop rather than risk an incomplete or duplicate index."
                )

            table_state = state["tables"].setdefault(table, {"last_rowid": 0, "complete": False})
            if table_state.get("complete"):
                print(f"Already complete: {table}", flush=True)
                continue

            last_rowid = int(table_state.get("last_rowid", 0))
            names = [col["name"] for col in columns]
            select_sql = (
                f"SELECT rowid AS __indexer_rowid__, * FROM {quote_ident(table)} "
                "WHERE rowid > ? ORDER BY rowid LIMIT ?"
            )
            print(
                f"Resuming {table} after rowid {last_rowid:,} "
                f"(batch size {BATCH_SIZE:,})",
                flush=True,
            )

            while True:
                batch = source.execute(select_sql, (last_rowid, BATCH_SIZE)).fetchall()
                if not batch:
                    break

                batch_matches = 0
                for row in batch:
                    rowid = int(row["__indexer_rowid__"])
                    record = {name: row[name] for name in names}
                    payload = json.dumps(
                        record, ensure_ascii=False, default=str, separators=(",", ":")
                    )
                    seen = set()
                    for column in phone_columns:
                        phone = normalize(row[column])
                        if not phone or phone in seen:
                            continue
                        seen.add(phone)
                        source_id = str(rowid)
                        get_shard(phone).execute(
                            """INSERT OR IGNORE INTO records
                               (phone, table_name, source_id, row_data)
                               VALUES (?, ?, ?, ?)""",
                            (phone, table, source_id, payload),
                        )
                        batch_matches += 1

                # Commit every touched shard before advancing the checkpoint.
                for con in shard_connections.values():
                    con.commit()
                last_rowid = int(batch[-1]["__indexer_rowid__"])
                table_state["last_rowid"] = last_rowid
                state["rows_scanned"] += len(batch)
                state["phone_matches_indexed"] += batch_matches
                save_state(state)
                print(
                    f"{table}: checkpoint rowid {last_rowid:,}; "
                    f"total rows scanned {state['rows_scanned']:,}; "
                    f"phone entries {state['phone_matches_indexed']:,}",
                    flush=True,
                )

            table_state["complete"] = True
            save_state(state)
            print(f"Completed table: {table}", flush=True)

        for con in shard_connections.values():
            con.commit()

        manifest = {
            "format_version": 2,
            "shard_count": SHARD_COUNT,
            "source_file": SOURCE_DB.name,
            "tables": schema,
            "rows_scanned": state["rows_scanned"],
            "phone_matches_indexed": state["phone_matches_indexed"],
            "complete": all(
                item.get("complete", False)
                for item in state["tables"].values()
            ),
            "progress_file": STATE_FILE.name,
            "phone_normalization": "digits-only; strips leading India country code 91 or trunk 0",
        }
        (INDEX_DIR / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(json.dumps(manifest, indent=2), flush=True)
    finally:
        for con in shard_connections.values():
            try:
                con.commit()
                con.close()
            except sqlite3.Error:
                pass
        source.close()


if __name__ == "__main__":
    main()
