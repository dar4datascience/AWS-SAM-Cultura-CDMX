#!/usr/bin/env python3
"""Run the cartelera scrape pipeline locally, without AWS.

Reuses the same client/parse/scrape code the Lambda uses, writes the JSON
array the Lambda would have uploaded, then coalesces it into a Parquet file
with the same DuckDB COPY the duckdb_handler uses.

Usage:
    python scripts/run_scraper_local.py --out /tmp/cartelera_test.json [--max-pages 3]

Outputs <out> (JSON) and <out with .parquet suffix> next to it, then prints
row count, schema, and a couple of sample records.
"""

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CARTELERA_DIR = REPO_ROOT / "cultura-cdmx-sam" / "cartelera"
sys.path.insert(0, str(CARTELERA_DIR))
sys.path.insert(0, str(REPO_ROOT / "cultura-cdmx-sam" / "duckdb_handler"))

import app as duckdb_handler  # noqa: E402
import cache as detail_cache  # noqa: E402
import client  # noqa: E402
import parse  # noqa: E402
import scrape  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="output JSON path")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="limit listing pages fetched (default: all pages)",
    )
    parser.add_argument(
        "--cache",
        default=None,
        help="optional local JSON cache file for event_info payloads",
    )
    parser.add_argument(
        "--cache-ttl-days",
        type=int,
        default=detail_cache.DEFAULT_TTL_DAYS,
        help="days a cached detail payload stays fresh (default: 3)",
    )
    args = parser.parse_args()

    out_path = Path(args.out).expanduser().resolve()
    parquet_path = out_path.with_suffix(".parquet")

    session = client.make_session()
    today = parse.mexico_today()

    cache_entries = {}
    if args.cache:
        cache_entries = detail_cache.load_cache_file(args.cache)
        print(f"Cache: {args.cache}  ({len(cache_entries)} entries loaded)")

    started_at = time.time()
    records, failed_count, new_cache_entries, stats = scrape.scrape_events(
        session, today, max_pages=args.max_pages,
        cache_entries=cache_entries, ttl_days=args.cache_ttl_days,
    )
    elapsed = time.time() - started_at

    out_path.write_text(
        json.dumps(records, ensure_ascii=False), encoding="utf-8"
    )

    if args.cache:
        detail_cache.save_cache_file(args.cache, new_cache_entries)
        print(f"Cache saved: {args.cache}  ({len(new_cache_entries)} entries)")

    import duckdb

    con = duckdb.connect(database=":memory:")
    con.execute(duckdb_handler.events_copy_sql(str(out_path), str(parquet_path)))

    row_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
    ).fetchone()[0]
    schema = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
    ).fetchall()

    print(f"\nRecords: {len(records)}  failed: {failed_count}  wall time: {elapsed:.1f}s")
    print(
        f"Stats: fetched={stats['fetched']}  cache_hits={stats['cache_hits']}  "
        f"stale_fallback={stats['stale_fallback']}  failed={stats['failed']}"
    )
    print(f"JSON:    {out_path}")
    print(f"Parquet: {parquet_path}  rows: {row_count}")
    print("\nSchema:")
    for col_name, col_type, *_ in schema:
        print(f"  {col_name}: {col_type}")
    print("\nSample records:")
    for record in records[:2]:
        print(json.dumps(record, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
