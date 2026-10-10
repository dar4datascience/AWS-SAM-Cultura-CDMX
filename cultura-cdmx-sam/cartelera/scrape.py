"""Cartelera scrape Lambda.

Fetches every listing page via the site's internal API, then every event's
embedded detail JSON, and writes a single events.json to S3 for the DuckDB
coalesce step. No browser involved.
"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

import cache as detail_cache
from client import (
    ScrapeDegraded,
    fetch_event_info,
    fetch_listing_page,
    make_session,
)
from parse import build_record, mexico_today

MAX_WORKERS = 6
MAX_DETAIL_FAILURE_RATIO = 0.2

s3_client = None


def _s3():
    global s3_client
    if s3_client is None:
        s3_client = boto3.client("s3")
    return s3_client


def _log_event(message, **fields):
    print(json.dumps({"message": message, **fields}, ensure_ascii=False))


def _emit_metric(metric_name, value, unit="Count", dimensions=None):
    dims = dimensions or {"Function": "CarteleraScrapeFunction"}
    metric_event = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": "CulturaScraper",
                    "Dimensions": [list(dims.keys())],
                    "Metrics": [{"Name": metric_name, "Unit": unit}],
                }
            ],
        },
        metric_name: value,
        **dims,
    }
    print(json.dumps(metric_event, ensure_ascii=False))


def _fetch_listing_items(session, max_pages=None):
    """Return [(page_number, card_index, listing_item), ...] for every page.

    Raises SiteUnavailable if any page still fails after client retries.
    """
    first_page = fetch_listing_page(session, 1)
    try:
        total_pages = int(first_page.get("event_pages_total") or 1)
    except (TypeError, ValueError):
        total_pages = 1
    if max_pages:
        total_pages = min(total_pages, max_pages)
    _log_event("listing_pages_detected", total_pages=total_pages)

    items_by_page = {1: first_page.get("event_items_list") or []}
    if total_pages > 1:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(fetch_listing_page, session, n): n
                for n in range(2, total_pages + 1)
            }
            for future in as_completed(futures):
                event_data = future.result()  # SiteUnavailable propagates
                items_by_page[futures[future]] = event_data.get("event_items_list") or []

    tasks = [
        (page_number, card_index, item)
        for page_number in sorted(items_by_page)
        for card_index, item in enumerate(items_by_page[page_number])
    ]
    _log_event(
        "listing_completed",
        pages_fetched=len(items_by_page),
        items_found=len(tasks),
    )
    return tasks


def scrape_events(session, today, max_pages=None, cache_entries=None, ttl_days=detail_cache.DEFAULT_TTL_DAYS):
    """Fetch all listings + details, reusing cached event_info payloads.

    Returns (records, failed_count, new_cache_entries, stats). Records are
    ordered by (page_number, card_index); new_cache_entries only contains
    event_ids present in today's listing (stale entries are pruned).
    """
    cache_entries = cache_entries or {}
    tasks = _fetch_listing_items(session, max_pages=max_pages)

    stats = {"fetched": 0, "cache_hits": 0, "stale_fallback": 0, "failed": 0}
    infos = {}          # event_id -> event_info (None = fetch failed, no cache)
    new_entries = {}    # event_id -> {"fetched_at", "info"} for the next cache
    to_fetch = {}       # event_id -> listing_item (first occurrence wins)

    for _page_number, _card_index, item in tasks:
        event_id = str(item.get("event_id") or "")
        if not event_id or event_id in to_fetch or event_id in infos:
            continue
        entry = cache_entries.get(event_id)
        listing_date = item.get("event_date_located") or ""
        if detail_cache.needs_refresh(entry, listing_date, today, ttl_days):
            to_fetch[event_id] = item
        else:
            infos[event_id] = entry["info"]
            new_entries[event_id] = entry  # keep original fetched_at
            stats["cache_hits"] += 1

    _log_event(
        "detail_fetch_plan",
        unique_ids=len(to_fetch) + stats["cache_hits"],
        to_fetch=len(to_fetch),
        cache_hits=stats["cache_hits"],
    )

    if to_fetch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {
                pool.submit(
                    fetch_event_info,
                    session,
                    event_id,
                    item.get("event_date_located"),
                    item.get("event_name"),
                ): event_id
                for event_id, item in to_fetch.items()
            }
            for future in as_completed(futures):
                event_id = futures[future]
                try:
                    info = future.result()
                except Exception as exc:  # noqa: BLE001 - fall back to cache or failure record
                    entry = cache_entries.get(event_id)
                    if entry and isinstance(entry.get("info"), dict):
                        infos[event_id] = entry["info"]
                        new_entries[event_id] = entry
                        stats["stale_fallback"] += 1
                        _log_event(
                            "detail_fetch_failed_using_cache",
                            event_id=event_id,
                            fetched_at=entry.get("fetched_at"),
                            error=str(exc),
                        )
                    else:
                        infos[event_id] = None
                        stats["failed"] += 1
                        _log_event(
                            "detail_fetch_failed",
                            event_id=event_id,
                            error=str(exc),
                        )
                else:
                    infos[event_id] = info
                    new_entries[event_id] = {
                        "fetched_at": today.isoformat(),
                        "info": info,
                    }
                    stats["fetched"] += 1

    records = []
    failed_count = 0
    for page_number, card_index, item in tasks:
        info = infos.get(str(item.get("event_id") or ""))
        if info is None:
            failed_count += 1
        records.append(build_record(item, info, page_number, card_index, today))

    records.sort(key=lambda r: (r["page_number"], r["card_index"]))
    return records, failed_count, new_entries, stats


def lambda_handler(event, context):
    started_at = time.time()

    snapshot_date = event.get("snapshot_date") if isinstance(event, dict) else None
    if isinstance(snapshot_date, dict):
        snapshot_date = snapshot_date.get("snapshot_date")
    if not snapshot_date:
        raise ValueError("Missing 'snapshot_date' in event")

    bucket_name = os.environ.get("BUCKET_NAME")
    if not bucket_name:
        raise RuntimeError("Environment variable BUCKET_NAME not set.")

    max_pages_env = os.environ.get("MAX_PAGES")
    max_pages = int(max_pages_env) if max_pages_env else None
    cache_key = os.environ.get("CACHE_KEY") or detail_cache.DEFAULT_CACHE_KEY
    ttl_days = int(os.environ.get("CACHE_TTL_DAYS") or detail_cache.DEFAULT_TTL_DAYS)

    _log_event("scrape_started", snapshot_date=snapshot_date, max_pages=max_pages)

    today = mexico_today()
    session = make_session()
    s3 = _s3()

    cache_entries = detail_cache.load_cache_s3(s3, bucket_name, cache_key)
    _log_event("cache_loaded", key=cache_key, entries=len(cache_entries))

    records, failed_count, new_cache_entries, stats = scrape_events(
        session, today, max_pages=max_pages, cache_entries=cache_entries,
        ttl_days=ttl_days,
    )
    total = len(records)

    duration_ms = int((time.time() - started_at) * 1000)
    _emit_metric("ScrapeDurationMs", duration_ms, unit="Milliseconds")
    _emit_metric("ScrapeEventCount", total)
    _emit_metric("ScrapeFailedCount", failed_count)
    _emit_metric("ScrapeDetailFetched", stats["fetched"])
    _emit_metric("ScrapeCacheHits", stats["cache_hits"])
    _emit_metric("ScrapeStaleFallback", stats["stale_fallback"])

    if total and failed_count / total > MAX_DETAIL_FAILURE_RATIO:
        raise ScrapeDegraded(
            f"{failed_count}/{total} detail fetches failed "
            f"(over the {MAX_DETAIL_FAILURE_RATIO:.0%} threshold); S3 write skipped"
        )

    s3_key = f"snapshot_date/{snapshot_date}/events.json"
    body = json.dumps(records, ensure_ascii=False)
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=body.encode("utf-8"))

    try:
        detail_cache.save_cache_s3(s3, bucket_name, cache_key, new_cache_entries)
    except Exception as exc:  # noqa: BLE001 - cache loss only costs a refetch next run
        _log_event("cache_save_failed", key=cache_key, error=str(exc))

    _log_event(
        "scrape_completed",
        snapshot_date=snapshot_date,
        s3_key=s3_key,
        events_count=total,
        failed_count=failed_count,
        duration_ms=duration_ms,
        **stats,
    )

    return {
        "snapshot_date": snapshot_date,
        "s3_key": s3_key,
        "events_count": total,
        "failed_count": failed_count,
        **stats,
    }
