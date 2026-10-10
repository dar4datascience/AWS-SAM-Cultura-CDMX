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


def scrape_events(session, today, max_pages=None):
    """Fetch all listings + details. Returns (records, failed_count) ordered
    by (page_number, card_index)."""
    tasks = _fetch_listing_items(session, max_pages=max_pages)

    records = [None] * len(tasks)
    failed_count = 0

    def _fetch_detail(task):
        page_number, card_index, item = task
        info = fetch_event_info(
            session,
            item.get("event_id"),
            item.get("event_date_located"),
            item.get("event_name"),
        )
        return info

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_detail, task): i for i, task in enumerate(tasks)}
        for future in as_completed(futures):
            i = futures[future]
            page_number, card_index, item = tasks[i]
            try:
                info = future.result()
            except Exception as exc:  # noqa: BLE001 - per-item failures become failure records
                failed_count += 1
                _log_event(
                    "detail_fetch_failed",
                    page_number=page_number,
                    card_index=card_index,
                    event_id=item.get("event_id"),
                    error=str(exc),
                )
                records[i] = build_record(item, None, page_number, card_index, today)
            else:
                records[i] = build_record(item, info, page_number, card_index, today)

    records.sort(key=lambda r: (r["page_number"], r["card_index"]))
    return records, failed_count


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

    _log_event("scrape_started", snapshot_date=snapshot_date, max_pages=max_pages)

    today = mexico_today()
    session = make_session()
    records, failed_count = scrape_events(session, today, max_pages=max_pages)
    total = len(records)

    duration_ms = int((time.time() - started_at) * 1000)
    _emit_metric("ScrapeDurationMs", duration_ms, unit="Milliseconds")
    _emit_metric("ScrapeEventCount", total)
    _emit_metric("ScrapeFailedCount", failed_count)

    if total and failed_count / total > MAX_DETAIL_FAILURE_RATIO:
        raise ScrapeDegraded(
            f"{failed_count}/{total} detail fetches failed "
            f"(over the {MAX_DETAIL_FAILURE_RATIO:.0%} threshold); S3 write skipped"
        )

    s3_key = f"snapshot_date/{snapshot_date}/events.json"
    body = json.dumps(records, ensure_ascii=False)
    _s3().put_object(Bucket=bucket_name, Key=s3_key, Body=body.encode("utf-8"))

    _log_event(
        "scrape_completed",
        snapshot_date=snapshot_date,
        s3_key=s3_key,
        events_count=total,
        failed_count=failed_count,
        duration_ms=duration_ms,
    )

    return {
        "snapshot_date": snapshot_date,
        "s3_key": s3_key,
        "events_count": total,
        "failed_count": failed_count,
    }
