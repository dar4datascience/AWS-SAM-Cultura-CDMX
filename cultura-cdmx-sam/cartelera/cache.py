"""Cache of event_info detail payloads (S3 for the Lambda, local file for runs).

Format: {"version": 1, "entries": {"<event_id>": {"fetched_at": "YYYY-MM-DD",
"info": {...event_info...}}}}

An entry is refreshed when it is missing, older than the TTL, or when the
listing's event_date_located is no longer among the cached schedule dates
(the site keeps a fresh copy only while the advertised date still matches).
"""

import json
from datetime import datetime
from pathlib import Path

from botocore.exceptions import ClientError

CACHE_VERSION = 1
DEFAULT_CACHE_KEY = "cache/event_info.json"
DEFAULT_TTL_DAYS = 3


def _log_event(message, **fields):
    print(json.dumps({"message": message, **fields}, ensure_ascii=False))


def _entries_from_payload(payload, source) -> dict:
    if (
        not isinstance(payload, dict)
        or payload.get("version") != CACHE_VERSION
        or not isinstance(payload.get("entries"), dict)
    ):
        _log_event("cache_load_bad_payload", source=source)
        return {}
    return payload["entries"]


def load_cache_s3(s3, bucket, key) -> dict:
    """Return the cached entries dict; {} on missing key or unusable payload."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        payload = json.loads(obj["Body"].read())
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NoSuchBucket"):
            return {}
        _log_event("cache_load_failed", key=key, error=str(exc))
        return {}
    except (ValueError, KeyError, TypeError) as exc:
        _log_event("cache_load_bad_payload", key=key, error=str(exc))
        return {}
    return _entries_from_payload(payload, key)


def save_cache_s3(s3, bucket, key, entries) -> None:
    body = json.dumps({"version": CACHE_VERSION, "entries": entries}, ensure_ascii=False)
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


def load_cache_file(path) -> dict:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (ValueError, OSError) as exc:
        _log_event("cache_load_bad_payload", path=str(path), error=str(exc))
        return {}
    return _entries_from_payload(payload, str(path))


def save_cache_file(path, entries) -> None:
    body = json.dumps({"version": CACHE_VERSION, "entries": entries}, ensure_ascii=False)
    Path(path).write_text(body, encoding="utf-8")


def needs_refresh(entry, listing_date_ddmmyyyy, today, ttl_days) -> bool:
    """Whether the cached entry must be re-fetched.

    True when the entry is missing, its fetched_at is unparseable or at least
    ttl_days old, or when the listing's located date no longer appears in the
    cached schedule (the event's dates changed upstream).
    """
    if not isinstance(entry, dict):
        return True
    try:
        fetched_day = datetime.strptime(str(entry.get("fetched_at")), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return True
    if (today - fetched_day).days >= ttl_days:
        return True

    info = entry.get("info") or {}
    schedule = info.get("schedule") or {}
    cached_dates = set()
    for sched_entry in schedule.values():
        if isinstance(sched_entry, dict):
            cached_dates.update(sched_entry.get("event_dates") or [])
    return listing_date_ddmmyyyy not in cached_dates
