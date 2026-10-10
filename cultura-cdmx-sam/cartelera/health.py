"""Site health check Lambda.

Verifies cartelera.cdmx.gob.mx is up and still serves the contract the
scraper depends on. Raises SiteUnavailable / SiteChanged so the state
machine can catch them and stop the pipeline with a notification.
"""

import json
import time
from datetime import datetime, timezone

from client import (
    SiteChanged,
    SiteUnavailable,
    fetch_event_info,
    fetch_listing_page,
    make_session,
)

HEALTH_ATTEMPTS = 2
HEALTH_RETRY_DELAY_SECONDS = 5


def _log_event(message, **fields):
    print(json.dumps({"message": message, **fields}, ensure_ascii=False))


def _sleep(seconds):
    time.sleep(seconds)


def _check_once(session) -> dict:
    event_data = fetch_listing_page(session, 1)

    try:
        pages_total = int(event_data.get("event_pages_total"))
    except (TypeError, ValueError):
        pages_total = 0
    items = event_data.get("event_items_list") or []
    if pages_total < 1 or not items:
        raise SiteUnavailable(
            f"listing page 1 returned pages_total={event_data.get('event_pages_total')!r} "
            f"with {len(items)} items"
        )

    first = items[0]
    info = fetch_event_info(
        session,
        first.get("event_id"),
        first.get("event_date_located"),
        first.get("event_name"),
    )
    if not info.get("event_name"):
        raise SiteChanged("event info parsed but has no event_name")

    try:
        items_total = int(event_data.get("event_items_total"))
    except (TypeError, ValueError):
        items_total = len(items)

    return {
        "healthy": True,
        "pages_total": pages_total,
        "items_total": items_total,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


def lambda_handler(event, context):
    _log_event("health_check_started")
    session = make_session()
    last_exc = None
    for attempt in range(1, HEALTH_ATTEMPTS + 1):
        try:
            result = _check_once(session)
        except (SiteUnavailable, SiteChanged) as exc:
            last_exc = exc
            _log_event(
                "health_check_attempt_failed",
                attempt=attempt,
                max_attempts=HEALTH_ATTEMPTS,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            if attempt < HEALTH_ATTEMPTS:
                _sleep(HEALTH_RETRY_DELAY_SECONDS)
        else:
            _log_event("health_check_ok", **result)
            return result
    _log_event(
        "health_check_failed",
        error_type=type(last_exc).__name__,
        error=str(last_exc),
    )
    raise last_exc
