"""HTTP client for cartelera.cdmx.gob.mx.

The site has a server-rendered frontend backed by a small internal JSON API
for the listing pages. Each event detail page is a plain GET that embeds a
`var cdmx_billboard_event_info = {...};` JSON payload — no browser needed.
"""

import json
import re
import time

import requests

BASE_URL = "https://cartelera.cdmx.gob.mx"
SEARCH_API_URL = f"{BASE_URL}/api/v1/internal/events/search_builder"
USER_AGENT = (
    "Mozilla/5.0 (compatible; CulturaCDMXBot/1.0; "
    "+https://github.com/dar4datascience/AWS-SAM-Cultura-CDMX)"
)
REQUEST_TIMEOUT_SECONDS = 20
MAX_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = (1, 2, 4)

_EVENT_INFO_RE = re.compile(r"var\s+cdmx_billboard_event_info\s*=\s*(\{.*\})\s*;")

_SLUG_DISALLOWED = re.compile(r"[^a-z0-9 \-]")
_SLUG_WHITESPACE = re.compile(r"\s+")
_SLUG_DASHES = re.compile(r"-{2,}")


class SiteUnavailable(Exception):
    """cartelera.cdmx.gob.mx did not respond or returned unusable responses."""


class SiteChanged(Exception):
    """The site responded, but its markup/contract changed and parsing failed."""


class ScrapeDegraded(Exception):
    """Scraping ran, but too many items failed for the output to be trusted."""


def _sleep(seconds):
    time.sleep(seconds)


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def request_with_retry(session, method, url, **kwargs) -> requests.Response:
    """Issue a request with up to MAX_ATTEMPTS tries (backoff 1s/2s/4s).

    Retries on connection errors, timeouts, HTTP 5xx and HTTP 429.
    Raises SiteUnavailable after the last attempt fails.
    """
    kwargs.setdefault("timeout", REQUEST_TIMEOUT_SECONDS)
    last_error = "unknown error"
    for attempt in range(MAX_ATTEMPTS):
        try:
            response = session.request(method, url, **kwargs)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_error = f"{method} {url} failed: {exc}"
        else:
            if response.status_code == 429 or response.status_code >= 500:
                last_error = f"{method} {url} returned HTTP {response.status_code}"
            else:
                return response
        if attempt < MAX_ATTEMPTS - 1:
            _sleep(RETRY_BACKOFF_SECONDS[attempt])
    raise SiteUnavailable(last_error)


def fetch_listing_page(session, page_number: int) -> dict:
    """Fetch one listing page; returns the response's `event_data` dict."""
    response = request_with_retry(
        session,
        "POST",
        SEARCH_API_URL,
        files={"by_event_page": (None, str(page_number))},
    )
    if response.status_code != 200:
        raise SiteUnavailable(
            f"listing page {page_number} returned HTTP {response.status_code}"
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise SiteUnavailable(
            f"listing page {page_number} returned invalid JSON"
        ) from exc
    if payload.get("status") != "OK":
        raise SiteUnavailable(
            f"listing page {page_number} returned status {payload.get('status')!r}"
        )
    try:
        return payload["data"]["event_data"]
    except (KeyError, TypeError) as exc:
        raise SiteUnavailable(
            f"listing page {page_number} response is missing data.event_data"
        ) from exc


def slugify(name) -> str:
    """Reproduce the site's slug rules.

    Lowercase, every char outside [a-z0-9 -] is dropped (accents are NOT
    transliterated: "México" -> "mxico"), whitespace runs become '-',
    repeated '-' collapse, edges are trimmed. Falls back to "evento" so the
    slug is never empty (the site 404s on an empty slug segment).
    """
    text = (name or "").lower()
    text = _SLUG_DISALLOWED.sub("", text)
    text = _SLUG_WHITESPACE.sub("-", text)
    text = _SLUG_DASHES.sub("-", text).strip("-")
    return text or "evento"


def detail_url(event_id, event_date: str, name) -> str:
    """Build a detail URL. `event_date` accepts DD/MM/YYYY or DD-MM-YYYY."""
    date_fmt = (event_date or "").replace("/", "-")
    return f"{BASE_URL}/{event_id}/{date_fmt}/{slugify(name)}"


def fetch_event_info(session, event_id, event_date: str, name) -> dict:
    """GET a detail page and return its embedded event info JSON."""
    url = detail_url(event_id, event_date, name)
    response = request_with_retry(session, "GET", url)
    if response.status_code != 200:
        raise SiteUnavailable(f"detail page {url} returned HTTP {response.status_code}")
    match = _EVENT_INFO_RE.search(response.text)
    if not match:
        raise SiteChanged(f"detail page {url} has no cdmx_billboard_event_info marker")
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise SiteChanged(
            f"detail page {url} event info is not valid JSON: {exc}"
        ) from exc
