import io
import json
from datetime import date

import pytest
from botocore.exceptions import ClientError

import client
import scrape

TODAY = date(2026, 10, 9)


class FakeS3:
    def __init__(self, cache_payload=None):
        self.puts = []
        self._cache_payload = cache_payload

    def put_object(self, **kwargs):
        self.puts.append(kwargs)

    def get_object(self, **kwargs):
        if self._cache_payload is not None:
            return {"Body": io.BytesIO(json.dumps(self._cache_payload).encode("utf-8"))}
        raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")


def _listing_items(count, page=1):
    return [
        {
            "event_id": str(1000 + page * 100 + i),
            "event_type": "Museos y Exposiciones",
            "event_name": f"Evento {page}-{i}",
            "event_venue": "Recinto",
            "event_date_located": "09/10/2026",
            "event_lat": "19.43",
            "event_lon": "-99.13",
        }
        for i in range(count)
    ]


def _event_info(item):
    return {
        "event_id": item["event_id"],
        "event_name": item["event_name"],
        "event_description": "<p>desc</p>",
        "event_duration": "01:00",
        "event_audience": "Todas",
        "event_accesibility": "Si",
        "event_entity": "Secretaría",
        "event_location": {"lat": "19.4", "lng": "-99.1", "address": "Calle"},
        "event_image": "https://img",
        "venue_name": item["event_venue"],
        "schedule": {"1": {"event_dates": ["09/10/2026", "10/10/2026"], "event_hours": ["12:00"]}},
        "costs": {"1": {"cost_name": "General", "cost_price": "Gratuito"}},
    }


def _cached_entry(event_id, fetched_at=None, name=None):
    """Fresh cache entry whose schedule contains the listing date 09/10/2026."""
    info = _event_info({"event_id": event_id, "event_name": name or f"Cached {event_id}",
                        "event_venue": "Recinto"})
    return {"fetched_at": fetched_at or TODAY.isoformat(), "info": info}


def _patch_scrape(monkeypatch, pages, fail_ids, cache_payload=None):
    """pages: dict page_number -> event_data dict. fail_ids: event_ids that raise.

    Returns (fake_s3, fetch_calls) where fetch_calls records event_ids fetched.
    """
    fetch_calls = []

    def fake_listing(_session, page_number):
        return pages[page_number]

    def fake_detail(_session, event_id, _date, _name):
        fetch_calls.append(str(event_id))
        if str(event_id) in fail_ids:
            raise client.SiteUnavailable("detail down")
        return _event_info({"event_id": event_id, "event_name": _name, "event_venue": "Recinto"})

    s3 = FakeS3(cache_payload=cache_payload)
    monkeypatch.setattr(scrape, "fetch_listing_page", fake_listing)
    monkeypatch.setattr(scrape, "fetch_event_info", fake_detail)
    monkeypatch.setattr(scrape, "s3_client", s3)
    monkeypatch.setattr(scrape, "make_session", lambda: object())
    monkeypatch.setattr(scrape, "mexico_today", lambda: TODAY)
    monkeypatch.setenv("BUCKET_NAME", "test-bucket")
    return s3, fetch_calls


def test_scrape_degraded_over_threshold(monkeypatch):
    pages = {1: {"event_pages_total": 1, "event_items_list": _listing_items(10)}}
    s3, _calls = _patch_scrape(monkeypatch, pages, fail_ids={"1100", "1101", "1102"})  # 30%
    with pytest.raises(client.ScrapeDegraded):
        scrape.lambda_handler({"snapshot_date": "20261009"}, None)
    # neither events.json nor the cache is written when the run degrades
    assert s3.puts == []


def test_scrape_writes_events_json_and_cache(monkeypatch):
    pages = {
        1: {"event_pages_total": 2, "event_items_list": _listing_items(3, page=1)},
        2: {"event_pages_total": 2, "event_items_list": _listing_items(3, page=2)},
    }
    s3, _calls = _patch_scrape(monkeypatch, pages, fail_ids={"1202"})  # 1/6 ~ 17% < 20%

    result = scrape.lambda_handler({"snapshot_date": "20261009"}, None)

    assert len(s3.puts) == 2
    put = s3.puts[0]
    assert put["Bucket"] == "test-bucket"
    assert put["Key"] == "snapshot_date/20261009/events.json"
    records = json.loads(put["Body"].decode("utf-8"))
    assert [(r["page_number"], r["card_index"]) for r in records] == [
        (1, 0), (1, 1), (1, 2), (2, 0), (2, 1), (2, 2)
    ]
    assert result["snapshot_date"] == "20261009"
    assert result["s3_key"] == "snapshot_date/20261009/events.json"
    assert result["events_count"] == 6
    assert result["failed_count"] == 1
    assert result["fetched"] == 5

    # the cache object is saved after events.json
    cache_put = s3.puts[1]
    assert cache_put["Key"] == "cache/event_info.json"
    cache_body = json.loads(cache_put["Body"].decode("utf-8"))
    assert cache_body["version"] == 1
    assert set(cache_body["entries"]) == {"1100", "1101", "1102", "1200", "1201"}


def test_scrape_accepts_nested_snapshot_date(monkeypatch):
    pages = {1: {"event_pages_total": 1, "event_items_list": _listing_items(1)}}
    _patch_scrape(monkeypatch, pages, fail_ids=set())
    result = scrape.lambda_handler(
        {"snapshot_date": {"snapshot_date": "20261009"}}, None
    )
    assert result["snapshot_date"] == "20261009"


def test_scrape_events_cache_hits_skip_fetch(monkeypatch):
    pages = {1: {"event_pages_total": 1, "event_items_list": _listing_items(3)}}
    _s3, fetch_calls = _patch_scrape(monkeypatch, pages, fail_ids=set())
    cache_entries = {
        "1100": _cached_entry("1100"),
        "1101": _cached_entry("1101"),
    }
    records, failed, new_entries, stats = scrape.scrape_events(
        object(), TODAY, cache_entries=cache_entries, ttl_days=3
    )
    assert sorted(new_entries.keys()) == ["1100", "1101", "1102"]
    assert fetch_calls == ["1102"]  # only the uncached id was fetched
    assert stats == {"fetched": 1, "cache_hits": 2, "stale_fallback": 0, "failed": 0}
    assert failed == 0
    assert len(records) == 3
    # reused entries keep their fetched_at; fresh fetches are stamped today
    assert new_entries["1100"]["fetched_at"] == cache_entries["1100"]["fetched_at"]
    assert new_entries["1102"]["fetched_at"] == TODAY.isoformat()
    # cached info is used in the record
    rec = next(r for r in records if r["event_id"] == "1100")
    assert rec["evento"] == "Cached 1100"


def test_scrape_events_stale_fallback_on_failure(monkeypatch):
    pages = {1: {"event_pages_total": 1, "event_items_list": _listing_items(2)}}
    # force refresh: cached schedule does not contain the listing date
    stale_entry = _cached_entry("1101")
    stale_entry["info"]["schedule"] = {"1": {"event_dates": ["01/11/2026"], "event_hours": ["19:00"]}}
    cache_entries = {"1101": stale_entry}
    _patch_scrape(monkeypatch, pages, fail_ids={"1101"})

    records, failed, new_entries, stats = scrape.scrape_events(
        object(), TODAY, cache_entries=cache_entries, ttl_days=3
    )
    assert stats["stale_fallback"] == 1
    assert stats["failed"] == 0
    assert failed == 0
    rec = next(r for r in records if r["event_id"] == "1101")
    assert rec["evento"] == "Cached 1101"  # record built from cached info
    assert new_entries["1101"]["fetched_at"] == stale_entry["fetched_at"]


def test_scrape_events_prunes_cache(monkeypatch):
    pages = {1: {"event_pages_total": 1, "event_items_list": _listing_items(1)}}
    _patch_scrape(monkeypatch, pages, fail_ids=set())
    cache_entries = {
        "1100": _cached_entry("1100"),
        "9999": _cached_entry("9999"),  # not in today's listing -> pruned
    }
    _records, _failed, new_entries, _stats = scrape.scrape_events(
        object(), TODAY, cache_entries=cache_entries, ttl_days=3
    )
    assert "9999" not in new_entries
    assert "1100" in new_entries


def test_scrape_events_fetches_each_id_once(monkeypatch):
    items = _listing_items(2)
    items.append(dict(items[0]))  # duplicate event_id "1100" listed twice
    pages = {1: {"event_pages_total": 1, "event_items_list": items}}
    _s3, fetch_calls = _patch_scrape(monkeypatch, pages, fail_ids=set())
    records, _f, _e, stats = scrape.scrape_events(object(), TODAY)
    assert fetch_calls.count("1100") == 1
    assert len(records) == 3
    assert stats["fetched"] == 2


def test_handler_loads_and_updates_cache(monkeypatch):
    pages = {1: {"event_pages_total": 1, "event_items_list": _listing_items(2)}}
    existing = {"version": 1, "entries": {"1100": _cached_entry("1100")}}
    s3, _calls = _patch_scrape(monkeypatch, pages, fail_ids=set(), cache_payload=existing)

    result = scrape.lambda_handler({"snapshot_date": "20261009"}, None)

    assert result["cache_hits"] == 1
    assert result["fetched"] == 1
    assert len(s3.puts) == 2
    cache_body = json.loads(s3.puts[1]["Body"].decode("utf-8"))
    assert set(cache_body["entries"]) == {"1100", "1101"}
