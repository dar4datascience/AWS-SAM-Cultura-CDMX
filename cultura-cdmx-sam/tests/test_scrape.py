import json

import pytest

import client
import scrape


class FakeS3:
    def __init__(self):
        self.puts = []

    def put_object(self, **kwargs):
        self.puts.append(kwargs)


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
        "schedule": {"1": {"event_dates": ["10/10/2026"], "event_hours": ["12:00"]}},
        "costs": {"1": {"cost_name": "General", "cost_price": "Gratuito"}},
    }


def _patch_scrape(monkeypatch, pages, fail_ids):
    """pages: dict page_number -> event_data dict. fail_ids: event_ids that raise."""

    def fake_listing(_session, page_number):
        return pages[page_number]

    def fake_detail(_session, event_id, _date, _name):
        if str(event_id) in fail_ids:
            raise client.SiteUnavailable("detail down")
        return _event_info({"event_id": event_id, "event_name": _name, "event_venue": "Recinto"})

    s3 = FakeS3()
    monkeypatch.setattr(scrape, "fetch_listing_page", fake_listing)
    monkeypatch.setattr(scrape, "fetch_event_info", fake_detail)
    monkeypatch.setattr(scrape, "s3_client", s3)
    monkeypatch.setattr(scrape, "make_session", lambda: object())
    monkeypatch.setenv("BUCKET_NAME", "test-bucket")
    return s3


def test_scrape_degraded_over_threshold(monkeypatch):
    pages = {1: {"event_pages_total": 1, "event_items_list": _listing_items(10)}}
    s3 = _patch_scrape(monkeypatch, pages, fail_ids={"1100", "1101", "1102"})  # 30%
    with pytest.raises(client.ScrapeDegraded):
        scrape.lambda_handler({"snapshot_date": "20261009"}, None)
    assert s3.puts == []


def test_scrape_writes_events_json(monkeypatch):
    pages = {
        1: {"event_pages_total": 2, "event_items_list": _listing_items(3, page=1)},
        2: {"event_pages_total": 2, "event_items_list": _listing_items(3, page=2)},
    }
    s3 = _patch_scrape(monkeypatch, pages, fail_ids={"1202"})  # 1/6 ~ 17% < 20%

    result = scrape.lambda_handler({"snapshot_date": "20261009"}, None)

    assert len(s3.puts) == 1
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


def test_scrape_accepts_nested_snapshot_date(monkeypatch):
    pages = {1: {"event_pages_total": 1, "event_items_list": _listing_items(1)}}
    _patch_scrape(monkeypatch, pages, fail_ids=set())
    result = scrape.lambda_handler(
        {"snapshot_date": {"snapshot_date": "20261009"}}, None
    )
    assert result["snapshot_date"] == "20261009"
