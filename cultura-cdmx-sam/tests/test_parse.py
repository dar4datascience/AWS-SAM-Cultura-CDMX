import json
from datetime import date

import client
import parse
from conftest import FIXTURES_DIR
from test_client import FakeResponse, FakeSession


def _fixture_event_info():
    html = (FIXTURES_DIR / "event_41272.html").read_text(encoding="utf-8")
    session = FakeSession([FakeResponse(status_code=200, text=html)])
    return client.fetch_event_info(session, "41272", "09/10/2026", "x")


def _fixture_listing_item(index=0):
    payload = json.loads((FIXTURES_DIR / "listing_page1.json").read_text(encoding="utf-8"))
    return payload["data"]["event_data"]["event_items_list"][index]


def test_schedule_picks_earliest_future_date():
    schedule = {"1": {"event_dates": ["09/10/2026", "15/10/2026"], "event_hours": ["11:00"]}}
    assert parse.build_schedule(schedule, date(2026, 10, 10)) == {
        "date": "Jueves, 15 De Octubre De 2026",
        "hour": "11:00",
    }
    assert parse.build_schedule(schedule, date(2026, 10, 9)) == {
        "date": "Viernes, 9 De Octubre De 2026",
        "hour": "11:00",
    }


def test_schedule_empty_dates():
    assert parse.build_schedule({"1": {"event_dates": [], "event_hours": ["11:00"]}}, date(2026, 10, 9)) == {
        "date": None,
        "hour": None,
    }
    assert parse.build_schedule(None, date(2026, 10, 9)) == {"date": None, "hour": None}


def test_schedule_multiple_hours_joined():
    schedule = {"1": {"event_dates": ["10/10/2026"], "event_hours": ["11:00", "17:00"]}}
    result = parse.build_schedule(schedule, date(2026, 10, 9))
    assert result["hour"] == "11:00 17:00"


def test_schedule_spanish_weekday_accents():
    wednesday = {"1": {"event_dates": ["14/10/2026"], "event_hours": ["11:00"]}}
    assert parse.build_schedule(wednesday, date(2026, 10, 9))["date"] == "Miércoles, 14 De Octubre De 2026"
    saturday = {"1": {"event_dates": ["17/10/2026"], "event_hours": ["11:00"]}}
    assert parse.build_schedule(saturday, date(2026, 10, 9))["date"] == "Sábado, 17 De Octubre De 2026"


def test_build_record_from_fixture():
    item = _fixture_listing_item(0)
    info = _fixture_event_info()
    record = parse.build_record(item, info, page_number=1, card_index=0, today=date(2026, 10, 9))

    # legacy columns: present with expected types
    assert record["page_number"] == 1 and isinstance(record["page_number"], int)
    assert record["card_index"] == 0 and isinstance(record["card_index"], int)
    assert record["detail_url"].startswith("https://cartelera.cdmx.gob.mx/41272/09-10-2026/")
    assert isinstance(record["description"], list) and record["description"]
    assert all(isinstance(d, str) for d in record["description"])
    assert isinstance(record["info"], list) and record["info"]
    assert record["info"][0].startswith("Duración:")
    assert record["info"][-1].startswith("Evento publicado por:")
    assert record["schedule"] == {"date": "Jueves, 15 De Octubre De 2026", "hour": "11:00"}
    assert record["location"] == info["event_location"]["address"]
    assert record["banner_url"] == info["event_image"]
    assert record["evento"] == info["event_name"]
    assert record["recinto"] == info["venue_name"]

    # new columns
    assert record["event_id"] == "41272"
    assert record["event_type"] == "Museos y Exposiciones"
    assert record["fecha_iso"] == "2026-10-09"
    assert record["lat"] == float(info["event_location"]["lat"]) and record["lon"] == float(info["event_location"]["lng"])
    assert record["venue_id"] == str(info["venue_id_id"])
    assert record["event_url"] == info["event_url"]
    assert record["costs"] == ["Entrada al museo: $ 50"]
    assert record["all_dates"] == ["2026-10-15", "2026-10-22", "2026-10-29"]


def test_build_record_failure_shape():
    item = _fixture_listing_item(1)
    record = parse.build_record(item, None, page_number=2, card_index=3, today=date(2026, 10, 9))

    assert record["page_number"] == 2 and record["card_index"] == 3
    assert record["evento"] == item["event_name"]
    assert record["recinto"] == item["event_venue"]
    assert record["detail_url"].startswith("https://cartelera.cdmx.gob.mx/38630/01-11-2026/")
    assert record["event_id"] == "38630"
    assert record["fecha_iso"] == "2026-11-01"
    assert record["lat"] == 19.4377 and record["lon"] == -99.1467
    assert record["event_type"] == item["event_type"]

    for key in ("description", "info", "schedule", "location", "banner_url",
                "venue_id", "event_url", "costs", "all_dates"):
        assert record[key] is None, key
