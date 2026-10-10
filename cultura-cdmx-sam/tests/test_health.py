import json

import pytest
import requests

import client
import health
from conftest import FIXTURES_DIR
from test_client import FakeResponse, FakeSession


@pytest.fixture(autouse=True)
def no_sleeps(monkeypatch):
    monkeypatch.setattr(client, "_sleep", lambda _s: None)
    monkeypatch.setattr(health, "_sleep", lambda _s: None)


def _listing_ok():
    payload = json.loads((FIXTURES_DIR / "listing_page1.json").read_text(encoding="utf-8"))
    return FakeResponse(status_code=200, json_data=payload)


def _detail_ok():
    html = (FIXTURES_DIR / "event_41272.html").read_text(encoding="utf-8")
    return FakeResponse(status_code=200, text=html)


def _run(monkeypatch, outcomes):
    session = FakeSession(outcomes)
    monkeypatch.setattr(health, "make_session", lambda: session)
    return health.lambda_handler({}, None), session


def test_health_listing_connection_error(monkeypatch):
    with pytest.raises(client.SiteUnavailable):
        _run(monkeypatch, [requests.ConnectionError("boom")])


def test_health_listing_bad_status(monkeypatch):
    bad = FakeResponse(status_code=200, json_data={"status": "Error", "data": {}})
    with pytest.raises(client.SiteUnavailable):
        _run(monkeypatch, [bad])


def test_health_detail_marker_missing(monkeypatch):
    no_marker = FakeResponse(status_code=200, text="<html><body>no marker</body></html>")
    outcomes = [_listing_ok(), no_marker, _listing_ok(), no_marker]
    with pytest.raises(client.SiteChanged):
        _run(monkeypatch, outcomes)


def test_health_ok(monkeypatch):
    result, _session = _run(monkeypatch, [_listing_ok(), _detail_ok()])
    assert result["healthy"] is True
    assert result["pages_total"] == 103
    assert result["items_total"] == 920
    assert result["checked_at"]
