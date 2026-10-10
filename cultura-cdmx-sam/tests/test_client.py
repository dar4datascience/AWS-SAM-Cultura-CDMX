import json

import client
from conftest import FIXTURES_DIR


class FakeResponse:
    def __init__(self, status_code=200, text="", json_data=None):
        self.status_code = status_code
        self.text = text
        self._json_data = json_data

    def json(self):
        if self._json_data is None:
            raise ValueError("no json")
        return self._json_data


class FakeSession:
    """Returns queued outcomes (FakeResponse) or raises queued Exceptions.

    The last outcome repeats indefinitely so retry loops never run dry.
    """

    def __init__(self, outcomes):
        if not outcomes:
            raise ValueError("outcomes must not be empty")
        self._outcomes = list(outcomes)
        self.requests = []

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        outcome = self._outcomes.pop(0) if len(self._outcomes) > 1 else self._outcomes[0]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_event_info_extraction_from_fixture():
    html = (FIXTURES_DIR / "event_41272.html").read_text(encoding="utf-8")
    session = FakeSession([FakeResponse(status_code=200, text=html)])
    info = client.fetch_event_info(session, "41272", "09/10/2026", "x")
    assert isinstance(info, dict)
    assert info["event_name"]
    assert str(info["event_id"]) == "41272"


def test_slugify():
    assert (
        client.slugify("Universidad de México +475 Años de concebir futuros.")
        == "universidad-de-mxico-475-aos-de-concebir-futuros"
    )
    assert (
        client.slugify("XXVI Feria Internacional del Libro del Zócalo 2026")
        == "xxvi-feria-internacional-del-libro-del-zcalo-2026"
    )
    assert client.slugify("!!!") == "evento"
    assert client.slugify("") == "evento"


def test_detail_url():
    url = client.detail_url("41272", "09/10/2026", "Akira Yamaoka")
    assert url == "https://cartelera.cdmx.gob.mx/41272/09-10-2026/akira-yamaoka"


def test_fetch_listing_page_parses_event_data():
    payload = json.loads((FIXTURES_DIR / "listing_page1.json").read_text(encoding="utf-8"))
    session = FakeSession([FakeResponse(status_code=200, json_data=payload)])
    event_data = client.fetch_listing_page(session, 1)
    assert event_data["event_pages_total"] == 103
    assert len(event_data["event_items_list"]) == 3
    # multipart form field
    _method, _url, kwargs = session.requests[0]
    assert "by_event_page" in kwargs.get("files", {})
