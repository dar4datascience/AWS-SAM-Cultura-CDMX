import io
import json
from datetime import date

from botocore.exceptions import ClientError

import cache

TODAY = date(2026, 10, 9)


def _entry(fetched_at="2026-10-08", dates=None, event_id="1"):
    return {
        "fetched_at": fetched_at,
        "info": {
            "event_id": event_id,
            "schedule": {
                "1": {
                    "event_dates": dates if dates is not None else ["09/10/2026"],
                    "event_hours": ["11:00"],
                }
            },
        },
    }


def test_needs_refresh_missing():
    assert cache.needs_refresh(None, "09/10/2026", TODAY, 3) is True
    assert cache.needs_refresh({}, "09/10/2026", TODAY, 3) is True


def test_needs_refresh_fresh_and_date_present():
    assert cache.needs_refresh(_entry("2026-10-08", ["09/10/2026"]), "09/10/2026", TODAY, 3) is False
    # date present in a different schedule entry also counts
    entry = _entry("2026-10-08", ["15/10/2026"])
    entry["info"]["schedule"]["2"] = {"event_dates": ["09/10/2026"], "event_hours": []}
    assert cache.needs_refresh(entry, "09/10/2026", TODAY, 3) is False


def test_needs_refresh_age_reaches_ttl():
    # age == ttl -> refresh (boundary)
    assert cache.needs_refresh(_entry("2026-10-06", ["09/10/2026"]), "09/10/2026", TODAY, 3) is True
    # one day younger stays fresh
    assert cache.needs_refresh(_entry("2026-10-07", ["09/10/2026"]), "09/10/2026", TODAY, 3) is False


def test_needs_refresh_listing_date_not_in_schedule():
    # fresh entry, but the listing's located date is gone from the schedule
    assert cache.needs_refresh(_entry("2026-10-08", ["15/10/2026"]), "09/10/2026", TODAY, 3) is True


def test_needs_refresh_bad_fetched_at():
    entry = {"fetched_at": "not-a-date", "info": {"schedule": {}}}
    assert cache.needs_refresh(entry, "09/10/2026", TODAY, 3) is True
    entry = {"info": {"schedule": {"1": {"event_dates": ["09/10/2026"]}}}}
    assert cache.needs_refresh(entry, "09/10/2026", TODAY, 3) is True


class _FakeS3Error:
    def __init__(self, code):
        self._code = code

    def get_object(self, **_kwargs):
        raise ClientError({"Error": {"Code": self._code}}, "GetObject")


class _FakeS3Body:
    def __init__(self, body):
        self._body = body
        self.puts = []

    def get_object(self, **_kwargs):
        return {"Body": io.BytesIO(self._body)}

    def put_object(self, **kwargs):
        self.puts.append(kwargs)


def test_load_cache_s3_missing_key():
    assert cache.load_cache_s3(_FakeS3Error("NoSuchKey"), "b", "k") == {}
    assert cache.load_cache_s3(_FakeS3Error("404"), "b", "k") == {}


def test_load_cache_s3_other_client_error_and_bad_payload():
    assert cache.load_cache_s3(_FakeS3Error("AccessDenied"), "b", "k") == {}
    assert cache.load_cache_s3(_FakeS3Body(b"not json"), "b", "k") == {}
    assert cache.load_cache_s3(_FakeS3Body(b'{"version": 2, "entries": {}}'), "b", "k") == {}
    assert cache.load_cache_s3(_FakeS3Body(b'{"version": 1}'), "b", "k") == {}


def test_cache_s3_roundtrip():
    entries = {"1": _entry()}
    s3 = _FakeS3Body(b"")
    cache.save_cache_s3(s3, "b", "k", entries)
    body = json.loads(s3.puts[0]["Body"].decode("utf-8"))
    assert body == {"version": 1, "entries": entries}
    assert cache.load_cache_s3(_FakeS3Body(s3.puts[0]["Body"]), "b", "k") == entries


def test_cache_file_roundtrip(tmp_path):
    path = tmp_path / "cache.json"
    entries = {"1": _entry()}
    cache.save_cache_file(path, entries)
    assert cache.load_cache_file(path) == entries


def test_cache_file_missing_and_corrupt(tmp_path):
    assert cache.load_cache_file(tmp_path / "missing.json") == {}
    bad = tmp_path / "bad.json"
    bad.write_text("{oops", encoding="utf-8")
    assert cache.load_cache_file(bad) == {}
