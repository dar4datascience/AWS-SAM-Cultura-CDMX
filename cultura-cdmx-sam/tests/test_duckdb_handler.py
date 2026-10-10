import json
import subprocess

import app as duckdb_app


def test_git_blob_sha_matches_git_hash_object():
    data = b"fake-parquet-bytes\x00\x01\x02"
    expected = subprocess.run(
        ["git", "hash-object", "--stdin"],
        input=data,
        capture_output=True,
        check=True,
    ).stdout.strip().decode()
    assert duckdb_app.git_blob_sha(data) == expected


def test_events_copy_sql_schema(tmp_path):
    duckdb = __import__("duckdb")
    records = [
        {
            "page_number": 1,
            "card_index": 0,
            "detail_url": "https://cartelera.cdmx.gob.mx/1/09-10-2026/x",
            "description": ["d"],
            "info": ["Duración: 01:00"],
            "schedule": {"date": "Viernes, 9 De Octubre De 2026", "hour": "11:00"},
            "location": "Calle",
            "banner_url": "https://img",
            "evento": "Ev",
            "recinto": "Rec",
            "event_id": "1",
            "event_type": "Teatro",
            "fecha_iso": "2026-10-09",
            "lat": 19.4,
            "lon": -99.1,
            "venue_id": "5",
            "event_url": "https://ev",
            "costs": ["General: Gratuito"],
            "all_dates": ["2026-10-09"],
            "sessions": ["2026-10-09 11:00"],
        },
        {
            # failure-record shape: detail-derived fields are null
            "page_number": 1,
            "card_index": 1,
            "detail_url": "https://cartelera.cdmx.gob.mx/2/09-10-2026/y",
            "description": None,
            "info": None,
            "schedule": None,
            "location": None,
            "banner_url": None,
            "evento": "Ev2",
            "recinto": "Rec2",
            "event_id": "2",
            "event_type": "Museos",
            "fecha_iso": "2026-10-09",
            "lat": 19.5,
            "lon": -99.2,
            "venue_id": None,
            "event_url": None,
            "costs": None,
            "all_dates": None,
            "sessions": None,
        },
    ]
    src = tmp_path / "events.json"
    src.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
    out = tmp_path / "out.parquet"

    con = duckdb.connect()
    con.execute(duckdb_app.events_copy_sql(f"file://{src}", f"file://{out}"))
    schema = {
        r[0]: r[1]
        for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{out}')").fetchall()
    }
    n = con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    con.close()

    # DuckDB quotes the `hour` member in DESCRIBE output (it's a keyword)
    assert schema["schedule"].replace('"', "") == "STRUCT(date VARCHAR, hour VARCHAR)"
    assert schema["description"] == "VARCHAR[]"
    assert schema["sessions"] == "VARCHAR[]"
    assert schema["fecha_iso"] == "VARCHAR"
    assert schema["lat"] == "DOUBLE"
    assert n == 2
