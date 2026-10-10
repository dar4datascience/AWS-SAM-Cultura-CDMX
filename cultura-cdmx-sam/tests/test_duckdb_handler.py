import gzip
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


def test_deterministic_gzip():
    data = b"parquet payload" * 100
    assert gzip.compress(data, mtime=0) == gzip.compress(data, mtime=0)
