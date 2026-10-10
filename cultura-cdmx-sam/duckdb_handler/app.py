import os
import json
import gzip
import base64
import hashlib
from urllib.parse import urlparse

import duckdb
import boto3
from botocore.exceptions import ClientError
import requests


bucket_name = os.getenv("BUCKET_NAME")
print(f"[DEBUG] BUCKET_NAME: {bucket_name}")

os.environ["DUCKDB_HOME"] = "/tmp"
os.environ["DUCKDB_TMPDIR"] = "/tmp"
print(f"[DEBUG] DUCKDB_HOME: {os.environ['DUCKDB_HOME']}")
print(f"[DEBUG] DUCKDB_TMPDIR: {os.environ['DUCKDB_TMPDIR']}")

# Explicit schema: auto-detection would turn "11:00" into TIME and dates into
# DATE depending on each day's data, breaking the dashboard's JSON export.
EVENTS_JSON_COLUMNS = """{
    'page_number': 'BIGINT', 'card_index': 'BIGINT', 'detail_url': 'VARCHAR',
    'description': 'VARCHAR[]', 'info': 'VARCHAR[]',
    'schedule': 'STRUCT(date VARCHAR, hour VARCHAR)',
    'location': 'VARCHAR', 'banner_url': 'VARCHAR', 'evento': 'VARCHAR', 'recinto': 'VARCHAR',
    'event_id': 'VARCHAR', 'event_type': 'VARCHAR', 'fecha_iso': 'VARCHAR',
    'lat': 'DOUBLE', 'lon': 'DOUBLE', 'venue_id': 'VARCHAR', 'event_url': 'VARCHAR',
    'costs': 'VARCHAR[]', 'all_dates': 'VARCHAR[]'
}"""


def events_copy_sql(input_path: str, output_path: str) -> str:
    return f"""
        COPY (
            SELECT *
            FROM read_json('{input_path}', format = 'array', columns = {EVENTS_JSON_COLUMNS})
        )
        TO '{output_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
    """

def get_github_token() -> str:
    parameter_name = os.environ.get("GITHUB_TOKEN_PARAMETER")
    if not parameter_name:
        print("[ERROR] Missing GITHUB_TOKEN_PARAMETER environment variable")
        raise RuntimeError("Missing GITHUB_TOKEN_PARAMETER environment variable.")
    print(f"[DEBUG] Retrieving GitHub token from SSM parameter: {parameter_name}")
    client = boto3.client("ssm")
    try:
        response = client.get_parameter(Name=parameter_name, WithDecryption=True)
        print("[DEBUG] SSM parameter retrieved successfully")
        return response["Parameter"]["Value"]
    except ClientError as e:
        print(f"[ERROR] Failed to retrieve SSM parameter: {e}")
        raise RuntimeError(f"Failed to retrieve GitHub token parameter: {e}")


def git_blob_sha(content: bytes) -> str:
    """SHA-1 Git computes for a blob object: sha1('blob <len>\\0' + content)."""
    return hashlib.sha1(b"blob %d\0" % len(content) + content).hexdigest()


def upload_to_github(file_path, repo_owner, repo_name, target_path, commit_message, github_token):
    print(f"[DEBUG] Uploading {file_path} to GitHub repo {repo_owner}/{repo_name} at {target_path}")
    api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{target_path}"

    # Compress the file before encoding (deterministic gzip so identical
    # content always produces identical bytes and thus the same git blob SHA)
    with open(file_path, "rb") as f:
        gz_content = gzip.compress(f.read(), mtime=0)

    encoded_content = base64.b64encode(gz_content).decode("utf-8")
    blob_sha = git_blob_sha(gz_content)

    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json",
    }

    payload = {
        "message": commit_message,
        "content": encoded_content
    }

    # Check if file already exists to get SHA; skip the upload if unchanged
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        sha = response.json().get("sha")
        if sha == blob_sha:
            print("[DEBUG] Remote file is identical, skipping upload")
            return {"skipped": True, "sha": sha}
        payload["sha"] = sha
        print(f"[DEBUG] File exists in repo, updating with SHA: {sha}")
    else:
        print(f"[DEBUG] File does not exist in repo, creating new file")

    # Upload the file
    response = requests.put(api_url, headers=headers, data=json.dumps(payload))
    if response.status_code not in (200, 201):
        print(f"[ERROR] GitHub upload failed: {response.status_code} - {response.text}")
        raise Exception(f"GitHub upload failed: {response.status_code} - {response.text}")

    print(f"[DEBUG] Upload successful: {response.json().get('content', {}).get('html_url')}")
    return response.json()

def lambda_handler(event, context):
    print(f"[DEBUG] Event received: {event}")
    snapshot_date = event.get("snapshot_date")
    if isinstance(snapshot_date, dict):
        snapshot_date = snapshot_date.get("snapshot_date")

    if not snapshot_date:
        print("[ERROR] Missing 'snapshot_date' in event")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing 'snapshot_date' in event"})
        }


    prefix = f"snapshot_date/{snapshot_date}/"
    output_key = f"database/scraped_data_{snapshot_date}.parquet"

    input_path = f"s3://{bucket_name}/{prefix}*.json"
    output_path = f"s3://{bucket_name}/{output_key}"
    print(f"[DEBUG] DuckDB input path: {input_path}")
    print(f"[DEBUG] DuckDB output path: {output_path}")

    con = duckdb.connect(database=":memory:")
    print("[DEBUG] DuckDB connection established")
    con.execute("SET home_directory= '/tmp';")
    print("[DEBUG] DuckDB home directory set to /tmp")
    con.install_extension("aws")
    con.install_extension("httpfs")
    con.load_extension("aws")
    con.load_extension("httpfs")
    print("[DEBUG] DuckDB extensions loaded")

    s3_client = boto3.client("s3")

    try:
        con.execute(events_copy_sql(input_path, output_path))
        print("[DEBUG] DuckDB COPY completed successfully")
    except Exception as e:
        print(f"[ERROR] DuckDB COPY failed: {e}")
        raise

    local_path = f"/tmp/scraped_data_cultura_cartelera_cdmx.parquet"
    print(f"[DEBUG] Downloading Parquet to local path: {local_path}")
    s3_client.download_file(bucket_name, output_key, local_path)
    print("[DEBUG] Download completed")

    repo_owner = os.getenv("GITHUB_OWNER")
    repo_name = os.getenv("GITHUB_REPO")
    target_path = f"data/scraped_data_cultura_cartelera_cdmx.parquet"
    commit_message = f"Add scraped parquet for {snapshot_date}"

    github_token = get_github_token()
    upload_result = upload_to_github(
        file_path=local_path,
        repo_owner=repo_owner,
        repo_name=repo_name,
        target_path=target_path,
        commit_message=commit_message,
        github_token=github_token
    )

    if upload_result.get("skipped"):
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Parquet unchanged, GitHub upload skipped",
                "skipped": True
            }),
            "skipped": True
        }

    print(f"[DEBUG] Parquet uploaded to GitHub: {upload_result['content']['html_url']}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Parquet uploaded to GitHub: {upload_result['content']['html_url']}"
        })
    }
