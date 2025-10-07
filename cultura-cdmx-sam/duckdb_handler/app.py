import json
import duckdb
import boto3
import os
from urllib.parse import urlparse
import base64
import requests
from botocore.exceptions import ClientError

bucket_name = os.getenv("BUCKET_NAME")
print(f"[DEBUG] BUCKET_NAME: {bucket_name}")

os.environ["DUCKDB_HOME"] = "/tmp"
os.environ["DUCKDB_TMPDIR"] = "/tmp"
print(f"[DEBUG] DUCKDB_HOME: {os.environ['DUCKDB_HOME']}")
print(f"[DEBUG] DUCKDB_TMPDIR: {os.environ['DUCKDB_TMPDIR']}")

def get_secret(secret_arn: str) -> str:
    print(f"[DEBUG] Retrieving secret from ARN: {secret_arn}")
    region_name = os.environ.get("AWS_REGION", "mx-central-1")
    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_arn)
        secret_str = response["SecretString"]
        secret_dict = json.loads(secret_str)
        token = secret_dict.get("token") or secret_dict.get("GITHUB_TOKEN") or secret_str
        print("[DEBUG] Secret retrieved successfully")
        return token
    except ClientError as e:
        print(f"[ERROR] Failed to retrieve secret: {e}")
        raise RuntimeError(f"Failed to retrieve GitHub secret: {e}")

def upload_to_github(file_path, repo_owner, repo_name, target_path, commit_message, github_token):
    print(f"[DEBUG] Uploading {file_path} to GitHub repo {repo_owner}/{repo_name} at {target_path}")
    api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{target_path}"

    with open(file_path, "rb") as f:
        encoded_content = base64.b64encode(f.read()).decode("utf-8")

    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json",
    }
    payload = {
        "message": commit_message,
        "content": encoded_content
    }

    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        sha = response.json().get("sha")
        payload["sha"] = sha
        print(f"[DEBUG] File exists in repo, updating with SHA: {sha}")
    else:
        print(f"[DEBUG] File does not exist in repo, creating new file")

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
        con.execute(f"""
            COPY (
                SELECT *
                FROM read_json_auto('{input_path}')
            )
            TO '{output_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
        """)
        print("[DEBUG] DuckDB COPY completed successfully")
    except Exception as e:
        print(f"[ERROR] DuckDB COPY failed: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    local_path = f"/tmp/scraped_data_{snapshot_date}.parquet"
    print(f"[DEBUG] Downloading Parquet to local path: {local_path}")
    s3_client.download_file(bucket_name, output_key, local_path)
    print("[DEBUG] Download completed")

    repo_owner = os.getenv("GITHUB_OWNER")
    repo_name = os.getenv("GITHUB_REPO")
    target_path = f"data/scraped_data_{snapshot_date}.parquet"
    commit_message = f"Add scraped parquet for {snapshot_date}"
    secret_arn = os.getenv("GITHUB_SECRET_ARN")
    if not secret_arn:
        print("[ERROR] Missing GITHUB_SECRET_ARN environment variable")
        raise RuntimeError("Missing GITHUB_SECRET_ARN environment variable.")

    github_token = get_secret(secret_arn)
    upload_result = upload_to_github(
        file_path=local_path,
        repo_owner=repo_owner,
        repo_name=repo_name,
        target_path=target_path,
        commit_message=commit_message,
        github_token=github_token
    )

    print(f"[DEBUG] Parquet uploaded to GitHub: {upload_result['content']['html_url']}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Parquet uploaded to GitHub: {upload_result['content']['html_url']}"
        })
    }
