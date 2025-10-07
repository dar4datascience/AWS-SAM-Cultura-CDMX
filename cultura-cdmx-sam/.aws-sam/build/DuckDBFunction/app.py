import json
import duckdb
import boto3
import os
from urllib.parse import urlparse

bucket_name = os.getenv("BUCKET_NAME")
os.environ["DUCKDB_HOME"] = "/tmp"
os.environ["DUCKDB_TMPDIR"] = "/tmp"

import base64
import requests
from botocore.exceptions import ClientError

def get_secret(secret_arn: str) -> str:
    """
    Retrieve the GitHub token from AWS Secrets Manager.

    Args:
        secret_arn (str): The ARN or name of the secret in Secrets Manager.

    Returns:
        str: The GitHub token string.

    Raises:
        ClientError: If the secret cannot be retrieved.
    """
    region_name = os.environ.get("AWS_REGION", "mx-central-1")
    session = boto3.session.Session()
    client = session.client("secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_arn)
        secret_str = response["SecretString"]
        secret_dict = json.loads(secret_str)
        return secret_dict.get("token") or secret_dict.get("GITHUB_TOKEN") or secret_str
    except ClientError as e:
        raise RuntimeError(f"Failed to retrieve GitHub secret: {e}")


def upload_to_github(file_path, repo_owner, repo_name, target_path, commit_message, github_token):
    """
    Uploads or updates a file in a GitHub repository using the REST API.
    
    Args:
        file_path (str): Local path to the file to upload.
        repo_owner (str): GitHub username or org name.
        repo_name (str): Repository name (without .git).
        target_path (str): Path in the repository where the file will be stored.
        commit_message (str): Commit message.
        github_token (str): GitHub personal access token (must have `contents:write`).
    """
    api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{target_path}"

    # Read and encode the file in Base64
    with open(file_path, "rb") as f:
        encoded_content = base64.b64encode(f.read()).decode("utf-8")

    # Prepare headers and payload
    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json",
    }
    payload = {
        "message": commit_message,
        "content": encoded_content
    }

    # Check if file exists to include SHA for updates
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        sha = response.json().get("sha")
        payload["sha"] = sha

    # Upload (create or update)
    response = requests.put(api_url, headers=headers, data=json.dumps(payload))

    if response.status_code not in (200, 201):
        raise Exception(f"GitHub upload failed: {response.status_code} - {response.text}")

    return response.json()


def lambda_handler(event, context):
    """
    Lambda that reads JSON files from a snapshot_date prefix in S3,
    merges them into a single Parquet, and writes back to S3 under 'database/'.
    
    Expected event:
    {
        "snapshot_date": "20251002"
    }
    """
    snapshot_date = event.get("snapshot_date")
    if not snapshot_date:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing 'snapshot_date' in event"})
        }

    prefix = f"snapshot_date/{snapshot_date}/"
    output_key = f"database/scraped_data_{snapshot_date}.parquet"

    # Construct S3 path DuckDB can read directly
    input_path = f"s3://{bucket_name}/{prefix}*.json"
    output_path = f"s3://{bucket_name}/{output_key}"

    # Register S3 for DuckDB (needs boto3 credentials in Lambda runtime)
    con = duckdb.connect(database=":memory:")
    con.execute("SET home_directory= '/tmp';")
    con.install_extension("aws")
    con.install_extension("httpfs")
    con.load_extension("aws")
    con.load_extension("httpfs")
    s3 = boto3.client("s3")
    con.sql("""
            CREATE SECRET (
                TYPE S3, 
                PROVIDER CREDENTIAL_CHAIN
            );
    """)


    # Read all JSON files into DuckDB and write a single Parquet
    try:
        con.execute(f"""
            COPY (
                SELECT *
                FROM read_json_auto('{input_path}')
            )
            TO '{output_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
        """)
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    # Download parquet from S3 into /tmp
    local_path = f"/tmp/scraped_data_{snapshot_date}.parquet"
    s3.download_file(bucket_name, output_key, local_path)

    # Upload to GitHub
    repo_owner = os.getenv("GITHUB_OWNER")       # e.g. "DanielAmieva"
    repo_name = os.getenv("GITHUB_REPO")         # e.g. "cultura-data"
    target_path = f"data/scraped_data_{snapshot_date}.parquet"
    commit_message = f"Add scraped parquet for {snapshot_date}"
    secret_arn = os.getenv("GITHUB_SECRET_ARN")
    if not secret_arn:
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

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Parquet uploaded to GitHub: {upload_result['content']['html_url']}"
        })
    }