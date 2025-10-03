import json
import duckdb
import boto3
import os
from urllib.parse import urlparse

s3 = boto3.client("s3")
bucket_name = os.getenv("BUCKET_NAME")

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
    output_key = f"database/{snapshot_date}.parquet"

    # Construct S3 path DuckDB can read directly
    input_path = f"s3://{bucket_name}/{prefix}*.json"
    output_path = f"s3://{bucket_name}/{output_key}"

    # Register S3 for DuckDB (needs boto3 credentials in Lambda runtime)
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='{}'".format(os.getenv("AWS_REGION", "mx-central-1")))
    con.execute("SET s3_access_key_id='{}'".format(os.getenv("AWS_ACCESS_KEY_ID", "")))
    con.execute("SET s3_secret_access_key='{}'".format(os.getenv("AWS_SECRET_ACCESS_KEY", "")))
    if os.getenv("AWS_SESSION_TOKEN"):
        con.execute("SET s3_session_token='{}'".format(os.getenv("AWS_SESSION_TOKEN")))

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

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Parquet written to s3://{bucket_name}/{output_key}"
        })
    }
