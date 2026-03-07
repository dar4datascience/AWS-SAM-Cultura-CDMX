"""
Lambda integration test for PlaywrightCardScrapper.

Invokes the deployed Lambda synchronously, then reads and validates the S3 output.
Uses a test_ snapshot_date prefix to avoid polluting production data.

Usage:
    python scripts/test_lambda.py [page_number]

Defaults: page_number=1

Environment: must have AWS credentials configured for mx-central-1.
"""

import boto3
import json
import sys
import time
from botocore.config import Config

# ── Config ────────────────────────────────────────────────────────────────────
REGION          = "mx-central-1"
FUNCTION_NAME   = "cultura-cartelera-cdmx-PlaywrightCardScrapper-2CUqPBlSBXUq"
BUCKET_NAME     = "cultura-cartelera-cdmx-mx-central-1-829489762414"
LAMBDA_TIMEOUT  = 310          # seconds — must exceed Lambda's 300s timeout
PAGE_NUMBER     = int(sys.argv[1]) if len(sys.argv) > 1 else 1
SNAPSHOT_DATE   = f"test_{time.strftime('%Y%m%d')}"

REQUIRED_FIELDS = ["detail_url", "evento", "recinto", "location", "schedule", "banner_url"]

# ── Helpers ───────────────────────────────────────────────────────────────────
def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

def ok(msg):   print(f"  [OK]   {msg}")
def fail(msg): print(f"  [FAIL] {msg}")
def info(msg): print(f"  [INFO] {msg}")


def validate_events(events: list) -> bool:
    passed = True
    for idx, ev in enumerate(events):
        empty = [f for f in REQUIRED_FIELDS if not ev.get(f)]
        if empty:
            fail(f"Event {idx} (card_index={ev.get('card_index')}) missing fields: {empty}")
            passed = False
        else:
            ok(f"Event {idx}  evento='{ev.get('evento', '')[:50]}'  recinto='{ev.get('recinto', '')[:40]}'")
    return passed


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\nLambda integration test")
    print(f"  Function  : {FUNCTION_NAME}")
    print(f"  Bucket    : {BUCKET_NAME}")
    print(f"  Page      : {PAGE_NUMBER}")
    print(f"  Snapshot  : {SNAPSHOT_DATE}")

    # ── Step 1: invoke Lambda ─────────────────────────────────────────────────
    section("STEP 1 — Invoke PlaywrightCardScrapper")

    lambda_client = boto3.client(
        "lambda",
        region_name=REGION,
        config=Config(read_timeout=LAMBDA_TIMEOUT, connect_timeout=10),
    )

    payload = {"page_number": PAGE_NUMBER, "snapshot_date": SNAPSHOT_DATE}
    info(f"Payload: {json.dumps(payload)}")
    info("Invoking synchronously (this may take up to 5 minutes)...")

    t0 = time.time()
    try:
        response = lambda_client.invoke(
            FunctionName=FUNCTION_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
    except Exception as exc:
        fail(f"Lambda invoke error: {exc}")
        return 1

    elapsed = time.time() - t0
    status_code = response["StatusCode"]
    function_error = response.get("FunctionError")

    info(f"HTTP status: {status_code}  elapsed: {elapsed:.1f}s")

    resp_payload = json.loads(response["Payload"].read())

    if function_error:
        fail(f"Lambda FunctionError={function_error}")
        print(json.dumps(resp_payload, indent=2)[:1000])
        return 1

    ok(f"Lambda returned successfully in {elapsed:.1f}s")
    print(f"  Lambda response: {json.dumps(resp_payload, indent=2)[:400]}")

    # ── Step 2: read S3 output ────────────────────────────────────────────────
    section("STEP 2 — Read S3 output")

    s3 = boto3.client("s3", region_name=REGION)
    s3_key = f"snapshot_date/{SNAPSHOT_DATE}/events_page_{PAGE_NUMBER}.json"
    info(f"s3://{BUCKET_NAME}/{s3_key}")

    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        events = json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        fail("S3 object not found — Lambda may have failed to upload")
        return 1
    except Exception as exc:
        fail(f"S3 read error: {exc}")
        return 1

    ok(f"S3 object found — {len(events)} events, {obj['ContentLength']} bytes")

    # ── Step 3: validate fields ───────────────────────────────────────────────
    section("STEP 3 — Validate event fields")

    cards_scraped = sum(1 for e in events if e.get("evento"))
    cards_failed  = sum(1 for e in events if not e.get("evento"))
    info(f"cards_scraped={cards_scraped}  cards_failed={cards_failed}  total={len(events)}")

    passed = validate_events(events)

    # ── Step 4: cleanup test data ─────────────────────────────────────────────
    section("STEP 4 — Cleanup test S3 object")
    try:
        s3.delete_object(Bucket=BUCKET_NAME, Key=s3_key)
        ok(f"Deleted s3://{BUCKET_NAME}/{s3_key}")
    except Exception as exc:
        info(f"Cleanup skipped: {exc}")

    # ── Summary ───────────────────────────────────────────────────────────────
    section("RESULT")
    if passed and cards_failed == 0:
        ok(f"ALL {len(events)} events scraped successfully — ready to deploy")
        return 0
    elif cards_scraped > 0:
        fail(f"{cards_failed}/{len(events)} events failed — check logs above")
        return 1
    else:
        fail("ALL events failed — scraper is broken")
        return 1


if __name__ == "__main__":
    sys.exit(main())
