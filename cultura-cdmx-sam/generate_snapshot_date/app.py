# generate_snapshot_date.py
from datetime import datetime, timezone
import json

def lambda_handler(event, context):
    """
    Returns a UTC snapshot_date in YYYYMMDD format.
    """
    return {
        "snapshot_date": datetime.now(timezone.utc).strftime("%Y%m%d")
    }
