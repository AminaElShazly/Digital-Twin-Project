"""Lambda: poll the greenhouse API, back up the raw JSON to S3, store readings in DynamoDB.

Triggered every 5 minutes by EventBridge Scheduler.

Required environment variables
    API_URL          base URL of the greenhouse telemetry API (no trailing slash)
    RAW_BUCKET       S3 bucket for raw JSON backups
    SENSORS_TABLE    DynamoDB table of sensor metadata   (default: Sensors)
    READINGS_TABLE   DynamoDB table of time-series data  (default: Readings)
"""
import json
import os
import time
import urllib.request
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3

API_URL = os.environ["API_URL"]
RAW_BUCKET = os.environ["RAW_BUCKET"]
SENSORS_TABLE = os.environ.get("SENSORS_TABLE", "Sensors")
READINGS_TABLE = os.environ.get("READINGS_TABLE", "Readings")

# AWS clients (created outside the handler so warm invocations reuse them)
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sensors_table = dynamodb.Table(SENSORS_TABLE)
readings_table = dynamodb.Table(READINGS_TABLE)

run_id = None
sensor_cache = {}


# ---------- helpers ----------
def fetch_with_retry(url, max_retries=3):
    """Fetch JSON, retrying with exponential backoff (1 s, then 2 s between attempts)."""
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "IJC308-DigitalTwin"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                print(f"[INFO] [{run_id}] API fetch successful (attempt {attempt + 1})")
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"[ERROR] [{run_id}] Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    return None


def iso_change(timestamp):
    """Convert an ISO-8601 timestamp to a Unix epoch (falls back to 'now' if unparseable)."""
    try:
        return int(datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp())
    except (ValueError, AttributeError):
        return int(datetime.now(timezone.utc).timestamp())


def to_decimal(value):
    """DynamoDB does not accept Python floats, so convert to Decimal."""
    if isinstance(value, float):
        return Decimal(str(round(value, 6)))
    if isinstance(value, int):
        return Decimal(value)
    return value


def load_sensors():
    """Load sensor metadata from the Sensors table (cached across warm invocations)."""
    global sensor_cache
    if sensor_cache:
        return sensor_cache
    response = sensors_table.scan()
    for item in response.get("Items", []):
        sensor_cache[item["sensor_key"]] = {
            "display_name": item.get("display_name", item["sensor_key"]),
            "unit": item.get("unit", ""),
            "category": item.get("category", "unknown"),
        }
    print(f"[INFO] [{run_id}] Loaded {len(sensor_cache)} sensors from Sensors table")
    return sensor_cache


# ---------- handler ----------
def lambda_handler(event, context):
    global run_id
    run_id = uuid.uuid4().hex[:8]
    print(f"[INFO] [{run_id}] Starting Data Collection")

    # Step 1: load sensor metadata
    sensors = load_sensors()

    # Step 2: fetch the latest reading from the API
    data = fetch_with_retry(f"{API_URL}/data/latest?count=1")
    if not data:
        return {"statusCode": 500, "body": "API failed"}

    # Step 3: validate the response
    if "data" not in data or len(data["data"]) == 0:
        return {"statusCode": 500, "body": "Invalid response"}
    print(f"[INFO] [{run_id}] Received {data.get('count', 1)} record(s)")

    # Step 4: back up the raw JSON to S3 *before* parsing
    s3_key = f"raw/{time.strftime('%Y/%m/%d')}/{int(time.time())}_{run_id}.json"
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=s3_key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"[INFO] [{run_id}] S3 backup: {s3_key}")

    # Step 5: parse timestamp
    row = data["data"][0]
    timestamp = row.get("timestamp", datetime.now(timezone.utc).isoformat())
    ts = iso_change(timestamp)
    day = timestamp[:10]
    print(f"[INFO] [{run_id}] Data timestamp: {timestamp}")

    # Step 6: store one item per sensor; a failure on one sensor does not stop the rest
    records = 0
    failed = 0
    for sensor_key, value in row.items():
        if sensor_key in ["timestamp", "matlab_datenum"] or value is None:
            continue

        sensor_key_str = str(sensor_key)  # API returns some keys as integers/strings like "14"

        if sensor_key_str not in sensors:
            print(f"[WARN] [{run_id}] Unknown sensor: {sensor_key_str}")

        try:
            readings_table.put_item(Item={
                "sensor_id": sensor_key_str,
                "ts": ts,
                "value": to_decimal(value) if isinstance(value, (int, float)) else str(value),
                "day": day,
                "timestamp_iso": timestamp,
                "raw_s3_key": s3_key,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            })
            records += 1
        except Exception as e:
            failed += 1
            print(f"[ERROR] [{run_id}] Failed {sensor_key_str}: {e}")

    print(f"[INFO] [{run_id}] Completed")
    print(f"[INFO] [{run_id}] Records: {records} written, {failed} failed")

    return {"statusCode": 200, "run_id": run_id, "records": records, "failed": failed, "s3_key": s3_key}
