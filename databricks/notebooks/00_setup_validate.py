# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 00 — Setup & Validation
# MAGIC
# MAGIC Run this once before starting the pipeline to:
# MAGIC 1. Confirm Volume paths are reachable
# MAGIC 2. Validate API connectivity for both RT feeds
# MAGIC 3. Inspect live feed entity counts and field shapes
# MAGIC 4. Download and preview the GTFS static feed
# MAGIC 5. Confirm you can write/read to Delta in this catalog/schema
# MAGIC
# MAGIC **Run time:** ~2 minutes

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

# MAGIC %md ## 1 — Volume Path Check

# COMMAND ----------

import os

paths_to_check = [
    BASE_PATH,
    RAW_STATIC_PATH,
    RAW_RT_PATH,
    TRIP_UPDATES_SNAPSHOTS_PATH,
    VEHICLE_POSITIONS_SNAPSHOTS_PATH,
    CHECKPOINT_PATH,
    TRIP_UPDATES_CHECKPOINT,
    VEHICLE_POSITIONS_CHECKPOINT,
    CURATED_PATH,
    LOGS_PATH,
]

print("Ensuring Volume paths exist...\n")
for p in paths_to_check:
    os.makedirs(p, exist_ok=True)
    print(f"  ✓  {p}")

print("\n✓ All Volume paths confirmed.")

# COMMAND ----------

# MAGIC %md ## 2 — Secrets Check

# COMMAND ----------

try:
    api_key = get_api_key()
    masked = "*" * (len(api_key) - 4) + api_key[-4:]
    print(f"✓ API key found via MECATRAN_API_KEY env var: {masked}")
except Exception as e:
    print(f"✗ {e}")
    print("""
Add this to a cell above and re-run:
  import os
  os.environ["MECATRAN_API_KEY"] = "<your key>"
""")
    raise

# COMMAND ----------

# MAGIC %md ## 3 — Trip Updates Feed

# COMMAND ----------

import requests
import json
from datetime import datetime, timezone

def fetch_feed(url: str, api_key: str) -> dict:
    resp = requests.get(url, params={"apiKey": api_key, "asJson": "true"}, timeout=15)
    resp.raise_for_status()
    return resp.json()

api_key = get_api_key()

print("Fetching TripUpdates feed...")
tu_data = fetch_feed(TRIP_UPDATES_URL, api_key)

header    = tu_data.get("header", {})
entities  = tu_data.get("entity", [])
feed_ts   = int(header.get("timestamp", 0))
feed_time = datetime.fromtimestamp(feed_ts, tz=timezone.utc).isoformat()

print(f"\nHeader:")
print(f"  gtfsRealtimeVersion : {header.get('gtfsRealtimeVersion')}")
print(f"  incrementality      : {header.get('incrementality')}")
print(f"  timestamp (UTC)     : {feed_time}")
print(f"\nTotal entities: {len(entities)}")

# Count entity types
with_trip_update = [e for e in entities if "tripUpdate" in e]
print(f"  → tripUpdate entities: {len(with_trip_update)}")

# Stop time update stats
all_stu_counts = [len(e["tripUpdate"].get("stopTimeUpdate", [])) for e in with_trip_update]
avg_stu = sum(all_stu_counts) / len(all_stu_counts) if all_stu_counts else 0
print(f"  → avg stopTimeUpdates per trip: {avg_stu:.1f}")
print(f"  → total stop-time rows this snapshot: {sum(all_stu_counts)}")

# Show one entity
print("\nSample entity:")
print(json.dumps(with_trip_update[0], indent=2))

# COMMAND ----------

# MAGIC %md ## 4 — Vehicle Positions Feed

# COMMAND ----------

print("Fetching VehiclePositions feed...")
vp_data = fetch_feed(VEHICLE_POSITIONS_URL, api_key)

vp_entities = vp_data.get("entity", [])
print(f"\nTotal VP entities: {len(vp_entities)}")

# Split trip-linked vs position-only
trip_linked   = [e for e in vp_entities if e.get("vehicle", {}).get("trip")]
position_only = [e for e in vp_entities if not e.get("vehicle", {}).get("trip")]

print(f"  → trip-linked (usable for analytics): {len(trip_linked)}")
print(f"  → position-only (no trip, filtered out): {len(position_only)}")

# Speed distribution
speeds = [e["vehicle"]["position"].get("speed", 0) for e in trip_linked]
speeds_nonzero = [s for s in speeds if s > 0]
print(f"\nSpeed stats (m/s) among moving vehicles:")
print(f"  count moving: {len(speeds_nonzero)}")
if speeds_nonzero:
    print(f"  min: {min(speeds_nonzero):.2f}  max: {max(speeds_nonzero):.2f}  avg: {sum(speeds_nonzero)/len(speeds_nonzero):.2f}")

# Unique routes active
routes = set(e["vehicle"]["trip"]["routeId"] for e in trip_linked)
print(f"\nActive routes right now: {len(routes)}")
print(f"  Sample: {sorted(routes)[:20]}")

print("\nSample trip-linked entity:")
print(json.dumps(trip_linked[0], indent=2))

# COMMAND ----------

# MAGIC %md ## 5 — GTFS Static Feed Preview

# COMMAND ----------

import zipfile
import io

static_zip_path = f"{RAW_STATIC_PATH}/googletransit.zip"

print(f"Downloading GTFS static to {static_zip_path}...")
headers = {"User-Agent": "Mozilla/5.0 (compatible; transit-pipeline/1.0)"}
resp = requests.get(GTFS_STATIC_URL, headers=headers, timeout=60)
resp.raise_for_status()
with open(static_zip_path, "wb") as f:
    f.write(resp.content)
print(f"✓ Downloaded. Size: {os.path.getsize(static_zip_path):,} bytes")

with zipfile.ZipFile(static_zip_path, "r") as zf:
    files = zf.namelist()
    print(f"\nFiles in ZIP ({len(files)} total):")
    for f in sorted(files):
        info = zf.getinfo(f)
        print(f"  {f:35s}  {info.file_size:>10,} bytes")

# COMMAND ----------

# Quick peek at key files
with zipfile.ZipFile(static_zip_path, "r") as zf:
    for fname in ["routes.txt", "stops.txt", "trips.txt", "stop_times.txt"]:
        if fname in zf.namelist():
            with zf.open(fname) as f:
                lines = f.read().decode("utf-8-sig").strip().split("\n")
                print(f"\n{fname} — {len(lines)-1:,} rows")
                print(f"  header: {lines[0]}")
                print(f"  row 1 : {lines[1]}")

# COMMAND ----------

# MAGIC %md ## 6 — Delta Write Smoke Test

# COMMAND ----------

from pyspark.sql import Row
from datetime import date

test_table = f"{CATALOG}.{SCHEMA}.zz_setup_smoke_test"

print(f"Writing test row to {test_table}...")
test_df = spark.createDataFrame([
    Row(test_ts=datetime.now(timezone.utc).replace(tzinfo=None), note="setup_ok")
])
test_df.write.format("delta").mode("overwrite").saveAsTable(test_table)

count = spark.table(test_table).count()
print(f"✓ Read back {count} row(s) — Delta write/read confirmed.")

# Clean up
spark.sql(f"DROP TABLE IF EXISTS {test_table}")
print("✓ Test table dropped. Setup complete.")
