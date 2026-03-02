# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 11 — Poller: Vehicle Positions
# MAGIC
# MAGIC **Schedule:** Every 1 minute via Databricks Workflow
# MAGIC
# MAGIC **What it does:**
# MAGIC - Fetches the VehiclePositions GTFS-RT feed from mecatran
# MAGIC - Filters to trip-linked entities only (position-only records have no trip_id
# MAGIC   and are not useful for performance analytics)
# MAGIC - Writes one NDJSON snapshot file to the Volume
# MAGIC
# MAGIC **Note on position-only vehicles:**
# MAGIC The mecatran feed includes two entity types:
# MAGIC - `RTVP:T:*` — trip-linked (has routeId, directionId, stop info) ✓ kept
# MAGIC - `RTVP:V:*` — position-only (no trip association) ✗ filtered at poll time
# MAGIC
# MAGIC **Output path:**
# MAGIC ```
# MAGIC /Volumes/.../raw/rt_snapshots/vehicle_positions/YYYY/MM/DD/HH-MM-SS.ndjson
# MAGIC ```

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

import requests
import json
import os
from datetime import datetime, timezone

# COMMAND ----------

def poll_vehicle_positions(api_key: str) -> dict:
    resp = requests.get(
        VEHICLE_POSITIONS_URL,
        params={"apiKey": api_key, "asJson": "true"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def write_snapshot(data: dict, snapshot_ts: datetime) -> str:
    """
    Write NDJSON snapshot for vehicle positions.
    Only includes trip-linked entities (those with vehicle.trip set).
    """
    header   = data.get("header", {})
    entities = data.get("entity", [])
    feed_ts  = header.get("timestamp", "")

    lines = []
    skipped = 0
    for entity in entities:
        veh = entity.get("vehicle", {})
        if not veh.get("trip"):
            skipped += 1
            continue   # position-only, no trip context

        record = {
            "feed_ts":     feed_ts,
            "snapshot_ts": snapshot_ts.isoformat(),
            "id":          entity.get("id"),
            "vehicle":     veh,
        }
        lines.append(json.dumps(record, separators=(",", ":")))

    if not lines:
        print("⚠  No trip-linked vehicle position entities — skipping write.")
        return None, skipped

    date_dir  = snapshot_ts.strftime("%Y/%m/%d")
    file_name = snapshot_ts.strftime("%H-%M-%S") + ".ndjson"
    dir_path  = f"{VEHICLE_POSITIONS_SNAPSHOTS_PATH}/{date_dir}"
    file_path = f"{dir_path}/{file_name}"

    os.makedirs(dir_path, exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return file_path, skipped

# COMMAND ----------

# MAGIC %md ## Run

# COMMAND ----------

snapshot_ts = datetime.now(timezone.utc).replace(tzinfo=None)
api_key     = get_api_key()

print(f"[{snapshot_ts.isoformat()}] Polling VehiclePositions...")

try:
    data    = poll_vehicle_positions(api_key)
    total   = len(data.get("entity", []))
    feed_ts = data.get("header", {}).get("timestamp", "unknown")

    file_path, skipped = write_snapshot(data, snapshot_ts)

    if file_path:
        file_size  = os.path.getsize(file_path)
        kept       = total - skipped
        print(f"✓ Wrote {kept} trip-linked entities ({file_size:,} bytes)")
        print(f"  skipped position-only: {skipped}")
        print(f"  feed_ts : {feed_ts}")
        print(f"  path    : {file_path}")
    else:
        print(f"  Nothing written (all {total} entities were position-only).")

except requests.exceptions.Timeout:
    print("✗ Request timed out — will retry on next schedule.")
except requests.exceptions.HTTPError as e:
    print(f"✗ HTTP error: {e}")
except Exception as e:
    print(f"✗ Unexpected error: {e}")
    raise
