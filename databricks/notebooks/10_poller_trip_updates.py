# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 10 — Poller: Trip Updates
# MAGIC
# MAGIC **Schedule:** Every 1 minute via Databricks Workflow (shortest supported interval)
# MAGIC
# MAGIC **What it does:**
# MAGIC - Fetches the TripUpdates GTFS-RT feed from mecatran
# MAGIC - Writes one NDJSON snapshot file (one JSON line per entity) to the Volume
# MAGIC - AutoLoader notebook `20_bronze_trip_updates` picks these up asynchronously
# MAGIC
# MAGIC **Why NDJSON files, not writing directly to Delta?**
# MAGIC - Raw files = permanent audit trail (survives schema changes)
# MAGIC - Decouples polling rate from Spark streaming batch cadence
# MAGIC - AutoLoader handles schema inference, file tracking, and exactly-once semantics
# MAGIC
# MAGIC **Output path:**
# MAGIC ```
# MAGIC /Volumes/.../raw/rt_snapshots/trip_updates/YYYY/MM/DD/HH-MM-SS.ndjson
# MAGIC ```

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

import requests
import json
import os
from datetime import datetime, timezone

# COMMAND ----------

def poll_trip_updates(api_key: str) -> dict:
    resp = requests.get(
        TRIP_UPDATES_URL,
        params={"apiKey": api_key, "asJson": "true"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def write_snapshot(data: dict, snapshot_ts: datetime) -> str:
    """
    Write one NDJSON file where each line is one entity record, enriched with
    feed_ts and snapshot_ts metadata.

    Returns the path written.
    """
    header   = data.get("header", {})
    entities = data.get("entity", [])
    feed_ts  = header.get("timestamp", "")

    # One line per entity — AutoLoader reads these as individual rows
    lines = []
    for entity in entities:
        if "tripUpdate" not in entity:
            continue  # skip any non-trip-update entities
        record = {
            "feed_ts":       feed_ts,
            "snapshot_ts":   snapshot_ts.isoformat(),
            "id":            entity.get("id"),
            "tripUpdate":    entity["tripUpdate"],
        }
        lines.append(json.dumps(record, separators=(",", ":")))

    if not lines:
        print("⚠  No tripUpdate entities in this snapshot — skipping write.")
        return None

    # Partition by date so AutoLoader can incrementally process
    date_dir = snapshot_ts.strftime("%Y/%m/%d")
    file_name = snapshot_ts.strftime("%H-%M-%S") + ".ndjson"
    dir_path  = f"{TRIP_UPDATES_SNAPSHOTS_PATH}/{date_dir}"
    file_path = f"{dir_path}/{file_name}"

    os.makedirs(dir_path, exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return file_path

# COMMAND ----------

# MAGIC %md ## Run

# COMMAND ----------

snapshot_ts = datetime.now(timezone.utc).replace(tzinfo=None)
api_key     = get_api_key()

print(f"[{snapshot_ts.isoformat()}] Polling TripUpdates...")

try:
    data        = poll_trip_updates(api_key)
    entity_cnt  = len([e for e in data.get("entity", []) if "tripUpdate" in e])
    feed_ts_raw = data.get("header", {}).get("timestamp", "unknown")

    file_path = write_snapshot(data, snapshot_ts)

    if file_path:
        file_size = os.path.getsize(file_path)
        print(f"✓ Wrote {entity_cnt} entities ({file_size:,} bytes)")
        print(f"  feed_ts : {feed_ts_raw}")
        print(f"  path    : {file_path}")
    else:
        print("  Nothing written.")

except requests.exceptions.Timeout:
    print("✗ Request timed out — will retry on next schedule.")
except requests.exceptions.HTTPError as e:
    print(f"✗ HTTP error: {e}")
except Exception as e:
    print(f"✗ Unexpected error: {e}")
    raise
