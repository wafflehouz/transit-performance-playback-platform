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
# MAGIC
# MAGIC Polls every `POLL_INTERVAL_SECONDS` for `MAX_RUNTIME_MINUTES` minutes then
# MAGIC exits cleanly. Schedule this job hourly — it self-terminates before the next
# MAGIC trigger so runs never overlap.

# COMMAND ----------

import time

MAX_RUNTIME_MINUTES = 58   # 58 min: leaves ~2 min gap for job startup on the next trigger
api_key    = get_api_key()
start_time = time.time()
run_count  = 0

print(f"Poller starting — will run for {MAX_RUNTIME_MINUTES} min at {POLL_INTERVAL_SECONDS}s intervals\n")

while (time.time() - start_time) < (MAX_RUNTIME_MINUTES * 60):
    snapshot_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    run_count  += 1

    try:
        data        = poll_trip_updates(api_key)
        entity_cnt  = len([e for e in data.get("entity", []) if "tripUpdate" in e])
        feed_ts_raw = data.get("header", {}).get("timestamp", "unknown")
        file_path   = write_snapshot(data, snapshot_ts)

        if file_path:
            file_size = os.path.getsize(file_path)
            print(f"[{snapshot_ts.strftime('%H:%M:%S')}] #{run_count:>3} ✓ {entity_cnt} entities ({file_size:,} bytes)")
        else:
            print(f"[{snapshot_ts.strftime('%H:%M:%S')}] #{run_count:>3} Nothing written.")

    except requests.exceptions.Timeout:
        print(f"[{snapshot_ts.strftime('%H:%M:%S')}] #{run_count:>3} ✗ Timeout — skipping.")
    except requests.exceptions.HTTPError as e:
        print(f"[{snapshot_ts.strftime('%H:%M:%S')}] #{run_count:>3} ✗ HTTP error: {e}")
    except Exception as e:
        print(f"[{snapshot_ts.strftime('%H:%M:%S')}] #{run_count:>3} ✗ Unexpected error: {e}")
        raise

    # Skip the final sleep if the next wake-up would fall outside the run window —
    # avoids a wasted 60s wait after the last poll before the loop condition is checked.
    elapsed = time.time() - start_time
    if elapsed + POLL_INTERVAL_SECONDS < MAX_RUNTIME_MINUTES * 60:
        time.sleep(POLL_INTERVAL_SECONDS)

print(f"\nPoller done — {run_count} snapshots written in {(time.time()-start_time)/60:.1f} min")
