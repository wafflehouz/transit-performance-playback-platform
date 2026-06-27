# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 60 — Rail Ground Truth Loader
# MAGIC
# MAGIC **Run manually** after uploading new survey JSON files to the ground_truth Volume.
# MAGIC **Not part of the scheduled pipeline** — the nightly workflows are untouched.
# MAGIC Safe to re-run; the MERGE is idempotent on `(survey_filename, stop_id, stop_sequence)`.
# MAGIC
# MAGIC ## Staging flow
# MAGIC 1. Record a trip survey in the iOS ground truth app
# MAGIC 2. Export via Share Sheet → AirDrop to local machine
# MAGIC 3. Upload the `.json` file to:
# MAGIC    `{BASE_PATH}/ground_truth/`
# MAGIC    (Databricks UI → Catalog → Volumes, or `databricks fs cp` CLI)
# MAGIC 4. Run this notebook
# MAGIC 5. Run notebook 61 to compare against pipeline output
# MAGIC
# MAGIC ## GTFS trip_id resolution
# MAGIC The iOS app stores a UUID as `trip.trip_id` (not a GTFS ID).  The actual GTFS
# MAGIC trip_id is resolved from the `feed_snapshots` embedded in each survey file:
# MAGIC each snapshot is a full GTFS-RT TripUpdates feed captured at ~20s intervals.
# MAGIC We score every candidate trip by comparing its `stopTimeUpdate.arrival.time`
# MAGIC values against the surveyed DOOR_OPEN timestamps; the trip with the lowest
# MAGIC mean absolute difference wins.  Requires ≥ 2 overlapping stops.

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

import json
from collections import defaultdict
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType,
)
from delta.tables import DeltaTable

# COMMAND ----------

GROUND_TRUTH_TABLE = f"{CATALOG}.{SCHEMA}.gold_rail_ground_truth"
MIN_OVERLAP_STOPS  = 2   # minimum common stops required to accept a trip_id match

# COMMAND ----------

# MAGIC %md ## Step 1 — Discover survey files

# COMMAND ----------

try:
    file_infos = dbutils.fs.ls(GROUND_TRUTH_PATH)
    # dbutils.fs.ls returns paths prefixed with "dbfs:" but Python's open()
    # needs a POSIX path. For Unity Catalog Volumes, strip "dbfs:" → "/Volumes/..."
    json_files = [
        f.path.replace("dbfs:/Volumes", "/Volumes") if f.path.startswith("dbfs:/Volumes") else f.path
        for f in file_infos if f.name.endswith(".json")
    ]
except Exception as exc:
    raise RuntimeError(
        f"Cannot list {GROUND_TRUTH_PATH}: {exc}\n"
        "Upload survey JSON files to that Volume path before running this notebook."
    )

if not json_files:
    raise RuntimeError(
        f"No .json files found at {GROUND_TRUTH_PATH}.\n"
        "Upload at least one survey export and re-run."
    )

print(f"Found {len(json_files)} survey file(s):")
for p in json_files:
    print(f"  {p}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Parse each survey and build rows

# COMMAND ----------

def _resolve_gtfs_trip_id(
    stop_open_times: dict,   # {stop_id: door_open_epoch_s}
    feed_snapshots: list,
    route_id: str,
    direction_id: int,
) -> str | None:
    """
    Score every GTFS trip that appears in the embedded feed snapshots by comparing
    its predicted arrival times against the surveyed DOOR_OPEN timestamps.
    Returns the trip_id with the lowest mean absolute timing difference,
    or None if no trip has enough overlapping stops.
    """
    # Accumulate {trip_id: {stop_id: first_seen_arrival_s}} across all snapshots
    trip_arrivals: dict[str, dict[str, int]] = defaultdict(dict)

    for snap in feed_snapshots:
        if snap.get("feed_type") != "TRIP_UPDATES":
            continue
        raw = snap.get("raw_json")
        feed = json.loads(raw) if isinstance(raw, str) else raw
        for ent in feed.get("entity", []):
            tu = ent.get("tripUpdate", {})
            trip = tu.get("trip", {})
            if (
                str(trip.get("routeId", "")).strip() != str(route_id).strip()
                or int(trip.get("directionId", -1)) != direction_id
            ):
                continue
            tid = trip.get("tripId")
            if not tid:
                continue
            for stu in tu.get("stopTimeUpdate", []):
                sid = stu.get("stopId")
                arr = stu.get("arrival", {}).get("time")
                if sid and arr and sid not in trip_arrivals[tid]:
                    trip_arrivals[tid][sid] = int(arr)

    if not trip_arrivals:
        return None

    # Score each trip
    best_tid, best_score = None, float("inf")
    for tid, stops in trip_arrivals.items():
        overlapping = [(sid, ts) for sid, ts in stops.items() if sid in stop_open_times]
        if len(overlapping) < MIN_OVERLAP_STOPS:
            continue
        mean_abs_diff = sum(
            abs(arr_s - stop_open_times[sid]) for sid, arr_s in overlapping
        ) / len(overlapping)
        if mean_abs_diff < best_score:
            best_score = mean_abs_diff
            best_tid = tid

    return best_tid


def _parse_survey(path: str) -> list[dict]:
    """
    Parse a single ground truth survey JSON and return a list of row dicts,
    one per deduplicated DOOR_OPEN event.
    """
    filename = path.split("/")[-1]

    with open(path, "r") as fh:
        data = json.load(fh)

    trip_meta   = data.get("trip", {})
    stop_events = data.get("stop_events", [])
    snapshots   = data.get("feed_snapshots", [])

    route_id     = trip_meta.get("route_id", "")
    direction_id = int(trip_meta.get("direction_id", -1))
    started_at   = trip_meta.get("started_at")
    service_date = (
        datetime.fromtimestamp(started_at / 1000, tz=timezone.utc).date()
        if started_at else None
    )

    # Group DOOR_OPEN and DOOR_CLOSE events by (stop_sequence, stop_id)
    # The app may record multiple DOOR_OPEN events per stop (e.g., an early tap
    # when approaching vs. the actual door open). We keep the LAST DOOR_OPEN per
    # (stop_sequence, stop_id) because it best represents actual door opening.
    opens_by_key:  dict[tuple, list] = defaultdict(list)
    closes_by_key: dict[tuple, list] = defaultdict(list)

    for ev in stop_events:
        key = (ev.get("stop_sequence"), ev.get("stop_id"))
        if ev.get("event_type") == "DOOR_OPEN":
            opens_by_key[key].append(ev)
        elif ev.get("event_type") == "DOOR_CLOSE":
            closes_by_key[key].append(ev)

    # Build {stop_id: last_open_epoch_s} for trip_id resolution
    stop_open_times: dict[str, int] = {}
    for (seq, sid), evs in opens_by_key.items():
        last_ev = max(evs, key=lambda e: e["timestamp"])
        stop_open_times[sid] = last_ev["timestamp"] // 1000

    gtfs_trip_id = _resolve_gtfs_trip_id(stop_open_times, snapshots, route_id, direction_id)
    print(
        f"  route={route_id} dir={direction_id} service_date={service_date} "
        f"gtfs_trip_id={gtfs_trip_id} feed_snapshots={len(snapshots)}"
    )

    rows = []
    for (seq, sid), open_evs in opens_by_key.items():
        open_ev  = max(open_evs, key=lambda e: e["timestamp"])
        open_ms  = open_ev["timestamp"]

        # First DOOR_CLOSE after this DOOR_OPEN, for the same (stop_sequence, stop_id)
        close_evs = sorted(closes_by_key.get((seq, sid), []), key=lambda e: e["timestamp"])
        close_ev  = next((e for e in close_evs if e["timestamp"] > open_ms), None)
        close_ms  = close_ev["timestamp"] if close_ev else None

        dwell_sec = round((close_ms - open_ms) / 1000) if close_ms else None

        rows.append({
            "survey_filename":      filename,
            "service_date":         service_date,
            "route_id":             route_id,
            "direction_id":         direction_id,
            "gtfs_trip_id":         gtfs_trip_id,
            "stop_id":              sid,
            "stop_sequence":        seq,
            "actual_door_open_ts":  datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc),
            "actual_door_close_ts": datetime.fromtimestamp(close_ms / 1000, tz=timezone.utc) if close_ms else None,
            "dwell_actual_seconds": dwell_sec,
            "gps_accuracy_m":       open_ev.get("gps_accuracy_m"),
            "survey_lat":           open_ev.get("lat"),
            "survey_lon":           open_ev.get("lon"),
        })

    return rows

# COMMAND ----------

all_rows = []

for dbfs_path in json_files:
    filename = dbfs_path.split("/")[-1]
    print(f"\nProcessing: {filename}")
    rows = _parse_survey(dbfs_path)
    print(f"  {len(rows)} DOOR_OPEN rows extracted")
    all_rows.extend(rows)

print(f"\nTotal rows across all files: {len(all_rows)}")

if not all_rows:
    raise RuntimeError("No DOOR_OPEN events found in any survey file.")

# COMMAND ----------

# MAGIC %md ## Step 3 — Build Spark DataFrame

# COMMAND ----------

schema = StructType([
    StructField("survey_filename",      StringType(),    False),
    StructField("service_date",         DateType(),      True),
    StructField("route_id",             StringType(),    True),
    StructField("direction_id",         IntegerType(),   True),
    StructField("gtfs_trip_id",         StringType(),    True),
    StructField("stop_id",              StringType(),    True),
    StructField("stop_sequence",        IntegerType(),   True),
    StructField("actual_door_open_ts",  TimestampType(), True),
    StructField("actual_door_close_ts", TimestampType(), True),
    StructField("dwell_actual_seconds", IntegerType(),   True),
    StructField("gps_accuracy_m",       DoubleType(),    True),
    StructField("survey_lat",           DoubleType(),    True),
    StructField("survey_lon",           DoubleType(),    True),
])

new_df = spark.createDataFrame(all_rows, schema=schema)
display(new_df.orderBy("survey_filename", "stop_sequence"))

# COMMAND ----------

# MAGIC %md ## Step 4 — Create table if needed and MERGE

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GROUND_TRUTH_TABLE} (
        survey_filename       STRING     NOT NULL,
        service_date          DATE,
        route_id              STRING,
        direction_id          INT,
        gtfs_trip_id          STRING,
        stop_id               STRING,
        stop_sequence         INT,
        actual_door_open_ts   TIMESTAMP,
        actual_door_close_ts  TIMESTAMP,
        dwell_actual_seconds  INT,
        gps_accuracy_m        DOUBLE,
        survey_lat            DOUBLE,
        survey_lon            DOUBLE
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

dt = DeltaTable.forName(spark, GROUND_TRUTH_TABLE)

(
    dt.alias("tgt")
    .merge(
        new_df.alias("src"),
        """tgt.survey_filename = src.survey_filename
           AND tgt.stop_id       = src.stop_id
           AND tgt.stop_sequence = src.stop_sequence""",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# MAGIC %md ## Step 5 — Verify

# COMMAND ----------

summary = spark.sql(f"""
    SELECT
        service_date,
        route_id,
        direction_id,
        gtfs_trip_id,
        COUNT(*)                           AS stops_surveyed,
        MIN(actual_door_open_ts)           AS first_stop_ts,
        MAX(actual_door_open_ts)           AS last_stop_ts,
        ROUND(AVG(dwell_actual_seconds), 1) AS avg_dwell_s,
        SUM(CASE WHEN gtfs_trip_id IS NULL THEN 1 ELSE 0 END) AS unresolved_trip_id
    FROM {GROUND_TRUTH_TABLE}
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3
""")
display(summary)

total = spark.table(GROUND_TRUTH_TABLE).count()
print(f"\nTotal rows in {GROUND_TRUTH_TABLE}: {total}")
