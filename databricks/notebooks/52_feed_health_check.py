# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 52 — Feed Health Monitor (Hourly)
# MAGIC
# MAGIC **Schedule:** Hourly, standalone job (`transit-feed-monitor`)
# MAGIC **Purpose:** Detects upstream GTFS-RT feed outages by comparing the current
# MAGIC live vehicle count against the number of trips scheduled to be active right now
# MAGIC according to the GTFS static schedule.
# MAGIC
# MAGIC **Checks performed:**
# MAGIC 1. Snapshot freshness — was a VP file written in the last 90 minutes?
# MAGIC 2. Live vehicle count — how many unique vehicles are in the latest snapshot?
# MAGIC 3. Expected trip count — how many GTFS trips should be active at this Phoenix time-of-day?
# MAGIC 4. Coverage ratio — live_vehicles / expected_trips; alert if below threshold
# MAGIC 5. Historical fallback — if GTFS calendar data is unavailable, compare against
# MAGIC    day-of-week average from silver_fact_vehicle_positions
# MAGIC
# MAGIC **Exit behaviour:**
# MAGIC - Feed healthy → exits with "HEALTHY" message
# MAGIC - Feed silent, thin, or stale → raises RuntimeError to trigger workflow failure notification

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timezone
import pytz

# COMMAND ----------

# MAGIC %md ## Step 1 — Widget Setup

# COMMAND ----------

# alert_ratio_threshold: raise if live_vehicles / expected_trips is below this value
dbutils.widgets.text("alert_ratio_threshold", "0.5")
# snapshot_max_age_minutes: raise if the most recent VP snapshot is older than this
dbutils.widgets.text("snapshot_max_age_minutes", "90")

alert_ratio_threshold    = float(dbutils.widgets.get("alert_ratio_threshold"))
snapshot_max_age_minutes = int(dbutils.widgets.get("snapshot_max_age_minutes"))

phoenix_tz  = pytz.timezone("America/Phoenix")
now_phoenix = datetime.now(tz=phoenix_tz)

print(f"Feed Health Monitor starting")
print(f"  Current Phoenix time      : {now_phoenix.strftime('%Y-%m-%d %H:%M:%S %Z')}")
print(f"  Alert ratio threshold     : {alert_ratio_threshold}")
print(f"  Max snapshot age (min)    : {snapshot_max_age_minutes}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Snapshot Freshness Check

# COMMAND ----------

# Walk the VP snapshots volume to find the most recently modified file.
# The poller writes to dated subdirectories: .../vehicle_positions/YYYY/MM/DD/HH-MM-SS.ndjson
# We list the top-level directory and recurse into today's (and if needed yesterday's)
# date partition to find the newest file.

def _ls_recursive_latest(base_path: str, depth: int = 3):
    """
    Recursively list files up to `depth` levels under base_path and return
    the FileInfo with the largest modificationTime.
    Returns None if no files are found.
    """
    best = None
    try:
        entries = dbutils.fs.ls(base_path)
    except Exception:
        return None

    for entry in entries:
        if entry.isDir() if hasattr(entry, "isDir") else entry.size == 0:
            if depth > 0:
                candidate = _ls_recursive_latest(entry.path, depth - 1)
                if candidate and (best is None or candidate.modificationTime > best.modificationTime):
                    best = candidate
        else:
            if best is None or entry.modificationTime > best.modificationTime:
                best = entry
    return best

print("Scanning VP snapshot volume for the most recent file...")
latest_file_info = _ls_recursive_latest(VEHICLE_POSITIONS_SNAPSHOTS_PATH)

if latest_file_info is None:
    # No files at all — volume is empty or unreadable
    latest_file_path = None
    file_age_minutes = float("inf")
    freshness_status = "STALE"
    print("  WARNING: No snapshot files found in the VP volume.")
else:
    latest_file_path = latest_file_info.path
    latest_file_name = latest_file_path.split("/")[-1]

    now_epoch_ms     = datetime.now(tz=timezone.utc).timestamp() * 1000
    file_age_ms      = now_epoch_ms - latest_file_info.modificationTime
    file_age_minutes = file_age_ms / 60_000

    if file_age_minutes > snapshot_max_age_minutes:
        freshness_status = "STALE"
    else:
        freshness_status = "OK"

    print(f"  Latest file  : {latest_file_name}")
    print(f"  File age     : {file_age_minutes:.1f} min")
    print(f"  Status       : {freshness_status}")

# COMMAND ----------

# MAGIC %md ## Step 3 — Parse Latest VP Snapshot for Live Vehicle Count

# COMMAND ----------

# Each snapshot file is NDJSON where every line is one vehicle record written by the
# VP poller (notebook 11).  Schema (from 21_bronze_vehicle_positions):
#   { "feed_ts": "...", "snapshot_ts": "...", "id": "...", "vehicle": { ... } }
# The vehicle_id lives at vehicle.vehicle.id.  We read with spark.read.json which
# infers the schema across all lines, then count distinct vehicle.vehicle.id values.

live_vehicle_count = 0

if latest_file_path is None:
    print("  No snapshot file available — live vehicle count = 0.")
else:
    try:
        snap_df = spark.read.json(latest_file_path)

        # vehicle.vehicle.id is the unique vehicle identifier set by the poller
        if "vehicle" in snap_df.columns:
            live_vehicle_count = (
                snap_df
                .select(F.col("vehicle.vehicle.id").alias("vehicle_id"))
                .filter(F.col("vehicle_id").isNotNull())
                .distinct()
                .count()
            )
        else:
            # Fallback: count rows as a proxy (1 row = 1 vehicle position entity)
            live_vehicle_count = snap_df.count()

        print(f"  Snapshot path   : {latest_file_path}")
        print(f"  File age        : {file_age_minutes:.1f} min")
        print(f"  Vehicles in feed: {live_vehicle_count:,}")

    except Exception as e:
        print(f"  WARNING: Could not parse snapshot file ({e}) — live count = 0.")
        live_vehicle_count = 0

# COMMAND ----------

# MAGIC %md ## Step 4 — Compute Expected Active Trips from GTFS Static
# MAGIC
# MAGIC Determine how many scheduled trips should be running right now based on:
# MAGIC - Current Phoenix seconds-since-midnight
# MAGIC - Active service_ids for today (from silver_dim_calendar + silver_dim_calendar_dates)
# MAGIC - Per-trip min/max scheduled times from silver_fact_stop_schedule

# COMMAND ----------

from pyspark.sql.utils import AnalysisException

expected_trips_now  = None   # set to int if schedule-based count succeeds
used_fallback       = False  # set True if we fall through to historical fallback

# ── 4a: Current Phoenix seconds since midnight ────────────────────────────────
phoenix_midnight = now_phoenix.replace(hour=0, minute=0, second=0, microsecond=0)
current_phoenix_secs = int((now_phoenix - phoenix_midnight).total_seconds())

# ── 4b: Today's date string in YYYYMMDD format (matches GTFS calendar format) ─
today_str_yyyymmdd = now_phoenix.strftime("%Y%m%d")

# ── 4c: Day-of-week column name for silver_dim_calendar ───────────────────────
day_of_week_col = now_phoenix.strftime("%A").lower()   # e.g. "monday", "tuesday"

print(f"  Phoenix time now             : {now_phoenix.strftime('%H:%M:%S')} "
      f"({current_phoenix_secs:,} secs since midnight)")
print(f"  Today (YYYYMMDD)             : {today_str_yyyymmdd}")
print(f"  Day-of-week column           : {day_of_week_col}")

# ── 4d: Determine active service_ids ─────────────────────────────────────────
# Strategy A: calendar.txt + calendar_dates.txt (standard GTFS weekly schedule)
# Strategy B: calendar_dates.txt only (exception_type=1 for today) — used by
#             agencies like Valley Metro that publish explicit per-date service
#             rather than a recurring weekly pattern.
try:
    cal_dates_df = (
        spark.table(SILVER_DIM_CALENDAR_DATES)
        .filter(F.col("date") == today_str_yyyymmdd)
    )
    today_added   = cal_dates_df.filter(F.col("exception_type") == 1).select("service_id")
    today_removed = cal_dates_df.filter(F.col("exception_type") == 2).select("service_id")

    try:
        cal_df = spark.table(SILVER_DIM_CALENDAR)
        if cal_df.count() == 0:
            raise ValueError("silver_dim_calendar is empty")

        # Standard: weekly base + calendar_dates overrides
        base_active = (
            cal_df
            .filter(F.col(day_of_week_col) == 1)
            .filter(F.col("start_date") <= today_str_yyyymmdd)
            .filter(F.col("end_date")   >= today_str_yyyymmdd)
            .select("service_id")
        )
        active_service_ids = (
            base_active
            .union(today_added)
            .distinct()
            .join(today_removed, "service_id", "left_anti")
        )
        print("  Calendar source: calendar.txt + calendar_dates.txt")

    except (AnalysisException, ValueError):
        # Agency uses calendar_dates.txt only (e.g. Valley Metro)
        # exception_type=1 for today = service runs today
        active_service_ids = today_added.distinct()
        print("  Calendar source: calendar_dates.txt only (no calendar.txt)")

    active_count = active_service_ids.count()
    print(f"  Active service_ids today     : {active_count}")

    # ── 4e–4g: Join to trips, compute trip window, filter to currently-active ──
    dim_trip_df = spark.table(SILVER_DIM_TRIP).select("trip_id", "service_id")
    sched_df    = spark.table(SILVER_FACT_STOP_SCHEDULE).select(
        "trip_id", "scheduled_arrival_secs", "scheduled_departure_secs"
    )

    # Per-trip: earliest departure and latest arrival define the trip's active window
    trip_windows = (
        sched_df
        .groupBy("trip_id")
        .agg(
            F.min("scheduled_departure_secs").alias("min_dep_secs"),
            F.max("scheduled_arrival_secs").alias("max_arr_secs"),
        )
    )

    # Active now: trip window brackets the current Phoenix second
    active_trips_now = (
        active_service_ids
        .join(dim_trip_df, "service_id")
        .join(trip_windows, "trip_id")
        .filter(F.col("min_dep_secs") <= current_phoenix_secs)
        .filter(F.col("max_arr_secs") >= current_phoenix_secs)
    )

    expected_trips_now = active_trips_now.count()
    print(f"  Expected trips active now    : {expected_trips_now}")

except AnalysisException as e:
    print(f"  WARNING: silver_dim_calendar_dates not available ({e})")
    print("  Falling through to historical fallback (Step 5).")
    used_fallback = True
except ValueError as e:
    print(f"  WARNING: {e}")
    print("  Falling through to historical fallback (Step 5).")
    used_fallback = True
except Exception as e:
    print(f"  WARNING: Unexpected error computing expected trips ({e})")
    print("  Falling through to historical fallback (Step 5).")
    used_fallback = True

# COMMAND ----------

# MAGIC %md ## Step 5 — Historical Fallback (if Calendar Unavailable)
# MAGIC
# MAGIC If silver_dim_calendar could not be queried, estimate expected vehicle count from
# MAGIC the historical silver_fact_vehicle_positions table for the same day-of-week and hour.

# COMMAND ----------

ABSOLUTE_FLOOR_VEHICLES = 20   # used only if no historical data is available either

if expected_trips_now is None:
    used_fallback = True
    print("Running historical fallback — querying silver_fact_vehicle_positions...")

    current_phoenix_hour = now_phoenix.hour

    try:
        vp_df = spark.table(SILVER_FACT_VEHICLE_POSITIONS)

        # Filter to same day-of-week and same Phoenix hour across all available dates.
        # silver_fact_vehicle_positions has a service_date (date) and snapshot_ts (timestamp).
        # Derive Phoenix hour from snapshot_ts.
        historical_avg = (
            vp_df
            .withColumn(
                "phoenix_hour",
                F.hour(F.from_utc_timestamp(F.col("event_ts"), "America/Phoenix"))
            )
            .withColumn(
                "dow",
                F.dayofweek(
                    F.to_date(F.from_utc_timestamp(F.col("event_ts"), "America/Phoenix"))
                )
            )
            # dayofweek: 1=Sunday, 2=Monday, ..., 7=Saturday
            # now_phoenix.isoweekday(): 1=Monday ... 7=Sunday  →  convert
            .filter(F.col("phoenix_hour") == current_phoenix_hour)
            .filter(
                F.col("dow") == ((now_phoenix.isoweekday() % 7) + 1)
            )
            .groupBy("service_date", "phoenix_hour")
            .agg(F.countDistinct("vehicle_id").alias("distinct_vehicles"))
            .agg(F.avg("distinct_vehicles").alias("avg_vehicles"))
            .collect()
        )

        if historical_avg and historical_avg[0]["avg_vehicles"] is not None:
            expected_trips_now = int(round(historical_avg[0]["avg_vehicles"]))
            print(f"  Historical avg vehicles (same DOW, hour {current_phoenix_hour:02d}xx): "
                  f"{expected_trips_now}")
            print("  NOTE: This is a historical vehicle-count estimate, not a schedule-based trip count.")
        else:
            expected_trips_now = ABSOLUTE_FLOOR_VEHICLES
            print(f"  No historical data for this DOW/hour — using absolute floor: {ABSOLUTE_FLOOR_VEHICLES}")

    except (AnalysisException, Exception) as e:
        expected_trips_now = ABSOLUTE_FLOOR_VEHICLES
        print(f"  silver_fact_vehicle_positions not available ({e})")
        print(f"  Using absolute floor: {ABSOLUTE_FLOOR_VEHICLES}")

# COMMAND ----------

# MAGIC %md ## Step 6 — Coverage Ratio and Pass/Fail

# COMMAND ----------

# Guard against division by zero
if expected_trips_now == 0:
    coverage_ratio  = 1.0   # no trips expected → nothing to fail on
    coverage_status = "OK"
else:
    coverage_ratio = live_vehicle_count / expected_trips_now
    if coverage_ratio < alert_ratio_threshold:
        coverage_status = "BELOW_THRESHOLD"
    else:
        coverage_status = "OK"

coverage_pct = coverage_ratio * 100

# ── Build the health report ───────────────────────────────────────────────────
run_ts_str      = now_phoenix.strftime("%Y-%m-%d %H:%M Phoenix")
expected_source = "historical estimate" if used_fallback else "GTFS schedule"

print()
print("=" * 60)
print(f"  FEED HEALTH MONITOR — {run_ts_str}")
print("=" * 60)
print()
print("[ SNAPSHOT FRESHNESS ]")
if latest_file_info:
    print(f"  Latest file     : {latest_file_path.split('/')[-1]}")
    print(f"  File age        : {file_age_minutes:.0f} min")
else:
    print(f"  Latest file     : (none found)")
    print(f"  File age        : n/a")
print(f"  Status          : {freshness_status}")
print()
print("[ LIVE FEED ]")
print(f"  Vehicles in feed  : {live_vehicle_count:,}")
print(f"  Expected ({expected_source:<19}): {expected_trips_now:,}")
print(f"  Coverage ratio    : {coverage_ratio:.2f}  (threshold: {alert_ratio_threshold:.2f})")
print(f"  Status            : {coverage_status}")
print()
print("[ OVERALL ]")

# ── Collect all problems ──────────────────────────────────────────────────────
problems = []

if freshness_status == "STALE":
    if latest_file_info:
        problems.append(
            f"STALE snapshot: latest file is {file_age_minutes:.0f} min old "
            f"(threshold: {snapshot_max_age_minutes} min)"
        )
    else:
        problems.append("STALE: no VP snapshot files found in volume")

if coverage_status == "BELOW_THRESHOLD":
    problems.append(
        f"LOW COVERAGE: {live_vehicle_count}/{expected_trips_now} vehicles "
        f"({coverage_pct:.0f}%, threshold {alert_ratio_threshold*100:.0f}%)"
    )

if problems:
    summary_msg = (
        f"FEED UNHEALTHY — "
        + "; ".join(problems)
        + f" | as of {run_ts_str}"
    )
    print(f"  UNHEALTHY — {'; '.join(problems)}")
    print()
    print(f"[FAIL] {summary_msg}")
    raise RuntimeError(summary_msg)
else:
    summary_msg = (
        f"HEALTHY — feed active, {live_vehicle_count}/{expected_trips_now} vehicles "
        f"reporting ({coverage_pct:.0f}%) | {run_ts_str}"
    )
    print(f"  {summary_msg}")
    print()
    print(f"[OK] {summary_msg}")
    dbutils.notebook.exit(summary_msg)
