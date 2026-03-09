# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 — GTFS Static Loader
# MAGIC
# MAGIC **Schedule:** Daily (run after midnight; Valley Metro publishes new feeds ~daily)
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Downloads the GTFS static ZIP from Phoenix Open Data
# MAGIC 2. Extracts and parses: routes, stops, trips, stop_times, calendar, shapes
# MAGIC 3. Writes/upserts Silver dimension tables
# MAGIC 4. Writes Silver `fact_stop_schedule` (scheduled arrival/departure per trip+stop)
# MAGIC
# MAGIC **Output tables:**
# MAGIC - `silver_dim_route`, `silver_dim_stop`, `silver_dim_trip`
# MAGIC - `silver_fact_stop_schedule`
# MAGIC
# MAGIC **Run time:** ~5–10 minutes (stop_times can be large)

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

import requests
import zipfile
import io
import os
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType
)

RUN_TS = datetime.now(timezone.utc).replace(tzinfo=None)
print(f"GTFS Static Loader starting at {RUN_TS} UTC")

# COMMAND ----------

# MAGIC %md ## Step 1 — Download GTFS Static ZIP

# COMMAND ----------

import json

static_zip_path   = f"{RAW_STATIC_PATH}/googletransit.zip"
feed_meta_path    = f"{RAW_STATIC_PATH}/feed_metadata.json"
req_headers       = {"User-Agent": "Mozilla/5.0 (compatible; transit-pipeline/1.0)"}

FORCE_RELOAD = False  # set True to re-download regardless of cache headers

def _remote_meta() -> dict:
    """HEAD request to get cache headers from the feed server."""
    try:
        r = requests.head(GTFS_STATIC_URL, headers=req_headers, timeout=15, allow_redirects=True)
        return {
            "etag":           r.headers.get("ETag", ""),
            "last_modified":  r.headers.get("Last-Modified", ""),
            "content_length": r.headers.get("Content-Length", ""),
        }
    except Exception as e:
        print(f"⚠ HEAD request failed ({e}) — will download unconditionally.")
        return {}

def _stored_meta() -> dict:
    if os.path.exists(feed_meta_path):
        with open(feed_meta_path) as f:
            return json.load(f)
    return {}

def _save_meta(meta: dict):
    with open(feed_meta_path, "w") as f:
        json.dump(meta, f, indent=2)

# ── Decide whether to download ────────────────────────────────────────────────
remote = _remote_meta()
stored = _stored_meta()
zip_exists = os.path.exists(static_zip_path)

needs_download = FORCE_RELOAD or not zip_exists
if not needs_download and remote:
    if remote.get("etag") and remote["etag"] != stored.get("etag"):
        needs_download = True
    elif remote.get("last_modified") and remote["last_modified"] != stored.get("last_modified"):
        needs_download = True
    elif not remote.get("etag") and not remote.get("last_modified"):
        needs_download = True   # server provides no cache signal — always refresh

if not needs_download:
    print(f"✓ Feed unchanged — skipping download.")
    print(f"  Last-Modified : {stored.get('last_modified') or 'n/a'}")
    print(f"  ETag          : {stored.get('etag') or 'n/a'}")
else:
    print("Downloading GTFS static feed...")
    resp = requests.get(GTFS_STATIC_URL, headers=req_headers, timeout=60)
    resp.raise_for_status()
    with open(static_zip_path, "wb") as f:
        f.write(resp.content)
    _save_meta(remote)
    size_mb = os.path.getsize(static_zip_path) / (1024 * 1024)
    print(f"✓ Downloaded ({size_mb:.1f} MB)")
    print(f"  Last-Modified : {remote.get('last_modified') or 'n/a'}")
    print(f"  ETag          : {remote.get('etag') or 'n/a'}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Extract to Volume

# COMMAND ----------

extract_dir = f"{RAW_STATIC_PATH}/extracted"

if needs_download or not os.path.exists(f"{extract_dir}/stops.txt"):
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(static_zip_path, "r") as zf:
        zf.extractall(extract_dir)
        extracted = zf.namelist()
    print(f"✓ Extracted {len(extracted)} files to {extract_dir}:")
    for f in sorted(extracted):
        size = os.path.getsize(f"{extract_dir}/{f}")
        print(f"  {f:35s}  {size:>10,} bytes")
else:
    print(f"✓ Using existing extracted files in {extract_dir}")

# COMMAND ----------

# MAGIC %md ## Step 3 — Load Dimensions

# COMMAND ----------

# Helper: return a column expression only if it exists in df, else null of given type
def safe_col(df, col_name: str, cast_type):
    if col_name in df.columns:
        return F.col(col_name).cast(cast_type).alias(col_name)
    return F.lit(None).cast(cast_type).alias(col_name)

# Helper: read a GTFS CSV file as a Spark DataFrame
def read_gtfs_csv(filename: str, schema=None):
    path = f"{extract_dir}/{filename}"
    if not os.path.exists(path):
        print(f"  ⚠ {filename} not found — skipping.")
        return None
    opts = {
        "header": "true",
        "inferSchema": "false" if schema else "true",
        "encoding": "UTF-8",
        "quote": '"',
        "escape": '"',
    }
    df = spark.read.format("csv").options(**opts).load(path)
    print(f"  ✓ {filename}: {df.count():,} rows, {len(df.columns)} cols")
    return df

# COMMAND ----------

# MAGIC %md ### 3a — silver_dim_route

# COMMAND ----------

print("Loading routes.txt...")
raw_routes = read_gtfs_csv("routes.txt")

dim_route = (
    raw_routes
    .select(
        F.col("route_id").cast(StringType()),
        F.col("agency_id").cast(StringType()),
        F.col("route_short_name").cast(StringType()),
        F.col("route_long_name").cast(StringType()),
        F.col("route_type").cast(IntegerType()),
        F.col("route_color").cast(StringType()),
        F.col("route_text_color").cast(StringType()),
    )
    .dropDuplicates(["route_id"])
    .withColumn("loaded_ts", F.lit(RUN_TS))
)

dim_route.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(SILVER_DIM_ROUTE)

print(f"✓ {SILVER_DIM_ROUTE}: {dim_route.count():,} routes")
dim_route.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ### 3b — silver_dim_stop

# COMMAND ----------

print("Loading stops.txt...")
raw_stops = read_gtfs_csv("stops.txt")

dim_stop = (
    raw_stops
    .select(
        F.col("stop_id").cast(StringType()),
        F.col("stop_name").cast(StringType()),
        F.col("stop_lat").cast(DoubleType()).alias("lat"),
        F.col("stop_lon").cast(DoubleType()).alias("lon"),
        safe_col(raw_stops, "parent_station",      StringType()),
        safe_col(raw_stops, "location_type",       IntegerType()),
        safe_col(raw_stops, "wheelchair_boarding",  IntegerType()),
    )
    .dropDuplicates(["stop_id"])
    .withColumn("loaded_ts", F.lit(RUN_TS))
)

dim_stop.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(SILVER_DIM_STOP)

print(f"✓ {SILVER_DIM_STOP}: {dim_stop.count():,} stops")
dim_stop.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ### 3c — silver_dim_trip

# COMMAND ----------

print("Loading trips.txt...")
raw_trips = read_gtfs_csv("trips.txt")

dim_trip = (
    raw_trips
    .select(
        F.col("trip_id").cast(StringType()),
        F.col("route_id").cast(StringType()),
        F.col("service_id").cast(StringType()),
        safe_col(raw_trips, "direction_id",         IntegerType()),
        safe_col(raw_trips, "shape_id",             StringType()),
        safe_col(raw_trips, "trip_headsign",        StringType()),
        safe_col(raw_trips, "trip_short_name",      StringType()),
        safe_col(raw_trips, "block_id",             StringType()),
        safe_col(raw_trips, "wheelchair_accessible", IntegerType()),
        safe_col(raw_trips, "bikes_allowed",        IntegerType()),
    )
    .dropDuplicates(["trip_id"])
    .withColumn("loaded_ts", F.lit(RUN_TS))
)

dim_trip.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(SILVER_DIM_TRIP)

print(f"✓ {SILVER_DIM_TRIP}: {dim_trip.count():,} trips")
dim_trip.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ## Step 4 — fact_stop_schedule
# MAGIC
# MAGIC GTFS `stop_times.txt` contains the scheduled arrival/departure for every
# MAGIC (trip, stop_sequence) pair. Times use GTFS notation (e.g. "25:30:00" for
# MAGIC 1:30 AM the next calendar day). We store raw strings and convert to seconds-
# MAGIC since-midnight for arithmetic in Gold.

# COMMAND ----------

def gtfs_time_to_seconds_udf(time_str):
    """Convert 'HH:MM:SS' (incl. >24h) to seconds since midnight."""
    if time_str is None:
        return None
    parts = time_str.strip().split(":")
    if len(parts) != 3:
        return None
    try:
        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        return h * 3600 + m * 60 + s
    except Exception:
        return None

from pyspark.sql.types import IntegerType as IntType
gtfs_time_udf = F.udf(gtfs_time_to_seconds_udf, IntType())

# COMMAND ----------

print("Loading stop_times.txt (this may take a minute)...")
raw_stop_times = read_gtfs_csv("stop_times.txt")

fact_stop_schedule = (
    raw_stop_times
    .select(
        F.col("trip_id").cast(StringType()),
        F.col("stop_id").cast(StringType()),
        F.col("stop_sequence").cast(IntegerType()),
        F.col("arrival_time").cast(StringType()).alias("scheduled_arrival_time"),
        F.col("departure_time").cast(StringType()).alias("scheduled_departure_time"),
        safe_col(raw_stop_times, "shape_dist_traveled", DoubleType()),
        safe_col(raw_stop_times, "timepoint",           IntegerType()),
        safe_col(raw_stop_times, "pickup_type",         IntegerType()),
        safe_col(raw_stop_times, "drop_off_type",       IntegerType()),
    )
    # Add seconds-since-midnight for schedule arithmetic in Gold
    .withColumn("scheduled_arrival_secs",   gtfs_time_udf(F.col("scheduled_arrival_time")))
    .withColumn("scheduled_departure_secs", gtfs_time_udf(F.col("scheduled_departure_time")))
    .withColumn("loaded_ts", F.lit(RUN_TS))
)

(
    fact_stop_schedule
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_FACT_STOP_SCHEDULE)
)

row_count = spark.table(SILVER_FACT_STOP_SCHEDULE).count()
print(f"✓ {SILVER_FACT_STOP_SCHEDULE}: {row_count:,} rows")
fact_stop_schedule.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md ## Step 4b — silver_dim_calendar

# COMMAND ----------

print("Loading calendar.txt...")
raw_calendar = read_gtfs_csv("calendar.txt")

if raw_calendar is None:
    print("  WARNING: calendar.txt not found — skipping silver_dim_calendar.")
else:
    dim_calendar = (
        raw_calendar
        .select(
            F.col("service_id").cast(StringType()),
            safe_col(raw_calendar, "monday",    IntegerType()),
            safe_col(raw_calendar, "tuesday",   IntegerType()),
            safe_col(raw_calendar, "wednesday", IntegerType()),
            safe_col(raw_calendar, "thursday",  IntegerType()),
            safe_col(raw_calendar, "friday",    IntegerType()),
            safe_col(raw_calendar, "saturday",  IntegerType()),
            safe_col(raw_calendar, "sunday",    IntegerType()),
            # start_date / end_date are YYYYMMDD strings — kept as STRING intentionally
            F.col("start_date").cast(StringType()),
            F.col("end_date").cast(StringType()),
        )
        .dropDuplicates(["service_id"])
        .withColumn("loaded_ts", F.lit(RUN_TS))
    )

    dim_calendar.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(SILVER_DIM_CALENDAR)

    print(f"✓ {SILVER_DIM_CALENDAR}: {dim_calendar.count():,} service IDs")
    dim_calendar.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ## Step 4c — silver_dim_calendar_dates

# COMMAND ----------

print("Loading calendar_dates.txt...")
raw_calendar_dates = read_gtfs_csv("calendar_dates.txt")

if raw_calendar_dates is None:
    print("  WARNING: calendar_dates.txt not found — skipping silver_dim_calendar_dates.")
else:
    # exception_type: 1 = service added for this date, 2 = service removed for this date
    dim_calendar_dates = (
        raw_calendar_dates
        .select(
            F.col("service_id").cast(StringType()),
            # date is YYYYMMDD string — kept as STRING intentionally (mirrors GTFS format)
            F.col("date").cast(StringType()),
            F.col("exception_type").cast(IntegerType()),
        )
        .dropDuplicates(["service_id", "date"])
        .withColumn("loaded_ts", F.lit(RUN_TS))
    )

    dim_calendar_dates.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(SILVER_DIM_CALENDAR_DATES)

    print(f"✓ {SILVER_DIM_CALENDAR_DATES}: {dim_calendar_dates.count():,} exception rows")
    dim_calendar_dates.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ## Step 5 — Sanity Checks

# COMMAND ----------

print("=== Sanity Checks ===\n")

route_count = spark.table(SILVER_DIM_ROUTE).count()
stop_count  = spark.table(SILVER_DIM_STOP).count()
trip_count  = spark.table(SILVER_DIM_TRIP).count()
stu_count   = spark.table(SILVER_FACT_STOP_SCHEDULE).count()

# Calendar tables are optional — may not exist if the feed didn't include them
try:
    calendar_count = spark.table(SILVER_DIM_CALENDAR).count()
    calendar_status = f"{calendar_count:>8,} rows"
except Exception:
    calendar_status = "        (not loaded)"

try:
    cal_dates_count = spark.table(SILVER_DIM_CALENDAR_DATES).count()
    cal_dates_status = f"{cal_dates_count:>8,} rows"
except Exception:
    cal_dates_status = "        (not loaded)"

print(f"  silver_dim_route          : {route_count:>8,} rows")
print(f"  silver_dim_stop           : {stop_count:>8,} rows")
print(f"  silver_dim_trip           : {trip_count:>8,} rows")
print(f"  silver_dim_calendar       : {calendar_status}")
print(f"  silver_dim_calendar_dates : {cal_dates_status}")
print(f"  silver_fact_stop_schedule : {stu_count:>8,} rows")

# Check referential integrity: trips in stop_times should exist in dim_trip
orphan_trips = (
    spark.table(SILVER_FACT_STOP_SCHEDULE)
    .select("trip_id").distinct()
    .join(spark.table(SILVER_DIM_TRIP).select("trip_id"), "trip_id", "left_anti")
    .count()
)
print(f"\n  Trip IDs in fact_stop_schedule with no dim_trip match: {orphan_trips}")

# Route type distribution
print("\nRoute type distribution:")
(
    spark.table(SILVER_DIM_ROUTE)
    .groupBy("route_type").count().orderBy("route_type")
    # 0=Tram/Rail, 3=Bus, 2=Rail
    .show()
)

print(f"\n✓ GTFS Static Loader complete at {datetime.now(timezone.utc)} UTC")
