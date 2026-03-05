# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 44 — Gold: Stop Dwell Inferred (Nightly Batch)
# MAGIC
# MAGIC **Schedule:** Nightly, parallel with notebooks 42 and 43 (after 41)
# MAGIC **Dependencies:** `silver_fact_vehicle_positions`, `silver_fact_stop_schedule`, `silver_dim_stop`
# MAGIC **Grain:** `(service_date, trip_id, stop_sequence)`
# MAGIC
# MAGIC **What it does:**
# MAGIC - Infers actual stop dwell from vehicle trajectory instead of GTFS-RT predictions
# MAGIC - Detects when a vehicle is stationary (speed < 1 m/s) within 40m of a scheduled stop
# MAGIC - Each 30-second VP bucket near a stop contributes 30 seconds of dwell
# MAGIC - Produces `inferred_arrival_ts`, `inferred_departure_ts`, and `dwell_seconds`
# MAGIC
# MAGIC **Why this exists:**
# MAGIC GTFS-RT TripUpdates report predicted arrival/departure times, not actual door events.
# MAGIC For many stops, `scheduled_arrival = scheduled_departure` (0s planned dwell), so
# MAGIC notebook 40's delta is near-zero even when buses sit for minutes. This notebook
# MAGIC reconstructs dwell from physical position + speed evidence.
# MAGIC
# MAGIC **Accuracy:** ±30 seconds (one VP bucket). Detects dwells ≥ 30 seconds reliably.
# MAGIC
# MAGIC **Key parameters (tunable in pipeline_config):**
# MAGIC - `VP_STOP_RADIUS_METERS = 40` — haversine match radius
# MAGIC - `VP_STATIONARY_THRESHOLD_MPS = 1.0` — speed cutoff for "stopped"
# MAGIC - `VP_DOWNSAMPLE_SECONDS = 30` — bucket size used for dwell calculation

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import date, timedelta

# COMMAND ----------

dbutils.widgets.text("target_date", (date.today() - timedelta(days=1)).isoformat())
target_date = dbutils.widgets.get("target_date")
print(f"Processing service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 1 — Read Stationary VP Points
# MAGIC
# MAGIC Filter Silver VP to speed ≤ threshold before the join — dramatically reduces
# MAGIC the number of rows that need haversine computation.

# COMMAND ----------

stationary_vp = (
    spark.table(SILVER_FACT_VEHICLE_POSITIONS)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
    .filter(
        F.col("speed_mps").isNull() |
        (F.col("speed_mps") <= VP_STATIONARY_THRESHOLD_MPS)
    )
    .select(
        "service_date", "trip_id", "route_id", "direction_id",
        "event_ts", "lat", "lon", "speed_mps",
    )
)

vp_count = stationary_vp.count()
print(f"Stationary VP points for {target_date}: {vp_count:,}")

if vp_count == 0:
    print("⚠ No stationary VP data. Run notebook 31 first.")
    dbutils.notebook.exit(f"No data for {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Get Stop Positions for Trips in VP Data
# MAGIC
# MAGIC Limit stop_schedule to trips we actually have VP data for. This avoids
# MAGIC joining all schedule stops against all VP points (would be enormous).

# COMMAND ----------

trip_ids = stationary_vp.select("trip_id").distinct()

stop_schedule = (
    spark.table(SILVER_FACT_STOP_SCHEDULE)
    .join(trip_ids, on="trip_id", how="inner")
    .select("trip_id", "stop_sequence", "stop_id")
)

stop_dim = (
    spark.table(SILVER_DIM_STOP)
    .select(
        "stop_id",
        F.col("lat").alias("stop_lat"),
        F.col("lon").alias("stop_lon"),
    )
)

# COMMAND ----------

# MAGIC %md ## Step 3 — Haversine Join
# MAGIC
# MAGIC Strategy: bounding box pre-filter (cheap) then haversine (exact).
# MAGIC
# MAGIC At Phoenix latitude (~33.5°):
# MAGIC - 0.0005° latitude  ≈ 55 m  (generous margin over 40 m radius)
# MAGIC - 0.0005° longitude ≈ 46 m
# MAGIC
# MAGIC The bounding box eliminates the vast majority of (VP, stop) candidate pairs
# MAGIC before the more expensive haversine calculation runs.

# COMMAND ----------

_BBOX = 0.0005  # degrees, ~55m bounding box

def haversine_m(lat1, lon1, lat2, lon2):
    """Haversine distance in metres between two lat/lon column expressions."""
    dlat = F.radians(lat2) - F.radians(lat1)
    dlon = F.radians(lon2) - F.radians(lon1)
    a = (
        F.pow(F.sin(dlat / 2), 2)
        + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2))
        * F.pow(F.sin(dlon / 2), 2)
    )
    return F.lit(2 * 6_371_000) * F.asin(F.sqrt(a))

near_stop = (
    stationary_vp
    .join(stop_schedule, on="trip_id", how="inner")
    .join(stop_dim,      on="stop_id",  how="inner")
    # Bounding box: cheap lat/lon comparison before haversine
    .filter(F.abs(F.col("lat") - F.col("stop_lat")) < _BBOX)
    .filter(F.abs(F.col("lon") - F.col("stop_lon")) < _BBOX)
    # Exact haversine distance
    .withColumn(
        "distance_m",
        haversine_m(F.col("lat"), F.col("lon"), F.col("stop_lat"), F.col("stop_lon"))
    )
    .filter(F.col("distance_m") <= VP_STOP_RADIUS_METERS)
)

# COMMAND ----------

# MAGIC %md ## Step 4 — Aggregate to Trip-Stop Grain
# MAGIC
# MAGIC Each VP bucket within the radius contributes VP_DOWNSAMPLE_SECONDS of dwell.
# MAGIC min(event_ts) = inferred arrival, max(event_ts) = inferred departure.
# MAGIC
# MAGIC Note: a single VP point (point_count = 1) means the vehicle was observed
# MAGIC stationary near the stop for one 30-second window — dwell ≥ 30s but exact
# MAGIC duration unknown. Multiple points give better resolution.

# COMMAND ----------

dwell_inferred = (
    near_stop
    .groupBy(
        "service_date", "trip_id", "route_id", "direction_id",
        "stop_id", "stop_sequence",
    )
    .agg(
        F.count("*").cast("int").alias("point_count"),
        (F.count("*") * VP_DOWNSAMPLE_SECONDS).cast("int").alias("dwell_seconds"),
        F.min("event_ts").alias("inferred_arrival_ts"),
        F.max("event_ts").alias("inferred_departure_ts"),
        F.min("distance_m").alias("min_distance_m"),
    )
)

# COMMAND ----------

# MAGIC %md ## Step 5 — Write

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_STOP_DWELL_INFERRED} (
        service_date          DATE,
        trip_id               STRING,
        route_id              STRING,
        direction_id          INT,
        stop_id               STRING,
        stop_sequence         INT,
        point_count           INT,
        dwell_seconds         INT,
        inferred_arrival_ts   TIMESTAMP,
        inferred_departure_ts TIMESTAMP,
        min_distance_m        DOUBLE
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

(
    dwell_inferred
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"service_date = '{target_date}'")
    .option("overwriteSchema", "false")
    .saveAsTable(GOLD_STOP_DWELL_INFERRED)
)

print(f"✓ Written to {GOLD_STOP_DWELL_INFERRED} for service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 6 — Quality Report

# COMMAND ----------

written = (
    spark.table(GOLD_STOP_DWELL_INFERRED)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

total_events   = written.count()
distinct_trips = written.select("trip_id").distinct().count()
distinct_stops = written.select("stop_id").distinct().count()

print(f"""
=== Gold Stop Dwell Inferred: {target_date} ===
  Dwell events detected   : {total_events:>10,}
  Distinct trips          : {distinct_trips:>10,}
  Distinct stops          : {distinct_stops:>10,}
""")

print("Dwell distribution (seconds):")
written.select(
    F.min("dwell_seconds").alias("min"),
    F.percentile_approx("dwell_seconds", 0.5).alias("p50"),
    F.percentile_approx("dwell_seconds", 0.9).alias("p90"),
    F.max("dwell_seconds").alias("max"),
).show()

print("Top 10 stops by avg inferred dwell (≥ 5 observations):")
(
    written
    .groupBy("stop_id", "route_id")
    .agg(
        F.avg("dwell_seconds").alias("avg_dwell_secs"),
        F.count("*").alias("observations"),
    )
    .filter(F.col("observations") >= 5)
    .orderBy(F.desc("avg_dwell_secs"))
    .limit(10)
    .show()
)

print("Route 29 — 35th Ave area (stop proximity check):")
(
    written
    .filter(F.col("route_id") == "29")
    .join(
        spark.table(SILVER_DIM_STOP).select("stop_id", F.col("stop_name")),
        on="stop_id", how="left"
    )
    .filter(F.col("stop_name").contains("35"))
    .groupBy("stop_id", "stop_name", "direction_id")
    .agg(
        F.avg("dwell_seconds").alias("avg_dwell_secs"),
        F.max("dwell_seconds").alias("max_dwell_secs"),
        F.count("*").alias("observations"),
    )
    .orderBy(F.desc("avg_dwell_secs"))
    .show()
)
