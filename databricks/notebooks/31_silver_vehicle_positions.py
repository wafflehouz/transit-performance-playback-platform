# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 31 — Silver: Vehicle Positions (Nightly Batch)
# MAGIC
# MAGIC **Schedule:** Nightly
# MAGIC
# MAGIC **What it does:**
# MAGIC - Reads `bronze_vehicle_position_events` for the target service_date
# MAGIC - Downsamples to 1 row per (vehicle_id, 30-second bucket), keeping the latest
# MAGIC   vehicle-reported position within each window
# MAGIC - Writes to `silver_fact_vehicle_positions` (replaceWhere — safe to re-run)
# MAGIC
# MAGIC **Why time-based downsampling (not state-change dedup):**
# MAGIC A stationary vehicle at a stop would have an identical hash every 60 seconds.
# MAGIC State-change dedup would collapse the entire dwell to a single row, breaking
# MAGIC playback map animation and segment speed calculations. We need a position point
# MAGIC every ~30 seconds regardless of movement.
# MAGIC
# MAGIC **Bucket size:** `VP_DOWNSAMPLE_SECONDS` (defined in pipeline_config.py, default 30)
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `target_date`: ISO date string (YYYY-MM-DD). Defaults to yesterday.

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import date, timedelta

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("target_date", (date.today() - timedelta(days=1)).isoformat())
target_date = dbutils.widgets.get("target_date")
print(f"Processing service_date = {target_date}")
print(f"Downsample bucket: {VP_DOWNSAMPLE_SECONDS}s")

# COMMAND ----------

# MAGIC %md ## Step 1 — Read Bronze

# COMMAND ----------

bronze_df = (
    spark.table(BRONZE_VEHICLE_POSITIONS)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

bronze_count = bronze_df.count()
print(f"Bronze VP rows for {target_date}: {bronze_count:,}")

if bronze_count == 0:
    print("⚠ No Bronze VP data for this date. Is the poller running? Exiting.")
    dbutils.notebook.exit(f"No data for {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Compute Time Buckets and Downsample
# MAGIC
# MAGIC Floor each vehicle_ts to the nearest 30-second boundary, then keep the
# MAGIC latest reading within each (vehicle_id, bucket) window.

# COMMAND ----------

# Compute the 30-second floor bucket
bucketed = bronze_df.withColumn(
    "ts_bucket",
    (
        F.floor(F.unix_timestamp("vehicle_ts") / VP_DOWNSAMPLE_SECONDS)
        * VP_DOWNSAMPLE_SECONDS
    ).cast("timestamp")
)

# Within each (vehicle_id, bucket), keep the row with the latest vehicle_ts
# Desc order → row_number == 1 is the most current reading in that window
w_bucket = Window.partitionBy("vehicle_id", "ts_bucket").orderBy(F.desc("vehicle_ts"))

silver_df = (
    bucketed
    .withColumn("rn", F.row_number().over(w_bucket))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

# MAGIC %md ## Step 3 — Select Final Silver Schema

# COMMAND ----------

silver_out = silver_df.select(
    "service_date",                               # partition column
    F.col("ts_bucket").alias("event_ts"),         # canonical position timestamp
    "vehicle_id",
    "trip_id",
    "route_id",
    "direction_id",
    "lat",
    "lon",
    "bearing",
    "speed_mps",
    "current_stop_sequence",
    "current_status",
    "stop_id",
    "vehicle_ts",                                 # original vehicle-reported timestamp
    "ingest_ts",
)

# COMMAND ----------

# MAGIC %md ## Step 4 — Create Table and Write

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_FACT_VEHICLE_POSITIONS} (
        service_date          DATE,
        event_ts              TIMESTAMP,
        vehicle_id            STRING,
        trip_id               STRING,
        route_id              STRING,
        direction_id          INT,
        lat                   DOUBLE,
        lon                   DOUBLE,
        bearing               DOUBLE,
        speed_mps             DOUBLE,
        current_stop_sequence INT,
        current_status        STRING,
        stop_id               STRING,
        vehicle_ts            TIMESTAMP,
        ingest_ts             TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

(
    silver_out
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"service_date = '{target_date}'")
    .option("overwriteSchema", "false")
    .saveAsTable(SILVER_FACT_VEHICLE_POSITIONS)
)

print(f"✓ Written to {SILVER_FACT_VEHICLE_POSITIONS} for service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 5 — Quality Report

# COMMAND ----------

written = (
    spark.table(SILVER_FACT_VEHICLE_POSITIONS)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

silver_count  = written.count()
distinct_vehs = written.select("vehicle_id").distinct().count()
reduction_pct = (bronze_count - silver_count) / bronze_count * 100
avg_per_veh   = silver_count / distinct_vehs if distinct_vehs > 0 else 0
max_buckets   = (24 * 3600) / VP_DOWNSAMPLE_SECONDS

print(f"""
=== Silver Vehicle Positions: {target_date} ===
  Bronze rows in        : {bronze_count:>10,}
  Silver rows out       : {silver_count:>10,}
  Reduction             : {reduction_pct:>9.1f}%
  Distinct vehicles     : {distinct_vehs:>10,}
  Avg rows per vehicle  : {avg_per_veh:>10.1f}
  Max possible/day      : {max_buckets:>10.0f}  (full-day vehicle at {VP_DOWNSAMPLE_SECONDS}s)
""")

print("Speed stats (m/s) for moving vehicles:")
(
    written
    .filter(F.col("speed_mps") > 0)
    .select(
        F.min("speed_mps").alias("min"),
        F.percentile_approx("speed_mps", 0.5).alias("p50"),
        F.percentile_approx("speed_mps", 0.9).alias("p90"),
        F.max("speed_mps").alias("max"),
        (F.max("speed_mps") * 2.23694).alias("max_mph"),
    )
    .show()
)

print("Coverage by route (top 15):")
(
    written
    .groupBy("route_id")
    .agg(
        F.countDistinct("vehicle_id").alias("vehicles"),
        F.count("*").alias("position_rows"),
    )
    .orderBy(F.desc("vehicles"))
    .limit(15)
    .show()
)
