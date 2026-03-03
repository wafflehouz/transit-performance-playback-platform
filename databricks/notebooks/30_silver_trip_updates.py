# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 30 — Silver: Trip Updates (Nightly Batch)
# MAGIC
# MAGIC **Schedule:** Nightly (after Bronze has settled for the day)
# MAGIC
# MAGIC **What it does:**
# MAGIC - Reads `bronze_trip_updates_events` for the target service_date
# MAGIC - Deduplicates by detecting state changes: keeps only rows where `event_hash`
# MAGIC   changed from the previous snapshot for the same (trip_id, stop_sequence)
# MAGIC - Marks the last state-change row per (trip_id, stop_sequence) as `is_final`
# MAGIC - Writes to `silver_fact_trip_updates` (replaceWhere — safe to re-run)
# MAGIC
# MAGIC **Why state-change only (not all rows):**
# MAGIC Bronze accumulates ~1,440 snapshots per day. Most consecutive snapshots for a
# MAGIC given trip+stop are identical — the prediction hasn't changed. Silver retains
# MAGIC only the rows where something actually changed, reducing volume by ~95%+ while
# MAGIC preserving the full history of meaningful changes.
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

# COMMAND ----------

# MAGIC %md ## Step 1 — Read Bronze

# COMMAND ----------

bronze_df = (
    spark.table(BRONZE_TRIP_UPDATES)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

bronze_count = bronze_df.cache().count()
print(f"Bronze rows for {target_date}: {bronze_count:,}")

if bronze_count == 0:
    print("⚠ No Bronze data for this date. Is the poller running? Exiting.")
    dbutils.notebook.exit(f"No data for {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 2 — State-Change Deduplication
# MAGIC
# MAGIC For each (trip_id, stop_sequence) pair, ordered by feed_ts:
# MAGIC - The first row is always a state change (no previous observation)
# MAGIC - Subsequent rows are state changes only when event_hash differs from the prior row
# MAGIC - `is_final` marks the last state-change row (last known prediction for that stop)

# COMMAND ----------

# Window for lag() — ordered by feed_ts ascending within each trip+stop
w_asc = Window.partitionBy("trip_id", "stop_sequence").orderBy("feed_ts")

# Step 1: detect state changes
deduped = (
    bronze_df
    .withColumn("prev_hash", F.lag("event_hash").over(w_asc))
    .filter(
        F.col("prev_hash").isNull() |            # first observation for this trip+stop
        (F.col("event_hash") != F.col("prev_hash"))  # hash changed
    )
    .drop("prev_hash")
)

# Step 2: mark is_final on the now-filtered state-change rows
# (last state change = last meaningful prediction for that stop)
w_final = Window.partitionBy("trip_id", "stop_sequence").orderBy(F.desc("feed_ts"))

silver_df = (
    deduped
    .withColumn("rn_desc", F.row_number().over(w_final))
    .withColumn("is_final", F.col("rn_desc") == 1)
    .drop("rn_desc")
)

# COMMAND ----------

# MAGIC %md ## Step 3 — Select Final Silver Schema

# COMMAND ----------

silver_out = silver_df.select(
    "service_date",                   # partition column
    "feed_ts",
    "trip_id",
    "route_id",
    "direction_id",
    "vehicle_id",
    "stop_sequence",
    "stop_id",
    "stop_schedule_relationship",
    "arrival_delay_seconds",
    "arrival_ts",
    "departure_delay_seconds",
    "departure_ts",
    "trip_schedule_relationship",
    "is_final",
    "event_hash",
    "ingest_ts",                      # kept for feed latency auditing
)

# COMMAND ----------

# MAGIC %md ## Step 4 — Create Table and Write

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_FACT_TRIP_UPDATES} (
        service_date                 DATE,
        feed_ts                      TIMESTAMP,
        trip_id                      STRING,
        route_id                     STRING,
        direction_id                 INT,
        vehicle_id                   STRING,
        stop_sequence                INT,
        stop_id                      STRING,
        stop_schedule_relationship   STRING,
        arrival_delay_seconds        INT,
        arrival_ts                   TIMESTAMP,
        departure_delay_seconds      INT,
        departure_ts                 TIMESTAMP,
        trip_schedule_relationship   STRING,
        is_final                     BOOLEAN,
        event_hash                   STRING,
        ingest_ts                    TIMESTAMP
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
    .saveAsTable(SILVER_FACT_TRIP_UPDATES)
)

print(f"✓ Written to {SILVER_FACT_TRIP_UPDATES} for service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 5 — Quality Report

# COMMAND ----------

written = (
    spark.table(SILVER_FACT_TRIP_UPDATES)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

silver_count   = written.count()
is_final_cnt   = written.filter(F.col("is_final")).count()
distinct_trips = written.select("trip_id").distinct().count()
distinct_pairs = written.select("trip_id", "stop_sequence").distinct().count()
reduction_pct  = (bronze_count - silver_count) / bronze_count * 100

print(f"""
=== Silver Trip Updates: {target_date} ===
  Bronze rows in          : {bronze_count:>10,}
  Silver rows out         : {silver_count:>10,}
  Reduction               : {reduction_pct:>9.1f}%
  is_final rows           : {is_final_cnt:>10,}
  Distinct trips          : {distinct_trips:>10,}
  Distinct (trip, stop)   : {distinct_pairs:>10,}

  is_final == distinct pairs: {'✓ OK' if is_final_cnt == distinct_pairs else '✗ MISMATCH — investigate'}
""")

print("Delay distribution (is_final rows, seconds):")
(
    written
    .filter(F.col("is_final") & F.col("arrival_delay_seconds").isNotNull())
    .select(
        F.min("arrival_delay_seconds").alias("min"),
        F.percentile_approx("arrival_delay_seconds", 0.25).alias("p25"),
        F.percentile_approx("arrival_delay_seconds", 0.5).alias("p50"),
        F.percentile_approx("arrival_delay_seconds", 0.75).alias("p75"),
        F.percentile_approx("arrival_delay_seconds", 0.9).alias("p90"),
        F.max("arrival_delay_seconds").alias("max"),
    )
    .show()
)
