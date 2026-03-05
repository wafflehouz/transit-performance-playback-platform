# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 42 — Gold: Route Metrics 15-Min (Nightly Batch)
# MAGIC
# MAGIC **Schedule:** Nightly, after notebook 41 completes
# MAGIC **Dependencies:** `gold_trip_timeline_fact`, `gold_stop_dwell_fact`
# MAGIC **Grain:** `(service_date, route_id, direction_id, time_bucket_15min)`
# MAGIC
# MAGIC **What it does:**
# MAGIC - Buckets trips into 15-minute windows based on actual_start_ts
# MAGIC - Aggregates trip count, avg/p90 delay, avg dwell delta, avg late start
# MAGIC - Only includes trips with confirmed actual data (data_quality_flag IS NULL)
# MAGIC - These metrics feed anomaly detection and the route grid UI

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

# MAGIC %md ## Step 1 — Read Gold Trip Timeline (confirmed trips only)

# COMMAND ----------

timeline = (
    spark.table(GOLD_TRIP_TIMELINE_FACT)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
    .filter(F.col("data_quality_flag").isNull())
    .filter(F.col("actual_start_ts").isNotNull())
)

trip_count = timeline.count()
print(f"Confirmed trips for {target_date}: {trip_count:,}")

if trip_count == 0:
    print("⚠ No confirmed trip data. Run notebook 41 first.")
    dbutils.notebook.exit(f"No data for {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Compute 15-Minute Bucket
# MAGIC
# MAGIC Floor `actual_start_ts` to the nearest 15-minute boundary.
# MAGIC 900 seconds = 15 minutes. Pattern mirrors the 30-second VP downsample in Silver.

# COMMAND ----------

bucketed = timeline.withColumn(
    "time_bucket_15min",
    F.to_timestamp(
        (F.floor(F.unix_timestamp("actual_start_ts") / 900) * 900).cast("long")
    )
)

# COMMAND ----------

# MAGIC %md ## Step 3 — Per-Trip Average Dwell Delta
# MAGIC
# MAGIC Read from stop_dwell_fact and compute avg dwell delta per trip, then join
# MAGIC to timeline so we can aggregate to route+bucket grain.

# COMMAND ----------

trip_dwell = (
    spark.table(GOLD_STOP_DWELL_FACT)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
    .filter(F.col("dwell_delta_seconds").isNotNull())
    .groupBy("trip_id")
    .agg(F.avg("dwell_delta_seconds").alias("trip_avg_dwell_delta"))
)

bucketed = bucketed.join(trip_dwell, on="trip_id", how="left")

# COMMAND ----------

# MAGIC %md ## Step 4 — Aggregate to Route + Direction + Bucket

# COMMAND ----------

metrics = (
    bucketed
    .groupBy("service_date", "route_id", "direction_id", "time_bucket_15min")
    .agg(
        F.count("trip_id").cast("int").alias("trip_count"),

        F.avg("total_trip_delay_seconds").alias("avg_delay_seconds"),

        F.percentile_approx(
            "total_trip_delay_seconds", 0.9, accuracy=100
        ).cast("double").alias("p90_delay_seconds"),

        F.avg("trip_avg_dwell_delta").alias("avg_dwell_delta_seconds"),

        F.avg("late_start_seconds").alias("avg_late_start_seconds"),

        # OTP: fraction of trips within FTA standard window (-60s early to +299s late)
        # NULL total_trip_delay_seconds rows are excluded from avg automatically.
        F.avg(
            F.when(
                F.col("total_trip_delay_seconds").between(-60, 299), F.lit(1)
            ).otherwise(F.lit(0))
        ).alias("pct_on_time"),
    )
)

# COMMAND ----------

# MAGIC %md ## Step 5 — Write

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_ROUTE_METRICS_15MIN} (
        service_date              DATE,
        route_id                  STRING,
        direction_id              INT,
        time_bucket_15min         TIMESTAMP,
        trip_count                INT,
        avg_delay_seconds         DOUBLE,
        p90_delay_seconds         DOUBLE,
        avg_dwell_delta_seconds   DOUBLE,
        avg_late_start_seconds    DOUBLE,
        pct_on_time               DOUBLE
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

# Add pct_on_time column to existing table if not already present.
try:
    spark.sql(f"ALTER TABLE {GOLD_ROUTE_METRICS_15MIN} ADD COLUMN pct_on_time DOUBLE")
    print("Added pct_on_time column to existing table.")
except Exception:
    pass  # column already exists

(
    metrics
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"service_date = '{target_date}'")
    .option("overwriteSchema", "false")
    .saveAsTable(GOLD_ROUTE_METRICS_15MIN)
)

print(f"✓ Written to {GOLD_ROUTE_METRICS_15MIN} for service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 6 — Quality Report

# COMMAND ----------

written = (
    spark.table(GOLD_ROUTE_METRICS_15MIN)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

bucket_count = written.count()
route_count  = written.select("route_id").distinct().count()

print(f"""
=== Gold Route Metrics 15-Min: {target_date} ===
  Route+direction+bucket rows : {bucket_count:>10,}
  Distinct routes             : {route_count:>10,}
""")

print("Delay by time of day (avg across all routes):")
(
    written
    .withColumn("hour", F.hour("time_bucket_15min"))
    .groupBy("hour")
    .agg(
        F.avg("avg_delay_seconds").alias("avg_delay"),
        F.avg("trip_count").alias("avg_trips"),
    )
    .orderBy("hour")
    .show(24)
)
