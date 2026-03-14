# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 45 — Gold: Route Metrics Baseline (Nightly Batch)
# MAGIC
# MAGIC **Schedule:** Nightly, after notebook 42 completes
# MAGIC **Dependencies:** `gold_route_metrics_15min`
# MAGIC **Grain:** `(route_id, direction_id, service_type, bucket_hour, bucket_minute)`
# MAGIC
# MAGIC **What it does:**
# MAGIC - Reads the rolling 28-day window of `gold_route_metrics_15min`
# MAGIC - Groups by route + direction + service_type + time-of-day bucket
# MAGIC - Computes avg, stddev, p50, p90 of delay; avg OTP; deviation band percentages
# MAGIC - Fully overwrites the table each night (stateless — no partition needed)
# MAGIC
# MAGIC **Service type logic (mirrors Valley Metro's three service patterns):**
# MAGIC - `weekday`  : Monday–Friday  (up to 20 samples per 28-day window)
# MAGIC - `saturday` : Saturday       (up to 4 samples)
# MAGIC - `sunday`   : Sunday         (up to 4 samples)
# MAGIC
# MAGIC **Deviation bands (from CAD/AVL runtime analysis parameters):**
# MAGIC - `lt1min`   :   0–59 s — negligible
# MAGIC - `1to3min`  :  60–179 s — moderate
# MAGIC - `3to6min`  : 180–359 s — significant
# MAGIC - `gt6min`   :   ≥ 360 s — severe

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
print(f"Building baseline using 28-day window ending {target_date} (exclusive)")

# COMMAND ----------

# MAGIC %md ## Step 1 — Read 28-Day History
# MAGIC
# MAGIC Window is [target_date - 28, target_date) — excludes target_date itself so the
# MAGIC baseline is never contaminated by the night being analysed.

# COMMAND ----------

window_start = (date.fromisoformat(target_date) - timedelta(days=28)).isoformat()

# ── Check for recent GTFS schedule change and shrink window if needed ─────────
# If a schedule change was detected within the last 28 days, using pre-change
# data would corrupt the baseline (old + new schedule mixed → inflated stddev).
# Shrink window_start to the change date so only post-change data is used.
try:
    version_log = spark.table(GOLD_GTFS_VERSION_LOG)
    recent_reset = (
        version_log
        .filter(F.col("baseline_reset_recommended") == True)
        .filter(F.col("detected_date") > F.lit(window_start).cast("date"))
        .filter(F.col("detected_date") <  F.lit(target_date).cast("date"))
        .orderBy(F.col("detected_date").desc())
        .limit(1)
        .collect()
    )
    if recent_reset:
        reset_date   = recent_reset[0]["detected_date"].isoformat()
        change_summary = recent_reset[0]["change_summary"]
        print(f"⚠ GTFS schedule change detected on {reset_date}: {change_summary}")
        print(f"  Shrinking baseline window: {window_start} → {reset_date}")
        window_start = reset_date
        print(f"  Baseline will use post-change data only ({window_start} → {target_date})")
        print(f"  Note: sample_days will be lower than normal until {reset_date} + 28 days.")
    else:
        print(f"  No recent GTFS schedule change — using full 28-day window.")
except Exception as e:
    print(f"  ⚠ Could not check version log ({e}) — using default 28-day window.")

print(f"Window: {window_start} → {target_date} (exclusive)")

history = (
    spark.table(GOLD_ROUTE_METRICS_15MIN)
    .filter(F.col("service_date") >= F.lit(window_start).cast("date"))
    .filter(F.col("service_date") <  F.lit(target_date).cast("date"))
    .filter(F.col("trip_count") > 0)
    .filter(F.col("avg_delay_seconds").isNotNull())
)

history_count = history.count()
print(f"History rows in window: {history_count:,}")

if history_count == 0:
    print("⚠ No history data — run notebook 42 for past dates first.")
    dbutils.notebook.exit(f"No history for window ending {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Classify Service Type and Extract Bucket Components
# MAGIC
# MAGIC Valley Metro runs three distinct service schedules: weekday, saturday, sunday.
# MAGIC Spark `dayofweek()`: 1 = Sunday, 2 = Monday, ..., 7 = Saturday.

# COMMAND ----------

enriched = (
    history
    .withColumn(
        "service_type",
        F.when(F.dayofweek("service_date") == 1, F.lit("sunday"))
         .when(F.dayofweek("service_date") == 7, F.lit("saturday"))
         .otherwise(F.lit("weekday"))
    )
    .withColumn("bucket_hour",   F.hour("time_bucket_15min"))
    .withColumn("bucket_minute", F.minute("time_bucket_15min"))
)

# COMMAND ----------

# MAGIC %md ## Step 3 — Winsorize, then Aggregate to Baseline Grain
# MAGIC
# MAGIC **Winsorization:** clip `avg_delay_seconds` to the 5th–95th percentile range
# MAGIC before aggregation. This prevents detour days, special events, or data quality
# MAGIC outliers from anchoring the baseline in an unrepresentative position.
# MAGIC
# MAGIC **Minimum sample guard:** require `sample_days >= 5` per slot. With a 28-day
# MAGIC window this gives at most 20 weekday samples and 4 weekend samples; requiring 5
# MAGIC ensures at least one full week of weekday evidence or two full weekends.
# MAGIC
# MAGIC Deviation bands mirror the CAD/AVL runtime analysis tool's classification:
# MAGIC | Band        | Seconds   | Operational meaning |
# MAGIC |-------------|-----------|---------------------|
# MAGIC | lt1min      | 0 – 59    | Negligible          |
# MAGIC | 1to3min     | 60 – 179  | Moderate            |
# MAGIC | 3to6min     | 180 – 359 | Significant         |
# MAGIC | gt6min      | ≥ 360     | Severe              |

# COMMAND ----------

# Compute per-slot winsorization bounds (p5 / p95) then clip before aggregation.
# Using a self-join on the group keys keeps this in a single Spark pass.
from pyspark.sql import Window

w_slot = Window.partitionBy("route_id", "direction_id", "service_type", "bucket_hour", "bucket_minute")

enriched_w = (
    enriched
    .withColumn(
        "p05_delay",
        F.percentile_approx("avg_delay_seconds", 0.05, accuracy=100).over(w_slot)
    )
    .withColumn(
        "p95_delay",
        F.percentile_approx("avg_delay_seconds", 0.95, accuracy=100).over(w_slot)
    )
    .withColumn(
        "delay_winsorized",
        F.greatest(
            F.col("p05_delay"),
            F.least(F.col("p95_delay"), F.col("avg_delay_seconds"))
        )
    )
)

baseline = (
    enriched_w
    .groupBy("route_id", "direction_id", "service_type", "bucket_hour", "bucket_minute")
    .agg(
        F.count("*").cast("int").alias("sample_days"),
        F.min("service_date").alias("baseline_start_date"),
        F.max("service_date").alias("baseline_end_date"),

        F.avg("delay_winsorized").alias("baseline_avg_delay_seconds"),
        F.stddev("delay_winsorized").alias("baseline_stddev_delay_seconds"),
        F.percentile_approx("delay_winsorized", 0.5, accuracy=100)
            .cast("double").alias("baseline_p50_delay_seconds"),
        F.percentile_approx("delay_winsorized", 0.9, accuracy=100)
            .cast("double").alias("baseline_p90_delay_seconds"),

        F.avg("pct_on_time").alias("baseline_avg_pct_on_time"),
        F.avg("trip_count").alias("baseline_avg_trip_count"),

        # Deviation band frequencies (on winsorized delay)
        F.avg(
            F.when(F.col("delay_winsorized") < 60,   F.lit(1.0)).otherwise(F.lit(0.0))
        ).alias("baseline_pct_lt1min"),
        F.avg(
            F.when(F.col("delay_winsorized").between(60, 179),  F.lit(1.0)).otherwise(F.lit(0.0))
        ).alias("baseline_pct_1to3min"),
        F.avg(
            F.when(F.col("delay_winsorized").between(180, 359), F.lit(1.0)).otherwise(F.lit(0.0))
        ).alias("baseline_pct_3to6min"),
        F.avg(
            F.when(F.col("delay_winsorized") >= 360, F.lit(1.0)).otherwise(F.lit(0.0))
        ).alias("baseline_pct_gt6min"),
    )
    .filter(F.col("sample_days") >= 5)
)

# COMMAND ----------

# MAGIC %md ## Step 4 — Write
# MAGIC
# MAGIC Full overwrite every night — the baseline is always the current rolling window.
# MAGIC No date partition needed.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_ROUTE_METRICS_BASELINE} (
        route_id                      STRING,
        direction_id                  INT,
        service_type                  STRING,
        bucket_hour                   INT,
        bucket_minute                 INT,
        sample_days                   INT,
        baseline_start_date           DATE,
        baseline_end_date             DATE,
        baseline_avg_delay_seconds    DOUBLE,
        baseline_stddev_delay_seconds DOUBLE,
        baseline_p50_delay_seconds    DOUBLE,
        baseline_p90_delay_seconds    DOUBLE,
        baseline_avg_pct_on_time      DOUBLE,
        baseline_avg_trip_count       DOUBLE,
        baseline_pct_lt1min           DOUBLE,
        baseline_pct_1to3min          DOUBLE,
        baseline_pct_3to6min          DOUBLE,
        baseline_pct_gt6min           DOUBLE
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

(
    baseline
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")
    .saveAsTable(GOLD_ROUTE_METRICS_BASELINE)
)

print(f"✓ Written to {GOLD_ROUTE_METRICS_BASELINE}")

# COMMAND ----------

# MAGIC %md ## Step 5 — Quality Report

# COMMAND ----------

written = spark.table(GOLD_ROUTE_METRICS_BASELINE)

total_rows  = written.count()
route_count = written.select("route_id").distinct().count()
date_range  = written.agg(
    F.min("baseline_start_date").alias("start"),
    F.max("baseline_end_date").alias("end"),
).collect()[0]

print(f"""
=== Gold Route Metrics Baseline ===
  Baseline rows         : {total_rows:>10,}
  Distinct routes       : {route_count:>10,}
  Window covers         : {date_range["start"]} → {date_range["end"]}
""")

print("Row count by service_type:")
written.groupBy("service_type").count().orderBy("service_type").show()

print("Sample baseline — Route 41, weekday, direction 0:")
(
    written
    .filter(
        (F.col("route_id") == "41") &
        (F.col("direction_id") == 0) &
        (F.col("service_type") == "weekday")
    )
    .select(
        "bucket_hour", "bucket_minute", "sample_days",
        F.round("baseline_avg_delay_seconds",    1).alias("avg_delay_s"),
        F.round("baseline_stddev_delay_seconds",  1).alias("stddev_s"),
        F.round("baseline_avg_pct_on_time",       3).alias("avg_otp"),
        F.round("baseline_pct_lt1min",  3).alias("pct_lt1m"),
        F.round("baseline_pct_1to3min", 3).alias("pct_1_3m"),
        F.round("baseline_pct_3to6min", 3).alias("pct_3_6m"),
        F.round("baseline_pct_gt6min",  3).alias("pct_gt6m"),
    )
    .orderBy("bucket_hour", "bucket_minute")
    .show(24)
)
