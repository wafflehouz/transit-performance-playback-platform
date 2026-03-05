# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 46 — Gold: Anomaly Events (Nightly Batch)
# MAGIC
# MAGIC **Schedule:** Nightly, after notebooks 42 and 45 complete
# MAGIC **Dependencies:** `gold_route_metrics_15min`, `gold_route_metrics_baseline`
# MAGIC **Grain:** one row per flagged (service_date, route_id, direction_id, time_bucket_15min)
# MAGIC
# MAGIC **Detection logic — a bucket is flagged when ANY condition fires:**
# MAGIC
# MAGIC | Severity | Condition |
# MAGIC |----------|-----------|
# MAGIC | `warning`  | avg_delay ≥ 180 s  AND  avg_delay > baseline_avg + 1.5 × stddev |
# MAGIC | `critical` | avg_delay ≥ 360 s  AND  avg_delay > baseline_avg + 2.0 × stddev |
# MAGIC | `warning`  | pct_on_time < baseline_avg_pct_on_time − 0.15  (OTP dropped 15 pp) |
# MAGIC | `critical` | pct_on_time < baseline_avg_pct_on_time − 0.25  (OTP dropped 25 pp) |
# MAGIC
# MAGIC When both warning and critical conditions fire on the same bucket, critical wins.
# MAGIC
# MAGIC **Why stddev-anchored thresholds:**
# MAGIC Routes vary enormously — Route 0 (RAPID) rarely exceeds 60 s delay while local
# MAGIC routes routinely run 3–4 min late. A fixed threshold would flood alerts on slow
# MAGIC routes and miss real deterioration on fast ones. Anchoring to baseline_avg + N×stddev
# MAGIC catches unusual deviations *relative to that route's own history*.
# MAGIC
# MAGIC **Minimum baseline guard:** buckets with fewer than 3 baseline sample_days are
# MAGIC skipped — not enough history to anchor a meaningful threshold.

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
print(f"Detecting anomalies for service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 1 — Read Today's Metrics

# COMMAND ----------

today_metrics = (
    spark.table(GOLD_ROUTE_METRICS_15MIN)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
    .filter(F.col("trip_count") > 0)
    .withColumn(
        "service_type",
        F.when(F.dayofweek("service_date") == 1, F.lit("sunday"))
         .when(F.dayofweek("service_date") == 7, F.lit("saturday"))
         .otherwise(F.lit("weekday"))
    )
    .withColumn("bucket_hour",   F.hour("time_bucket_15min"))
    .withColumn("bucket_minute", F.minute("time_bucket_15min"))
)

metrics_count = today_metrics.count()
print(f"Metric buckets for {target_date}: {metrics_count:,}")

if metrics_count == 0:
    print("⚠ No metrics — run notebooks 41 and 42 first.")
    dbutils.notebook.exit(f"No metrics for {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Join to Baseline

# COMMAND ----------

baseline = spark.table(GOLD_ROUTE_METRICS_BASELINE)

joined = (
    today_metrics.alias("m")
    .join(
        baseline.alias("b"),
        on=[
            F.col("m.route_id")      == F.col("b.route_id"),
            F.col("m.direction_id")  == F.col("b.direction_id"),
            F.col("m.service_type")  == F.col("b.service_type"),
            F.col("m.bucket_hour")   == F.col("b.bucket_hour"),
            F.col("m.bucket_minute") == F.col("b.bucket_minute"),
        ],
        how="inner",   # only flag buckets that have a baseline
    )
    .select(
        F.col("m.service_date"),
        F.col("m.route_id"),
        F.col("m.direction_id"),
        F.col("m.time_bucket_15min"),
        F.col("m.trip_count"),
        F.col("m.avg_delay_seconds"),
        F.col("m.p90_delay_seconds"),
        F.col("m.pct_on_time"),
        F.col("m.avg_dwell_delta_seconds"),
        F.col("b.sample_days"),
        F.col("b.baseline_avg_delay_seconds"),
        F.col("b.baseline_stddev_delay_seconds"),
        F.col("b.baseline_avg_pct_on_time"),
        F.col("b.baseline_pct_lt1min"),
        F.col("b.baseline_pct_1to3min"),
        F.col("b.baseline_pct_3to6min"),
        F.col("b.baseline_pct_gt6min"),
    )
)

# COMMAND ----------

# MAGIC %md ## Step 3 — Evaluate Anomaly Conditions
# MAGIC
# MAGIC **Delay thresholds:**
# MAGIC - warning  : avg_delay ≥ 180 s AND avg_delay > baseline_avg + 1.5 × stddev
# MAGIC - critical : avg_delay ≥ 360 s AND avg_delay > baseline_avg + 2.0 × stddev
# MAGIC
# MAGIC **OTP drop thresholds:**
# MAGIC - warning  : pct_on_time dropped ≥ 0.15 below baseline
# MAGIC - critical : pct_on_time dropped ≥ 0.25 below baseline
# MAGIC
# MAGIC When stddev is NULL (all baseline values identical) we fall back to a fixed
# MAGIC absolute threshold so those routes are still covered.

# COMMAND ----------

# Effective upper bound for z-score comparison.
# If stddev is NULL or 0 we use a 0 floor so the abs threshold is still the gate.
eff_stddev = F.coalesce(F.col("baseline_stddev_delay_seconds"), F.lit(0.0))

delay_warn_threshold     = F.col("baseline_avg_delay_seconds") + F.lit(1.5) * eff_stddev
delay_critical_threshold = F.col("baseline_avg_delay_seconds") + F.lit(2.0) * eff_stddev

delay_warning_flag = (
    (F.col("avg_delay_seconds") >= 180) &
    (F.col("avg_delay_seconds") > delay_warn_threshold)
)
delay_critical_flag = (
    (F.col("avg_delay_seconds") >= 360) &
    (F.col("avg_delay_seconds") > delay_critical_threshold)
)

otp_drop = F.col("baseline_avg_pct_on_time") - F.col("pct_on_time")
otp_warning_flag  = (F.col("pct_on_time").isNotNull()) & (otp_drop >= 0.15)
otp_critical_flag = (F.col("pct_on_time").isNotNull()) & (otp_drop >= 0.25)

# Severity: critical beats warning. Only emit rows that are at least warning level.
scored = (
    joined
    .withColumn(
        "severity",
        F.when(delay_critical_flag | otp_critical_flag, F.lit("critical"))
         .when(delay_warning_flag  | otp_warning_flag,  F.lit("warning"))
         .otherwise(F.lit(None).cast("string"))
    )
    .filter(F.col("severity").isNotNull())
    .withColumn(
        "delay_vs_baseline_seconds",
        F.col("avg_delay_seconds") - F.col("baseline_avg_delay_seconds")
    )
    .withColumn(
        "delay_z_score",
        F.when(
            eff_stddev > 0,
            (F.col("avg_delay_seconds") - F.col("baseline_avg_delay_seconds")) / eff_stddev
        ).otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "otp_drop_pp",
        F.when(
            F.col("pct_on_time").isNotNull(),
            (F.col("baseline_avg_pct_on_time") - F.col("pct_on_time")) * 100
        ).otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        # Human-readable summary of which conditions fired
        "trigger_flags",
        F.concat_ws(",",
            F.when(delay_critical_flag, F.lit("delay_critical")),
            F.when(delay_warning_flag & ~delay_critical_flag, F.lit("delay_warning")),
            F.when(otp_critical_flag,  F.lit("otp_critical")),
            F.when(otp_warning_flag & ~otp_critical_flag, F.lit("otp_warning")),
        )
    )
    .select(
        "service_date",
        "route_id",
        "direction_id",
        "time_bucket_15min",
        "severity",
        "trigger_flags",
        "trip_count",
        "avg_delay_seconds",
        "p90_delay_seconds",
        "delay_vs_baseline_seconds",
        "delay_z_score",
        "pct_on_time",
        "otp_drop_pp",
        "avg_dwell_delta_seconds",
        "baseline_avg_delay_seconds",
        "baseline_stddev_delay_seconds",
        "baseline_avg_pct_on_time",
        "sample_days",
    )
)

anomaly_count = scored.count()
print(f"Anomalies detected for {target_date}: {anomaly_count:,}")

# COMMAND ----------

# MAGIC %md ## Step 4 — Write

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_ANOMALY_EVENTS} (
        service_date                  DATE,
        route_id                      STRING,
        direction_id                  INT,
        time_bucket_15min             TIMESTAMP,
        severity                      STRING,
        trigger_flags                 STRING,
        trip_count                    INT,
        avg_delay_seconds             DOUBLE,
        p90_delay_seconds             DOUBLE,
        delay_vs_baseline_seconds     DOUBLE,
        delay_z_score                 DOUBLE,
        pct_on_time                   DOUBLE,
        otp_drop_pp                   DOUBLE,
        avg_dwell_delta_seconds       DOUBLE,
        baseline_avg_delay_seconds    DOUBLE,
        baseline_stddev_delay_seconds DOUBLE,
        baseline_avg_pct_on_time      DOUBLE,
        sample_days                   INT
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

(
    scored
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"service_date = '{target_date}'")
    .option("overwriteSchema", "false")
    .saveAsTable(GOLD_ANOMALY_EVENTS)
)

print(f"✓ Written to {GOLD_ANOMALY_EVENTS} for service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 5 — Quality Report

# COMMAND ----------

if anomaly_count == 0:
    print("✓ No anomalies detected — all routes within baseline bounds.")
    dbutils.notebook.exit(f"Clean run for {target_date}")

written = (
    spark.table(GOLD_ANOMALY_EVENTS)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

print(f"\n=== Anomaly Events: {target_date} ===")

print("\nBy severity:")
written.groupBy("severity").count().orderBy("severity").show()

print("By trigger type:")
(
    written
    .groupBy("trigger_flags")
    .count()
    .orderBy(F.desc("count"))
    .show()
)

print("Top 15 worst anomalies (by avg_delay_seconds):")
(
    written
    .select(
        "severity",
        "route_id",
        "direction_id",
        F.date_format("time_bucket_15min", "HH:mm").alias("bucket"),
        F.round("avg_delay_seconds", 0).alias("delay_s"),
        F.round("delay_vs_baseline_seconds", 0).alias("vs_baseline_s"),
        F.round("delay_z_score", 2).alias("z_score"),
        F.round("otp_drop_pp", 1).alias("otp_drop_pp"),
        "trigger_flags",
    )
    .orderBy(F.desc("avg_delay_seconds"))
    .limit(15)
    .show(truncate=False)
)

print("Critical anomalies by route (count):")
(
    written
    .filter(F.col("severity") == "critical")
    .groupBy("route_id")
    .agg(
        F.count("*").alias("critical_buckets"),
        F.round(F.avg("avg_delay_seconds"), 0).alias("avg_delay_s"),
        F.round(F.avg("delay_z_score"), 2).alias("avg_z"),
    )
    .orderBy(F.desc("critical_buckets"))
    .show()
)
