# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 49 — Gold: Missed Trips (Nightly Batch)
# MAGIC
# MAGIC **Schedule:** Nightly, parallel with 42/43/44/47/48 (after notebook 40)
# MAGIC **Dependencies:** `silver_dim_trip`, `silver_dim_calendar_dates`,
# MAGIC                   `silver_fact_stop_schedule`, `gold_stop_dwell_fact`
# MAGIC **Grain:** `(service_date, trip_id)`
# MAGIC
# MAGIC **What it does:**
# MAGIC - Enumerates every trip scheduled to run on `service_date` using
# MAGIC   `silver_dim_calendar_dates` (exception_type=1, date matches service_date).
# MAGIC - Computes planned start/end timestamps from the GTFS schedule
# MAGIC   (first/last stop in silver_fact_stop_schedule).
# MAGIC - Cross-references against `gold_stop_dwell_fact` (observed trips) to
# MAGIC   classify each scheduled trip as:
# MAGIC     complete   — ≥ 80 % of scheduled stops observed
# MAGIC     partial    — 1–79 % of scheduled stops observed
# MAGIC     missed     — 0 stops observed (trip never appeared in TripUpdates)
# MAGIC - Computes `scheduled_stop_count`, `observed_stop_count`, `completion_pct`
# MAGIC   for CAD/AVL validation and dashboard display.
# MAGIC
# MAGIC **GTFS transition safety:**
# MAGIC   When Valley Metro publishes an inclusive GTFS (current + future schedule
# MAGIC   in the same file), silver_dim_trip contains trips for both periods.
# MAGIC   The calendar_dates filter (exception_type=1 for service_date) ensures
# MAGIC   only trips whose service_id is active on the queried date are included —
# MAGIC   future-schedule trip_ids are excluded automatically.

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

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

# silver_dim_calendar_dates stores dates as YYYYMMDD strings
target_date_str = target_date.replace("-", "")  # e.g. "20260407"

# COMMAND ----------

# MAGIC %md ## Step 1 — Build "should run" set from calendar_dates + dim_trip

# COMMAND ----------

# Active service_ids for this date: exception_type=1 means service runs this day
active_service_ids = (
    spark.table(SILVER_DIM_CALENDAR_DATES)
    .filter(
        (F.col("date") == F.lit(target_date_str)) &
        (F.col("exception_type") == 1)
    )
    .select("service_id")
    .distinct()
)

active_service_count = active_service_ids.count()
print(f"Active service_ids for {target_date}: {active_service_count}")

if active_service_count == 0:
    print("⚠ No active service_ids for this date. Check calendar_dates.")
    dbutils.notebook.exit(f"No active service_ids for {target_date}")

# All trips whose service_id is active today
scheduled_trips = (
    spark.table(SILVER_DIM_TRIP)
    .join(active_service_ids, on="service_id", how="inner")
    .select(
        "trip_id",
        "route_id",
        "service_id",
        F.col("direction_id").cast("int"),
        "trip_headsign",
    )
    .distinct()
)

scheduled_count = scheduled_trips.count()
print(f"Trips scheduled for {target_date}: {scheduled_count:,}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Planned start/end times from GTFS schedule
# MAGIC
# MAGIC Uses silver_fact_stop_schedule (all stops) to derive:
# MAGIC - planned_start: first stop scheduled_arrival_secs → UTC timestamp
# MAGIC - planned_end:   last stop scheduled_arrival_secs → UTC timestamp
# MAGIC - first_timepoint_secs: first stop with timepoint=1 (matches printed timetable)
# MAGIC - scheduled_stop_count: total stops in GTFS for this trip

# COMMAND ----------

# Phoenix midnight for this service_date as a UTC epoch value
phoenix_midnight_unix = F.unix_timestamp(
    F.to_utc_timestamp(
        F.to_timestamp(F.lit(target_date), "yyyy-MM-dd"),
        "America/Phoenix"
    )
)

sched = spark.table(SILVER_FACT_STOP_SCHEDULE)

trip_sched_agg = (
    sched
    .groupBy("trip_id")
    .agg(
        F.count("*").alias("scheduled_stop_count"),
        F.min("scheduled_arrival_secs").alias("min_sched_secs"),
        F.max("scheduled_arrival_secs").alias("max_sched_secs"),
        # First timepoint (timepoint=1) — aligns with printed timetable
        F.min(
            F.when(F.col("timepoint") == 1, F.col("scheduled_arrival_secs"))
        ).alias("first_tp_secs"),
    )
    .withColumn(
        "planned_start_ts",
        F.to_timestamp(phoenix_midnight_unix + F.col("min_sched_secs"))
    )
    .withColumn(
        "planned_end_ts",
        F.to_timestamp(phoenix_midnight_unix + F.col("max_sched_secs"))
    )
    .withColumn(
        "first_timepoint_ts",
        F.when(
            F.col("first_tp_secs").isNotNull(),
            F.to_timestamp(phoenix_midnight_unix + F.col("first_tp_secs"))
        ).otherwise(F.lit(None).cast("timestamp"))
    )
    .drop("min_sched_secs", "max_sched_secs", "first_tp_secs")
)

# Join schedule aggregation to scheduled trips
scheduled_with_times = (
    scheduled_trips
    .join(trip_sched_agg, on="trip_id", how="left")
)

# COMMAND ----------

# MAGIC %md ## Step 3 — Observed stop counts from Gold 40

# COMMAND ----------

observed_counts = (
    spark.table(GOLD_STOP_DWELL_FACT)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
    .groupBy("trip_id")
    .agg(
        F.count("*").alias("observed_stop_count"),
        F.countDistinct("stop_id").alias("observed_unique_stops"),
    )
)

# COMMAND ----------

# MAGIC %md ## Step 4 — Join and classify

# COMMAND ----------

classified = (
    scheduled_with_times
    .join(observed_counts, on="trip_id", how="left")
    .withColumn(
        "observed_stop_count",
        F.coalesce(F.col("observed_stop_count"), F.lit(0))
    )
    .withColumn(
        "observed_unique_stops",
        F.coalesce(F.col("observed_unique_stops"), F.lit(0))
    )
    .withColumn(
        "completion_pct",
        F.when(
            F.col("scheduled_stop_count") > 0,
            F.round(
                F.col("observed_stop_count").cast("double") /
                F.col("scheduled_stop_count").cast("double") * 100.0,
                1
            )
        ).otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "observation_status",
        F.when(F.col("observed_stop_count") == 0, F.lit("missed"))
         .when(F.col("completion_pct") >= 80.0,   F.lit("complete"))
         .otherwise(F.lit("partial"))
    )
    .withColumn("service_date", F.lit(target_date).cast("date"))
    .select(
        "service_date",
        "trip_id",
        "route_id",
        "direction_id",
        "trip_headsign",
        "service_id",
        "planned_start_ts",
        "planned_end_ts",
        "first_timepoint_ts",
        "scheduled_stop_count",
        "observed_stop_count",
        "completion_pct",
        "observation_status",
    )
)

# COMMAND ----------

# MAGIC %md ## Step 5 — Write

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_MISSED_TRIPS} (
        service_date             DATE,
        trip_id                  STRING,
        route_id                 STRING,
        direction_id             INT,
        trip_headsign            STRING,
        service_id               STRING,
        planned_start_ts         TIMESTAMP,
        planned_end_ts           TIMESTAMP,
        first_timepoint_ts       TIMESTAMP,
        scheduled_stop_count     LONG,
        observed_stop_count      LONG,
        completion_pct           DOUBLE,
        observation_status       STRING
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

(
    classified
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"service_date = '{target_date}'")
    .option("mergeSchema", "true")
    .saveAsTable(GOLD_MISSED_TRIPS)
)

print(f"✓ Written to {GOLD_MISSED_TRIPS} for service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 6 — Quality Report

# COMMAND ----------

result = (
    spark.table(GOLD_MISSED_TRIPS)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

total       = result.count()
complete    = result.filter(F.col("observation_status") == "complete").count()
partial     = result.filter(F.col("observation_status") == "partial").count()
missed      = result.filter(F.col("observation_status") == "missed").count()
fleet_pct   = round((complete + partial) / total * 100, 1) if total > 0 else 0.0

print(f"""
=== Gold Missed Trips: {target_date} ===
  Total scheduled trips   : {total:>8,}
  Complete  (≥80% stops)  : {complete:>8,}
  Partial   (1–79% stops) : {partial:>8,}
  Missed    (0 stops)     : {missed:>8,}
  Fleet completion        : {fleet_pct:>7.1f}%
""")

print("Missed trips by route (top 20):")
(
    result
    .filter(F.col("observation_status") == "missed")
    .groupBy("route_id")
    .agg(
        F.count("*").alias("missed_count"),
        F.countDistinct("direction_id").alias("directions_affected"),
    )
    .orderBy(F.desc("missed_count"))
    .limit(20)
    .show()
)

print("Partial trips by route (top 20):")
(
    result
    .filter(F.col("observation_status") == "partial")
    .groupBy("route_id")
    .agg(
        F.count("*").alias("partial_count"),
        F.round(F.avg("completion_pct"), 1).alias("avg_completion_pct"),
    )
    .orderBy(F.desc("partial_count"))
    .limit(20)
    .show()
)
