# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 48 — Gold: Rail Stop Actuals (Nightly Batch)
# MAGIC
# MAGIC **Schedule:** Nightly, after Silver 31 (VP) completes — runs alongside Gold 40-47
# MAGIC **Dependencies:** `silver_fact_vehicle_positions`, `silver_fact_stop_schedule`
# MAGIC **Grain:** `(service_date, trip_id, stop_sequence)`
# MAGIC **Routes:** Rail only — `RAIL_ROUTE_IDS` (A, B, S)
# MAGIC
# MAGIC ## Why This Notebook Exists
# MAGIC
# MAGIC Valley Metro Rail's SCADA system reports a **single trip-level delay offset**
# MAGIC that gets applied uniformly to all stop predictions in the TripUpdates feed.
# MAGIC Even with `is_first_past`, Gold 40 captures the SCADA offset — not a true
# MAGIC per-stop actual arrival. Every stop in a rail trip gets the same delay value.
# MAGIC
# MAGIC Swiftly shows sub-minute per-stop OTP for rail (e.g., +6:26, -1:12 with
# MAGIC natural variation within a trip). Swiftly uses VP `current_stop_sequence`
# MAGIC transitions from the same Mecatran feed — no direct CAD/AVL integration.
# MAGIC
# MAGIC ## The Signal
# MAGIC
# MAGIC The **first `vehicle_ts`** when `current_stop_sequence` first appears for a
# MAGIC `(trip_id, stop_sequence)` combination = actual arrival at that stop.
# MAGIC
# MAGIC Why this works:
# MAGIC - `current_stop_sequence` advances as the train departs each stop
# MAGIC - The first VP ping at a new sequence = the vehicle just arrived there
# MAGIC - `vehicle_ts` has second-level precision from the vehicle's own clock
# MAGIC - One polling cycle of latency (~60s) but timestamp precision is sub-minute
# MAGIC   within that window — same limitation Swiftly has
# MAGIC
# MAGIC ## Coverage Note
# MAGIC
# MAGIC `current_status` is 100% null in Valley Metro's VP feed (confirmed 2026-04-03,
# MAGIC route B, 11,365 rows). STOPPED_AT/IN_TRANSIT_TO transitions are unavailable.
# MAGIC Sequence transitions are the only usable signal.
# MAGIC
# MAGIC ## Tempe Streetcar ("S")
# MAGIC
# MAGIC Included in `RAIL_ROUTE_IDS` for completeness. If no VP data exists for "S",
# MAGIC the notebook exits cleanly after Step 1's route filter returns 0 rows.

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
print(f"Rail route IDs: {RAIL_ROUTE_IDS}")

# COMMAND ----------

# MAGIC %md ## Step 1 — Read Silver VP for Rail Routes

# COMMAND ----------

vp_rail = (
    spark.table(SILVER_FACT_VEHICLE_POSITIONS)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
    .filter(F.col("route_id").isin(RAIL_ROUTE_IDS))
    # Require a usable stop sequence — rows with NULL sequence can't be joined
    .filter(F.col("current_stop_sequence").isNotNull())
    # Require a trip_id — unassigned vehicles won't match stop_times
    .filter(F.col("trip_id").isNotNull())
    .select(
        "service_date",
        "vehicle_id",
        "trip_id",
        "route_id",
        "direction_id",
        F.col("current_stop_sequence").alias("stop_sequence"),
        F.col("stop_id"),
        F.col("vehicle_ts"),   # precise vehicle clock timestamp
    )
)

rail_count = vp_rail.count()
print(f"Rail VP rows for {target_date}: {rail_count:,}")

if rail_count == 0:
    print("No rail VP data for this date. Exiting.")
    dbutils.notebook.exit(f"No rail VP data for {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Detect First VP Ping per (trip_id, stop_sequence)
# MAGIC
# MAGIC Within each `(trip_id, stop_sequence)` window, the row with the earliest
# MAGIC `vehicle_ts` is the moment the train first appeared at that stop — i.e.,
# MAGIC the actual arrival. Subsequent pings at the same sequence represent dwell.
# MAGIC
# MAGIC The 30-second downsampling in Silver 31 means we have at most one row per
# MAGIC vehicle per 30s bucket, so `vehicle_ts` here is the most precise timestamp
# MAGIC available within each bucket.

# COMMAND ----------

w_first = Window.partitionBy("trip_id", "stop_sequence").orderBy("vehicle_ts")

first_ping = (
    vp_rail
    .withColumn("rn", F.row_number().over(w_first))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumnRenamed("vehicle_ts", "actual_arrival_ts")
)

stop_count = first_ping.count()
print(f"Distinct (trip, stop_sequence) arrivals detected: {stop_count:,}")

# COMMAND ----------

# MAGIC %md ## Step 3 — Join to GTFS Scheduled Arrivals
# MAGIC
# MAGIC Join on `(trip_id, stop_sequence)` to get the scheduled arrival time in
# MAGIC seconds-since-midnight (Phoenix local), then convert to a UTC timestamp
# MAGIC using the same Phoenix midnight anchor used in Gold 40.
# MAGIC
# MAGIC Gaps in `current_stop_sequence` (skipped stops at off-peak) produce no row
# MAGIC in `first_ping` — the left join leaves them absent in the output, which is
# MAGIC correct: we only report stops where a VP transition was observed.

# COMMAND ----------

schedule = (
    spark.table(SILVER_FACT_STOP_SCHEDULE)
    .select("trip_id", "stop_sequence", "scheduled_arrival_secs")
    .filter(F.col("scheduled_arrival_secs").isNotNull())
)

joined = first_ping.join(schedule, on=["trip_id", "stop_sequence"], how="left")

# COMMAND ----------

# MAGIC %md ## Step 4 — Compute Scheduled Arrival Timestamp and Delay
# MAGIC
# MAGIC GTFS schedule times are seconds-since-midnight in Phoenix local time
# MAGIC (America/Phoenix = UTC-7, no DST). Same anchor logic as Gold 40.

# COMMAND ----------

phoenix_midnight_unix = F.unix_timestamp(
    F.to_utc_timestamp(
        F.to_timestamp(F.col("service_date").cast("string"), "yyyy-MM-dd"),
        "America/Phoenix"
    )
)

actuals = (
    joined
    .withColumn(
        "scheduled_arrival_ts",
        F.when(
            F.col("scheduled_arrival_secs").isNotNull(),
            F.to_timestamp(phoenix_midnight_unix + F.col("scheduled_arrival_secs"))
        )
    )
    .withColumn(
        "arrival_delay_seconds",
        F.when(
            F.col("scheduled_arrival_ts").isNotNull() & F.col("actual_arrival_ts").isNotNull(),
            (F.unix_timestamp("actual_arrival_ts") - F.unix_timestamp("scheduled_arrival_ts")).cast("int")
        )
    )
    .select(
        "service_date",
        "trip_id",
        "route_id",
        "direction_id",
        "vehicle_id",
        "stop_sequence",
        "stop_id",
        "actual_arrival_ts",
        "scheduled_arrival_ts",
        "scheduled_arrival_secs",
        "arrival_delay_seconds",
    )
)

# COMMAND ----------

# MAGIC %md ## Step 5 — Write

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_RAIL_STOP_ACTUALS} (
        service_date            DATE,
        trip_id                 STRING,
        route_id                STRING,
        direction_id            INT,
        vehicle_id              STRING,
        stop_sequence           INT,
        stop_id                 STRING,
        actual_arrival_ts       TIMESTAMP,
        scheduled_arrival_ts    TIMESTAMP,
        scheduled_arrival_secs  INT,
        arrival_delay_seconds   INT
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

(
    actuals
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"service_date = '{target_date}'")
    .option("overwriteSchema", "false")
    .saveAsTable(GOLD_RAIL_STOP_ACTUALS)
)

print(f"Written to {GOLD_RAIL_STOP_ACTUALS} for service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 6 — Quality Report

# COMMAND ----------

written = (
    spark.table(GOLD_RAIL_STOP_ACTUALS)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

total_rows     = written.count()
trips_with_data = written.select("trip_id").distinct().count()
stops_with_sched = written.filter(F.col("scheduled_arrival_ts").isNotNull()).count()
sched_coverage = stops_with_sched / total_rows * 100 if total_rows > 0 else 0

print(f"""
=== Rail Stop Actuals: {target_date} ===
  Total (trip, stop) rows   : {total_rows:>10,}
  Distinct trips            : {trips_with_data:>10,}
  Avg stops per trip        : {total_rows // max(trips_with_data, 1):>10,}
  Stops with scheduled time : {stops_with_sched:>10,}  ({sched_coverage:.1f}%)
""")

print("Delay distribution (seconds) — stops with schedule:")
(
    written
    .filter(F.col("arrival_delay_seconds").isNotNull())
    .select(
        F.min("arrival_delay_seconds").alias("min_s"),
        F.percentile_approx("arrival_delay_seconds", 0.1).alias("p10_s"),
        F.percentile_approx("arrival_delay_seconds", 0.5).alias("p50_s"),
        F.percentile_approx("arrival_delay_seconds", 0.9).alias("p90_s"),
        F.max("arrival_delay_seconds").alias("max_s"),
        F.round(F.avg("arrival_delay_seconds"), 1).alias("avg_s"),
        (F.sum(F.when(F.col("arrival_delay_seconds") <= 60, 1).otherwise(0))
          * 100.0 / F.count("*")).alias("pct_on_time"),
    )
    .show()
)

print("Coverage by route:")
(
    written
    .groupBy("route_id")
    .agg(
        F.countDistinct("trip_id").alias("trips"),
        F.count("*").alias("stop_arrivals"),
        F.round(
            F.sum(F.when(F.col("arrival_delay_seconds").isNotNull(), 1).otherwise(0))
            * 100.0 / F.count("*"), 1
        ).alias("pct_with_delay"),
        F.round(F.avg("arrival_delay_seconds"), 1).alias("avg_delay_s"),
    )
    .orderBy("route_id")
    .show()
)

print("Sample — 10 stop arrivals with largest delay:")
(
    written
    .filter(F.col("arrival_delay_seconds").isNotNull())
    .orderBy(F.desc("arrival_delay_seconds"))
    .select(
        "route_id", "trip_id", "vehicle_id", "stop_sequence", "stop_id",
        "actual_arrival_ts", "scheduled_arrival_ts", "arrival_delay_seconds"
    )
    .limit(10)
    .show(truncate=False)
)
