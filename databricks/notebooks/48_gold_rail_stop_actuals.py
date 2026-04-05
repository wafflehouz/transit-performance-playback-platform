# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 48 — Gold: Rail Stop Actuals (Nightly Batch)
# MAGIC
# MAGIC **Schedule:** Nightly, after Silver 31 (VP) completes — runs alongside Gold 40-47
# MAGIC **Dependencies:** `silver_fact_vehicle_positions`, `silver_fact_stop_schedule`
# MAGIC **Grain:** `(service_date, trip_id, stop_sequence)`
# MAGIC **Routes:** `RAIL_ROUTE_IDS` (A, B) — Tempe Streetcar (S) excluded; see note below
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
# MAGIC ## Tempe Streetcar ("S") — Excluded
# MAGIC
# MAGIC Route S reuses the same `trip_id` across multiple headway runs per day.
# MAGIC The first-VP-ping signal breaks because an early run's arrival gets matched
# MAGIC against a later run's scheduled time, producing multi-hour positive outliers.
# MAGIC S is grouped with bus routes in Gold 40 (TripUpdates-based) and mirrors
# MAGIC how Swiftly groups S on the frontend.

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
        F.col("lat"),          # vehicle GPS lat — used for proximity filter in Step 2
        F.col("lon"),          # vehicle GPS lon — used for proximity filter in Step 2
        F.col("vehicle_ts"),   # precise vehicle clock timestamp
    )
)

rail_count = vp_rail.count()
print(f"Rail VP rows for {target_date}: {rail_count:,}")

if rail_count == 0:
    print("No rail VP data for this date. Exiting.")
    dbutils.notebook.exit(f"No rail VP data for {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Detect Arrival via VP Proximity Filter
# MAGIC
# MAGIC **Problem with naive first-ping approach:**
# MAGIC Valley Metro's VP feed advances `current_stop_sequence` to N when the vehicle
# MAGIC **departs** the previous stop (IN_TRANSIT_TO semantics), not when it arrives at
# MAGIC stop N. Since `current_status` is 100% null, we can't distinguish IN_TRANSIT_TO
# MAGIC from STOPPED_AT. Taking the absolute first ping at seq N gives us the vehicle
# MAGIC 1–3 minutes before it physically reaches the platform — producing a systematic
# MAGIC early bias (~39% early vs Swiftly's ~3.5% early).
# MAGIC
# MAGIC **Fix — proximity-aware first ping:**
# MAGIC Join each VP ping to the stop's GPS coordinates (via `stop_id`), compute the
# MAGIC haversine distance between the vehicle and the stop, then order pings so that
# MAGIC near-stop pings (≤ `RAIL_ARRIVAL_RADIUS_M`) come first. The first ping within
# MAGIC radius = vehicle has physically arrived near the platform.
# MAGIC
# MAGIC **Fallback:** if no ping is within the radius for a (trip_id, stop_sequence)
# MAGIC (GPS gap, coordinate mismatch), falls back to the absolute earliest ping —
# MAGIC preserving original behavior rather than dropping the stop.
# MAGIC
# MAGIC **Radius choice:** 300 m. At peak rail speed (~11 m/s) with 30 s downsampling,
# MAGIC the train travels ~330 m between pings. 300 m guarantees at least one ping is
# MAGIC captured within one polling cycle of actual platform arrival.

# COMMAND ----------

RAIL_ARRIVAL_RADIUS_M = 300.0
_EARTH_R = 6371000.0

stop_locs = (
    spark.table(SILVER_DIM_STOP)
    .select(
        F.col("stop_id"),
        F.col("lat").alias("stop_lat"),
        F.col("lon").alias("stop_lon"),
    )
)

# Join VP pings to the stop they're reporting (stop_id = current stop at that seq)
vp_with_dist = (
    vp_rail
    .join(stop_locs, on="stop_id", how="left")
    .withColumn("_dlat", F.radians(F.col("lat") - F.col("stop_lat")))
    .withColumn("_dlon", F.radians(F.col("lon") - F.col("stop_lon")))
    .withColumn(
        "_a",
        F.pow(F.sin(F.col("_dlat") / 2), 2)
        + F.cos(F.radians(F.col("stop_lat")))
        * F.cos(F.radians(F.col("lat")))
        * F.pow(F.sin(F.col("_dlon") / 2), 2),
    )
    .withColumn("_dist_m", F.lit(2.0 * _EARTH_R) * F.asin(F.sqrt(F.col("_a"))))
    .withColumn("_near_stop", F.col("_dist_m") <= RAIL_ARRIVAL_RADIUS_M)
    .drop("_dlat", "_dlon", "_a", "_dist_m", "stop_lat", "stop_lon")
)

# Arrival window: VP feeds sometimes keep broadcasting a stale trip_id for hours
# after the trip ends (e.g., parked vehicle near a stop late at night). Proximity
# filter without a time bound would select those late pings over the true arrival.
# Fix: anchor the search to within 10 minutes of the FIRST ping at each sequence.
# The true arrival is always within this window; stale broadcasts hours later are not.
_w_min = Window.partitionBy("trip_id", "stop_sequence")
vp_with_dist = (
    vp_with_dist
    .withColumn("_first_ts_unix", F.min(F.unix_timestamp("vehicle_ts")).over(_w_min))
    .withColumn(
        "_in_window",
        (F.unix_timestamp("vehicle_ts") - F.col("_first_ts_unix")) <= 600  # 10-min window
    )
)

# Priority:
# 1 — near stop AND in arrival window (best: physically arrived, timely)
# 2 — in arrival window only (timely but not yet within 300m — use as fallback)
# 3 — everything else (last resort: prevents gaps but may be stale)
w_arrival = Window.partitionBy("trip_id", "stop_sequence").orderBy(
    F.when(F.col("_near_stop") & F.col("_in_window"), F.lit(1))
     .when(F.col("_in_window"), F.lit(2))
     .otherwise(F.lit(3)),
    "vehicle_ts",
)

first_ping = (
    vp_with_dist
    .withColumn("rn", F.row_number().over(w_arrival))
    .filter(F.col("rn") == 1)
    .drop("rn", "_near_stop", "_in_window", "_first_ts_unix", "lat", "lon")
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
# MAGIC (America/Phoenix = UTC-7, no DST).
# MAGIC
# MAGIC **Midnight-crossing anchor fix:**
# MAGIC When `scheduled_arrival_secs > 86400`, the GTFS trip is listed under the
# MAGIC previous service day (e.g., a 1 AM run listed under 2026-04-02 as 25:00:00).
# MAGIC The VP `service_date` for those pings is the physical date (2026-04-03),
# MAGIC one day ahead of the GTFS service_date. Anchoring to VP service_date + 90000s
# MAGIC produces a ~-86400s delay. Fix: use `service_date - 1 day` as the anchor
# MAGIC whenever `scheduled_arrival_secs > 86400`.

# COMMAND ----------

# Anchor date: subtract 1 day for stops that cross midnight (GTFS >24:00 times).
# For all other stops, anchor to the VP service_date as usual.
anchor_date = F.when(
    F.col("scheduled_arrival_secs") > 86400,
    F.date_sub(F.col("service_date"), 1)
).otherwise(F.col("service_date"))

phoenix_midnight_unix = F.unix_timestamp(
    F.to_utc_timestamp(
        F.to_timestamp(anchor_date.cast("string"), "yyyy-MM-dd"),
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
