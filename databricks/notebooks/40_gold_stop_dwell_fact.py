# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 40 — Gold: Stop Dwell Fact (Nightly Batch)
# MAGIC
# MAGIC **Schedule:** Nightly, after Silver completes
# MAGIC **Dependencies:** `silver_fact_trip_updates`, `silver_fact_stop_schedule`
# MAGIC **Grain:** `(service_date, trip_id, stop_sequence)`
# MAGIC
# MAGIC **What it does:**
# MAGIC - Joins Silver is_final trip update rows (last known state per stop) to the
# MAGIC   static GTFS schedule to compute scheduled vs actual arrival/departure times
# MAGIC - Converts GTFS seconds-since-midnight (Phoenix local time) to UTC timestamps
# MAGIC - Computes actual_dwell, scheduled_dwell, and dwell_delta per stop
# MAGIC - First and last stops get NULL dwell (no arrival/departure respectively)

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

# COMMAND ----------

# MAGIC %md ## Step 1 — Read Silver is_final rows

# COMMAND ----------

# Prefer is_first_past over is_final where available.
# is_first_past = earliest snapshot where feed_ts >= arrival_ts (stop has been passed).
# For bus feeds this is equivalent to is_final (feed drops the stop once passed).
# For rail feeds that retroactively inflate all stop delays as the trip-wide delay
# grows, is_first_past captures the actual passing moment before the inflation occurs.
# Guard: fall back to is_final for Silver partitions written before is_first_past existed.
_silver = spark.table(SILVER_FACT_TRIP_UPDATES).filter(
    F.col("service_date") == F.lit(target_date).cast("date")
)
_has_first_past = "is_first_past" in _silver.columns

if _has_first_past:
    _w_pref = Window.partitionBy("trip_id", "stop_sequence").orderBy(
        F.when(F.col("is_first_past"), F.lit(1)).otherwise(F.lit(2)),
        "feed_ts",
    )
    _candidates = (
        _silver
        .filter(F.col("is_first_past") | F.col("is_final"))
        .withColumn("_rn", F.row_number().over(_w_pref))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
else:
    _candidates = _silver.filter(F.col("is_final") == True)

final_tu = (
    _candidates
    .select(
        "service_date",
        "trip_id",
        "route_id",
        "direction_id",
        "stop_sequence",
        "stop_id",
        F.col("arrival_ts").alias("actual_arrival_ts"),
        F.col("departure_ts").alias("actual_departure_ts"),
        "arrival_delay_seconds",
        "departure_delay_seconds",
    )
)

row_count = final_tu.count()
print(f"Silver is_final rows for {target_date}: {row_count:,}")

if row_count == 0:
    print("⚠ No Silver data for this date. Run Silver notebooks first.")
    dbutils.notebook.exit(f"No Silver data for {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 1b — Override Rail Arrivals from Gold 48
# MAGIC
# MAGIC For routes in `RAIL_ROUTE_IDS` (A, B), replace the TripUpdates-based
# MAGIC `actual_arrival_ts` and `arrival_delay_seconds` with VP stop-sequence
# MAGIC transition actuals from `gold_rail_stop_actuals` (notebook 48).
# MAGIC
# MAGIC **Why:** Valley Metro Rail's SCADA system reports a single trip-level delay
# MAGIC offset applied uniformly to all stops. Even with `is_first_past`, Gold 40
# MAGIC captures the SCADA offset — every stop in a rail trip gets the same delay.
# MAGIC Gold 48 uses the first VP ping per `(trip_id, current_stop_sequence)` to
# MAGIC derive true per-stop arrival times with second-level precision.
# MAGIC
# MAGIC **Fallback:** Where Gold 48 has no coverage (VP downsampling gaps, missed
# MAGIC stops), `actual_arrival_ts` stays as the TripUpdates value — better than NULL.
# MAGIC Departure times remain TripUpdates-based (no VP departure signal available).
# MAGIC
# MAGIC **Guard:** If Gold 48 table doesn't exist yet (first pipeline run before 48
# MAGIC has written), skip silently — TripUpdates used for all routes.

# COMMAND ----------

if spark.catalog.tableExists(GOLD_RAIL_STOP_ACTUALS):
    _rail48 = (
        spark.table(GOLD_RAIL_STOP_ACTUALS)
        .filter(F.col("service_date") == F.lit(target_date).cast("date"))
        .select(
            "trip_id",
            "stop_sequence",
            F.col("actual_arrival_ts").alias("_rail_actual_ts"),
            F.col("arrival_delay_seconds").alias("_rail_delay_s"),
        )
    )

    final_tu = (
        final_tu
        .join(_rail48, on=["trip_id", "stop_sequence"], how="left")
        .withColumn(
            "actual_arrival_ts",
            F.when(
                F.col("route_id").isin(RAIL_ROUTE_IDS) & F.col("_rail_actual_ts").isNotNull(),
                F.col("_rail_actual_ts")
            ).otherwise(F.col("actual_arrival_ts"))
        )
        .withColumn(
            "arrival_delay_seconds",
            F.when(
                F.col("route_id").isin(RAIL_ROUTE_IDS) & F.col("_rail_delay_s").isNotNull(),
                F.col("_rail_delay_s")
            ).otherwise(F.col("arrival_delay_seconds"))
        )
        .drop("_rail_actual_ts", "_rail_delay_s")
    )

    _rail_count = final_tu.filter(F.col("route_id").isin(RAIL_ROUTE_IDS)).count()
    print(f"Rail arrival override active — {_rail_count:,} rail stops in final_tu (routes {RAIL_ROUTE_IDS})")
else:
    print(f"⚠ {GOLD_RAIL_STOP_ACTUALS} not found — using TripUpdates for all routes")

# COMMAND ----------

# MAGIC %md ## Step 2 — Join to Static Schedule

# COMMAND ----------

# early_allowed is a Phoenix-specific custom field (1=may depart early; NULL=must hold).
# Phoenix leaves early_allowed NULL for regular stops and sets 1 only for terminals/layovers.
# Guard against existing silver data that pre-dates notebook 01 adding the column.
_sched_raw = spark.table(SILVER_FACT_STOP_SCHEDULE)
_early_allowed_col = (
    F.col("early_allowed").cast("int").alias("early_allowed")
    if "early_allowed" in _sched_raw.columns
    else F.lit(None).cast("int").alias("early_allowed")
)

schedule = (
    _sched_raw
    .select(
        "trip_id",
        "stop_sequence",
        "scheduled_arrival_secs",
        "scheduled_departure_secs",
        F.col("pickup_type").cast("int"),   # 0=regular, 1=no pickup (drop-off only)
        F.col("drop_off_type").cast("int"), # 0=regular, 1=no drop-off (pickup only)
        _early_allowed_col,
    )
)

joined = final_tu.join(schedule, on=["trip_id", "stop_sequence"], how="left")

# COMMAND ----------

# MAGIC %md ## Step 3 — Convert Schedule Times to UTC Timestamps
# MAGIC
# MAGIC GTFS schedule times are seconds-since-midnight in Phoenix local time
# MAGIC (America/Phoenix = UTC-7, no DST). We anchor to Phoenix midnight on the
# MAGIC service_date, then add the scheduled seconds offset.
# MAGIC
# MAGIC `to_utc_timestamp(naive_ts, "America/Phoenix")` interprets the naive
# MAGIC timestamp as Phoenix local time and returns the UTC equivalent.

# COMMAND ----------

# Phoenix midnight for this service_date as a UTC epoch value
# to_timestamp("2025-03-01", "yyyy-MM-dd") creates a naive timestamp (no tz)
# Spark (session tz = UTC) interprets it as midnight UTC
# to_utc_timestamp(..., "America/Phoenix") re-interprets it as midnight Phoenix → UTC
phoenix_midnight_unix = F.unix_timestamp(
    F.to_utc_timestamp(
        F.to_timestamp(F.col("service_date").cast("string"), "yyyy-MM-dd"),
        "America/Phoenix"
    )
)

joined = (
    joined
    .withColumn(
        "scheduled_arrival_ts",
        F.when(
            F.col("scheduled_arrival_secs").isNotNull(),
            F.to_timestamp(phoenix_midnight_unix + F.col("scheduled_arrival_secs"))
        ).otherwise(F.lit(None).cast("timestamp"))
    )
    .withColumn(
        "scheduled_departure_ts",
        F.when(
            F.col("scheduled_departure_secs").isNotNull(),
            F.to_timestamp(phoenix_midnight_unix + F.col("scheduled_departure_secs"))
        ).otherwise(F.lit(None).cast("timestamp"))
    )
)

# COMMAND ----------

# MAGIC %md ## Step 4 — Compute Dwell Columns
# MAGIC
# MAGIC - **First stop** (`rn_asc == 1`): no prior segment, dwell = NULL
# MAGIC - **Last stop** (`rn_desc == 1`): trip ends here, dwell = NULL
# MAGIC - **Middle stops**: `actual_dwell = departure_ts - arrival_ts` (clamped ≥ 0)
# MAGIC - **dwell_delta** = actual_dwell - scheduled_dwell (NULL if either is NULL)

# COMMAND ----------

w_asc  = Window.partitionBy("trip_id").orderBy("stop_sequence")
w_desc = Window.partitionBy("trip_id").orderBy(F.desc("stop_sequence"))

joined = (
    joined
    .withColumn("_rn_asc",  F.row_number().over(w_asc))
    .withColumn("_rn_desc", F.row_number().over(w_desc))
)

# actual_dwell_seconds
joined = joined.withColumn(
    "actual_dwell_seconds",
    F.when(
        (F.col("_rn_asc") == 1) | (F.col("_rn_desc") == 1),
        F.lit(None).cast("int")
    ).when(
        F.col("actual_arrival_ts").isNull() | F.col("actual_departure_ts").isNull(),
        F.lit(None).cast("int")
    ).otherwise(
        F.when(
            F.unix_timestamp("actual_departure_ts") >= F.unix_timestamp("actual_arrival_ts"),
            (F.unix_timestamp("actual_departure_ts") - F.unix_timestamp("actual_arrival_ts")).cast("int")
        ).otherwise(F.lit(None).cast("int"))   # clamp negative dwell to NULL
    )
)

# scheduled_dwell_seconds
joined = joined.withColumn(
    "scheduled_dwell_seconds",
    F.when(
        (F.col("_rn_asc") == 1) | (F.col("_rn_desc") == 1),
        F.lit(None).cast("int")
    ).when(
        F.col("scheduled_arrival_secs").isNull() | F.col("scheduled_departure_secs").isNull(),
        F.lit(None).cast("int")
    ).otherwise(
        (F.col("scheduled_departure_secs") - F.col("scheduled_arrival_secs")).cast("int")
    )
)

# dwell_delta_seconds
joined = joined.withColumn(
    "dwell_delta_seconds",
    F.when(
        F.col("actual_dwell_seconds").isNull() | F.col("scheduled_dwell_seconds").isNull(),
        F.lit(None).cast("int")
    ).otherwise(
        (F.col("actual_dwell_seconds") - F.col("scheduled_dwell_seconds")).cast("int")
    )
)

# COMMAND ----------

# MAGIC %md ## Step 4b — VP-Based Terminal Arrival Correction
# MAGIC
# MAGIC TripUpdate predictions for the **last stop** of a trip are frequently stale:
# MAGIC the vehicle recovers time mid-route but the feed never revises the terminal
# MAGIC estimate, resulting in artificially inflated delay values at terminals.
# MAGIC
# MAGIC For last stops only (`_rn_desc == 1`), we find the first VP ping within 40m
# MAGIC of the stop at speed ≤ 1.0 m/s and use that event_ts as the true
# MAGIC actual_arrival_ts, replacing the TripUpdate prediction when it is earlier.

# COMMAND ----------

# ── Stop locations ─────────────────────────────────────────────────────────────
stop_locs = (
    spark.table(SILVER_DIM_STOP)
    .select(
        F.col("stop_id"),
        F.col("lat").alias("stop_lat"),
        F.col("lon").alias("stop_lon"),
    )
)

# ── Low-speed VP pings for this date ──────────────────────────────────────────
vp = (
    spark.table(SILVER_FACT_VEHICLE_POSITIONS)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
    .filter(F.col("speed_mps") <= 1.0)
    .select("trip_id", "event_ts", "lat", "lon")
)

# ── Terminal stop rows joined to their stop location ──────────────────────────
terminal_stops = (
    joined
    .filter(F.col("_rn_desc") == 1)
    .select("trip_id", "stop_id", "scheduled_arrival_ts")
    .join(stop_locs, on="stop_id", how="left")
    .filter(F.col("stop_lat").isNotNull())
)

# ── Spatial join: VP pings within 40m of each terminal stop ───────────────────
_R = 6371000.0  # Earth radius in metres

terminal_vp = (
    terminal_stops
    .join(vp, on="trip_id", how="inner")
    # Bounding box pre-filter (~40m ≈ 0.00036°, use 0.0005° for safety)
    .filter(F.abs(F.col("lat") - F.col("stop_lat")) <= 0.0005)
    .filter(F.abs(F.col("lon") - F.col("stop_lon")) <= 0.0005)
    # Haversine distance (pure Spark, no UDF)
    .withColumn("_dlat", F.radians(F.col("lat") - F.col("stop_lat")))
    .withColumn("_dlon", F.radians(F.col("lon") - F.col("stop_lon")))
    .withColumn(
        "_a",
        F.pow(F.sin(F.col("_dlat") / 2), 2)
        + F.cos(F.radians(F.col("stop_lat")))
        * F.cos(F.radians(F.col("lat")))
        * F.pow(F.sin(F.col("_dlon") / 2), 2),
    )
    .withColumn("_dist_m", F.lit(2.0 * _R) * F.asin(F.sqrt(F.col("_a"))))
    .filter(F.col("_dist_m") <= 40.0)
    # Earliest VP arrival per trip
    .groupBy("trip_id")
    .agg(F.min("event_ts").alias("vp_arrival_ts"))
)

# ── Patch joined: replace terminal actual_arrival_ts when VP is earlier ────────
# vp_arrival_ts joins to every row for that trip_id (left join); the _rn_desc
# guard ensures we only overwrite the last stop row.
joined = (
    joined
    .join(terminal_vp, on="trip_id", how="left")
    .withColumn(
        "_use_vp",
        (F.col("_rn_desc") == 1)
        & F.col("vp_arrival_ts").isNotNull()
        & (
            F.col("actual_arrival_ts").isNull()
            | (F.unix_timestamp("vp_arrival_ts") < F.unix_timestamp("actual_arrival_ts"))
        ),
    )
    .withColumn(
        "actual_arrival_ts",
        F.when(F.col("_use_vp"), F.col("vp_arrival_ts"))
        .otherwise(F.col("actual_arrival_ts")),
    )
    .withColumn(
        "arrival_delay_seconds",
        F.when(
            F.col("_use_vp") & F.col("scheduled_arrival_ts").isNotNull(),
            (F.unix_timestamp("vp_arrival_ts") - F.unix_timestamp("scheduled_arrival_ts")).cast("int"),
        ).otherwise(F.col("arrival_delay_seconds")),
    )
    .drop("vp_arrival_ts", "_use_vp")
)

vp_corrections = terminal_vp.count()
print(f"VP-based terminal corrections applied: {vp_corrections:,} trips")

# COMMAND ----------

# MAGIC %md ### Step 4b (ii) — Penultimate Propagation Fallback
# MAGIC
# MAGIC When no VP ping exists near the terminal (GPS blackout), VP correction cannot
# MAGIC fire. If the terminal delay is >300s worse than the penultimate stop, we derive
# MAGIC the terminal arrival by propagating penultimate timing through the scheduled
# MAGIC segment:
# MAGIC
# MAGIC `estimated_arrival = penult_actual_ts + (terminal_sched_ts - penult_sched_ts)`
# MAGIC
# MAGIC This preserves the real delay carried into the final segment while eliminating
# MAGIC the stale TripUpdate inflation.

# COMMAND ----------

penultimate = (
    joined
    .filter(F.col("_rn_desc") == 2)
    .select(
        "trip_id",
        F.col("actual_arrival_ts").alias("penult_actual_ts"),
        F.col("scheduled_arrival_ts").alias("penult_sched_ts"),
        F.col("arrival_delay_seconds").alias("penult_delay_seconds"),
    )
    .filter(F.col("penult_actual_ts").isNotNull())
)

terminal_rows = (
    joined
    .filter(F.col("_rn_desc") == 1)
    .select(
        "trip_id",
        F.col("actual_arrival_ts").alias("term_actual_ts"),
        F.col("scheduled_arrival_ts").alias("term_sched_ts"),
        F.col("arrival_delay_seconds").alias("term_delay_seconds"),
    )
)

penult_fallback = (
    terminal_rows
    .join(penultimate, on="trip_id", how="inner")
    .withColumn(
        "estimated_arrival_ts",
        F.to_timestamp(
            F.unix_timestamp("penult_actual_ts")
            + F.unix_timestamp("term_sched_ts")
            - F.unix_timestamp("penult_sched_ts")
        )
    )
    # Apply only when: estimate is earlier than current TU value AND delay jump >300s
    .filter(
        F.col("term_actual_ts").isNotNull()
        & (F.unix_timestamp("estimated_arrival_ts") < F.unix_timestamp("term_actual_ts"))
        & (F.col("term_delay_seconds") - F.col("penult_delay_seconds") > 300)
    )
    .select("trip_id", "estimated_arrival_ts")
)

joined = (
    joined
    .join(penult_fallback, on="trip_id", how="left")
    .withColumn(
        "_use_penult",
        (F.col("_rn_desc") == 1) & F.col("estimated_arrival_ts").isNotNull()
    )
    .withColumn(
        "actual_arrival_ts",
        F.when(F.col("_use_penult"), F.col("estimated_arrival_ts"))
        .otherwise(F.col("actual_arrival_ts"))
    )
    .withColumn(
        "arrival_delay_seconds",
        F.when(
            F.col("_use_penult") & F.col("scheduled_arrival_ts").isNotNull(),
            (F.unix_timestamp("estimated_arrival_ts") - F.unix_timestamp("scheduled_arrival_ts")).cast("int"),
        ).otherwise(F.col("arrival_delay_seconds"))
    )
    .drop("estimated_arrival_ts", "_use_penult")
)

penult_corrections = penult_fallback.count()
print(f"Penultimate-propagation fallback corrections: {penult_corrections:,} trips")

# COMMAND ----------

# MAGIC %md ## Step 5 — Final Select and Write

# COMMAND ----------

gold_dwell = joined.select(
    "service_date",
    "trip_id",
    "route_id",
    "direction_id",
    "stop_id",
    "stop_sequence",
    "scheduled_arrival_ts",
    "scheduled_departure_ts",
    "actual_arrival_ts",
    "actual_departure_ts",
    "arrival_delay_seconds",
    "departure_delay_seconds",
    "actual_dwell_seconds",
    "scheduled_dwell_seconds",
    "dwell_delta_seconds",
    "pickup_type",    # 0=regular, 1=no pickup (drop-off only)
    "drop_off_type",  # 0=regular, 1=no drop-off (pickup only)
    "early_allowed",  # Phoenix custom: 0=must hold if early, 1=may depart early — used for OTP early classification
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_STOP_DWELL_FACT} (
        service_date             DATE,
        trip_id                  STRING,
        route_id                 STRING,
        direction_id             INT,
        stop_id                  STRING,
        stop_sequence            INT,
        scheduled_arrival_ts     TIMESTAMP,
        scheduled_departure_ts   TIMESTAMP,
        actual_arrival_ts        TIMESTAMP,
        actual_departure_ts      TIMESTAMP,
        arrival_delay_seconds    INT,
        departure_delay_seconds  INT,
        actual_dwell_seconds     INT,
        scheduled_dwell_seconds  INT,
        dwell_delta_seconds      INT,
        pickup_type              INT,
        drop_off_type            INT,
        early_allowed            INT
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

(
    gold_dwell
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"service_date = '{target_date}'")
    .option("mergeSchema", "true")   # allows adding pickup_type/drop_off_type on first run
    .saveAsTable(GOLD_STOP_DWELL_FACT)
)

print(f"✓ Written to {GOLD_STOP_DWELL_FACT} for service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 6 — Quality Report

# COMMAND ----------

written = (
    spark.table(GOLD_STOP_DWELL_FACT)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

total_rows    = written.count()
with_dwell    = written.filter(F.col("actual_dwell_seconds").isNotNull()).count()
no_schedule   = written.filter(F.col("scheduled_arrival_ts").isNull()).count()

print(f"""
=== Gold Stop Dwell Fact: {target_date} ===
  Total rows              : {total_rows:>10,}
  Rows with dwell data    : {with_dwell:>10,}
  Rows missing schedule   : {no_schedule:>10,}
""")

print("Dwell delta distribution (seconds, non-null):")
written.filter(F.col("dwell_delta_seconds").isNotNull()).select(
    F.min("dwell_delta_seconds").alias("min"),
    F.percentile_approx("dwell_delta_seconds", 0.5).alias("p50"),
    F.percentile_approx("dwell_delta_seconds", 0.9).alias("p90"),
    F.max("dwell_delta_seconds").alias("max"),
).show()

print("Top 10 stops by avg dwell inflation:")
(
    written
    .filter(F.col("dwell_delta_seconds").isNotNull())
    .groupBy("stop_id", "route_id")
    .agg(
        F.avg("dwell_delta_seconds").alias("avg_dwell_delta"),
        F.count("*").alias("observations"),
    )
    .filter(F.col("observations") >= 3)
    .orderBy(F.desc("avg_dwell_delta"))
    .limit(10)
    .show()
)
