# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 61 — Rail Ground Truth Comparison
# MAGIC
# MAGIC **Run manually** after notebook 60 has loaded at least one survey.
# MAGIC **Not part of the scheduled pipeline.** Safe to re-run (full overwrite per service_date).
# MAGIC
# MAGIC ## What it does
# MAGIC Joins surveyed door-open timestamps (`gold_rail_ground_truth`) to the pipeline's
# MAGIC computed arrival estimates (`gold_stop_dwell_fact`) on
# MAGIC `(service_date, gtfs_trip_id, stop_sequence)` and computes two error metrics
# MAGIC per stop:
# MAGIC
# MAGIC | Metric | Definition |
# MAGIC |---|---|
# MAGIC | `timing_error_seconds` | `actual_door_open_ts` − `pipeline_actual_arrival_ts` |
# MAGIC | `dwell_error_seconds`   | `dwell_actual_seconds` − `pipeline_dwell_seconds` |
# MAGIC
# MAGIC **Positive `timing_error_seconds`** means the pipeline's estimate was earlier than
# MAGIC reality — the train arrived later than predicted (typical for SCADA uniform delay
# MAGIC where the rail offset is set at departure and doesn't grow during the trip).
# MAGIC
# MAGIC Results are written to `gold_rail_ground_truth_comparison` and printed as
# MAGIC summary statistics in the notebook output.

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

GT_TABLE         = f"{CATALOG}.{SCHEMA}.gold_rail_ground_truth"
DWELL_TABLE      = f"{CATALOG}.{SCHEMA}.gold_stop_dwell_fact"
STOP_TABLE       = f"{CATALOG}.{SCHEMA}.silver_dim_stop"
COMPARISON_TABLE = f"{CATALOG}.{SCHEMA}.gold_rail_ground_truth_comparison"

# Rows where the survey GPS position is more than this distance from the recorded
# stop_id's known coordinates are flagged as suspect — likely a "forgot to advance
# the stop" data entry error in the iOS app.
SUSPECT_GPS_DISTANCE_M = 150.0

# COMMAND ----------

# MAGIC %md ## Step 1 — Load ground truth

# COMMAND ----------

gt = spark.table(GT_TABLE)

service_dates = [r.service_date for r in gt.select("service_date").distinct().collect()]
if not service_dates:
    raise RuntimeError(
        f"{GT_TABLE} is empty. Run notebook 60 first to load survey data."
    )

print(f"Ground truth covers {len(service_dates)} service date(s): {sorted(str(d) for d in service_dates)}")
print(f"Ground truth rows: {gt.count()}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Load pipeline stop dwell for matching service dates

# COMMAND ----------

pipeline = (
    spark.table(DWELL_TABLE)
    .filter(F.col("service_date").isin(service_dates))
    .select(
        "service_date",
        F.col("trip_id").alias("gtfs_trip_id"),
        "stop_id",
        "stop_sequence",
        F.col("actual_arrival_ts").alias("pipeline_actual_arrival_ts"),
        F.col("actual_dwell_seconds").alias("pipeline_dwell_seconds"),
        "arrival_delay_seconds",
        "scheduled_arrival_ts",
    )
)

print(f"Pipeline rows for matching dates: {pipeline.count()}")

# COMMAND ----------

# MAGIC %md ## Step 2b — Load stop coordinates for GPS validation

# COMMAND ----------

# silver_dim_stop uses lat/lon columns (not stop_lat/stop_lon)
stop_coords = (
    spark.table(STOP_TABLE)
    .select(
        F.col("stop_id").alias("ref_stop_id"),
        F.col("lat").alias("stop_lat"),
        F.col("lon").alias("stop_lon"),
    )
)

# COMMAND ----------

# MAGIC %md ## Step 3 — Join and compute error metrics

# COMMAND ----------

_R_METERS = F.lit(6_371_000.0)

def _haversine_m(lat1, lon1, lat2, lon2):
    """Haversine distance in metres between two GPS points (Spark column expressions)."""
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    )
    return _R_METERS * 2 * F.asin(F.sqrt(a))

comparison = (
    gt.alias("gt")
    .join(
        pipeline.alias("p"),
        on=["service_date", "gtfs_trip_id", "stop_sequence"],
        how="left",
    )
    # Join known stop coordinates on the survey's recorded stop_id
    .join(
        stop_coords.alias("s"),
        F.col("gt.stop_id") == F.col("s.ref_stop_id"),
        how="left",
    )
    .select(
        # Identity
        "gt.survey_filename",
        "gt.service_date",
        "gt.route_id",
        "gt.direction_id",
        "gt.gtfs_trip_id",
        "gt.stop_id",
        "gt.stop_sequence",
        # Ground truth actuals
        "gt.actual_door_open_ts",
        "gt.actual_door_close_ts",
        "gt.dwell_actual_seconds",
        "gt.gps_accuracy_m",
        # Pipeline estimates
        "p.pipeline_actual_arrival_ts",
        "p.pipeline_dwell_seconds",
        "p.arrival_delay_seconds",
        "p.scheduled_arrival_ts",
        # Error metrics
        # timing_error > 0: train arrived later than pipeline predicted
        # timing_error < 0: train arrived earlier than pipeline predicted
        F.when(
            F.col("gt.actual_door_open_ts").isNotNull()
            & F.col("p.pipeline_actual_arrival_ts").isNotNull(),
            (
                F.unix_timestamp("gt.actual_door_open_ts")
                - F.unix_timestamp("p.pipeline_actual_arrival_ts")
            ).cast("int"),
        ).alias("timing_error_seconds"),
        # dwell_error > 0: actual dwell was longer than pipeline estimated
        F.when(
            F.col("gt.dwell_actual_seconds").isNotNull()
            & F.col("p.pipeline_dwell_seconds").isNotNull(),
            (F.col("gt.dwell_actual_seconds") - F.col("p.pipeline_dwell_seconds")).cast("int"),
        ).alias("dwell_error_seconds"),
        # Abs errors for quick aggregation
        F.when(
            F.col("gt.actual_door_open_ts").isNotNull()
            & F.col("p.pipeline_actual_arrival_ts").isNotNull(),
            F.abs(
                F.unix_timestamp("gt.actual_door_open_ts")
                - F.unix_timestamp("p.pipeline_actual_arrival_ts")
            ).cast("int"),
        ).alias("timing_abs_error_seconds"),
        # GPS position vs known stop location
        F.when(
            F.col("gt.survey_lat").isNotNull()
            & F.col("s.stop_lat").isNotNull(),
            F.round(
                _haversine_m(
                    F.col("gt.survey_lat"), F.col("gt.survey_lon"),
                    F.col("s.stop_lat"),    F.col("s.stop_lon"),
                ),
                1,
            ),
        ).alias("gps_stop_distance_m"),
        # Suspect flag: survey GPS was too far from the recorded stop_id.
        # Most likely cause is the surveyor forgetting to advance to the next stop
        # in the app — the physical location matches a different stop.
        F.when(
            F.col("gt.survey_lat").isNotNull() & F.col("s.stop_lat").isNotNull(),
            _haversine_m(
                F.col("gt.survey_lat"), F.col("gt.survey_lon"),
                F.col("s.stop_lat"),    F.col("s.stop_lon"),
            ) > F.lit(SUSPECT_GPS_DISTANCE_M),
        ).otherwise(F.lit(False)).alias("is_position_suspect"),
    )
)

# COMMAND ----------

# MAGIC %md ## Step 4 — Write comparison table (overwrite per service_date)
# MAGIC
# MAGIC > **First-run note:** If the table already exists with the old schema (missing
# MAGIC > `gps_stop_distance_m` / `is_position_suspect`), drop it once:
# MAGIC > `DROP TABLE IF EXISTS gold_rail_ground_truth_comparison`

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {COMPARISON_TABLE} (
        survey_filename             STRING,
        service_date                DATE,
        route_id                    STRING,
        direction_id                INT,
        gtfs_trip_id                STRING,
        stop_id                     STRING,
        stop_sequence               INT,
        actual_door_open_ts         TIMESTAMP,
        actual_door_close_ts        TIMESTAMP,
        dwell_actual_seconds        INT,
        gps_accuracy_m              DOUBLE,
        pipeline_actual_arrival_ts  TIMESTAMP,
        pipeline_dwell_seconds      INT,
        arrival_delay_seconds       INT,
        scheduled_arrival_ts        TIMESTAMP,
        timing_error_seconds        INT,
        dwell_error_seconds         INT,
        timing_abs_error_seconds    INT,
        gps_stop_distance_m         DOUBLE,
        is_position_suspect         BOOLEAN
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# replaceWhere for idempotent partitioned overwrite
(
    comparison
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"service_date IN ({', '.join(repr(str(d)) for d in service_dates)})")
    .saveAsTable(COMPARISON_TABLE)
)

print(f"Wrote {comparison.count()} rows to {COMPARISON_TABLE}")

# COMMAND ----------

# MAGIC %md ## Step 5 — Summary statistics

# COMMAND ----------

_timing_cols = """
        COUNT(*)                                         AS stops_matched,
        SUM(CASE WHEN is_position_suspect THEN 1 ELSE 0 END) AS suspect_stops,
        COUNT(timing_error_seconds)                      AS stops_with_timing,
        ROUND(AVG(timing_error_seconds), 1)              AS mean_error_s,
        ROUND(PERCENTILE(timing_error_seconds, 0.5), 1)  AS p50_error_s,
        ROUND(PERCENTILE(timing_error_seconds, 0.9), 1)  AS p90_error_s,
        MIN(timing_error_seconds)                        AS min_error_s,
        MAX(timing_error_seconds)                        AS max_error_s,
        ROUND(STDDEV(timing_error_seconds), 1)           AS stddev_s
"""

print("=" * 60)
print("TIMING ERROR — ALL ROWS (incl. suspect)")
print("Positive = train arrived LATER than pipeline predicted")
print("=" * 60)
spark.sql(f"""
    SELECT service_date, route_id, direction_id, gtfs_trip_id, {_timing_cols}
    FROM {COMPARISON_TABLE}
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3
""").show(truncate=False)

print("=" * 60)
print(f"TIMING ERROR — CLEAN ONLY (gps_stop_distance_m <= {SUSPECT_GPS_DISTANCE_M}m)")
print("=" * 60)
spark.sql(f"""
    SELECT service_date, route_id, direction_id, gtfs_trip_id, {_timing_cols}
    FROM {COMPARISON_TABLE}
    WHERE NOT is_position_suspect
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3
""").show(truncate=False)

# COMMAND ----------

print("=" * 60)
print("PER-STOP DETAIL (sorted by stop_sequence)")
print("=" * 60)

display(
    spark.sql(f"""
        SELECT
            service_date,
            route_id,
            direction_id,
            gtfs_trip_id,
            stop_sequence,
            stop_id,
            is_position_suspect,
            ROUND(gps_stop_distance_m, 0) AS gps_stop_distance_m,
            actual_door_open_ts,
            pipeline_actual_arrival_ts,
            timing_error_seconds,
            dwell_actual_seconds,
            pipeline_dwell_seconds,
            dwell_error_seconds,
            scheduled_arrival_ts,
            arrival_delay_seconds,
            ROUND(gps_accuracy_m, 1) AS gps_accuracy_m
        FROM {COMPARISON_TABLE}
        ORDER BY service_date, route_id, direction_id, stop_sequence
    """)
)

# COMMAND ----------

print("=" * 60)
print("UNMATCHED GROUND TRUTH STOPS (no pipeline row found)")
print("=" * 60)

unmatched = spark.sql(f"""
    SELECT
        survey_filename,
        service_date,
        route_id,
        direction_id,
        gtfs_trip_id,
        stop_sequence,
        stop_id,
        actual_door_open_ts
    FROM {COMPARISON_TABLE}
    WHERE pipeline_actual_arrival_ts IS NULL
    ORDER BY service_date, route_id, stop_sequence
""")

count = unmatched.count()
print(f"{count} unmatched stop(s)")
if count > 0:
    display(unmatched)
    print(
        "\nPossible causes:\n"
        "  - gtfs_trip_id resolution mismatch (check notebook 60 scoring output)\n"
        "  - Stop served but not in TripUpdates feed (rail skips some stops)\n"
        "  - Gold 40 ran before this service_date was available\n"
        "  - stop_sequence mismatch between GTFS and feed"
    )
