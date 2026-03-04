# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 43 — Gold: Trip Path Fact (Nightly Batch)
# MAGIC
# MAGIC **Schedule:** Nightly, after notebook 41 completes (parallel with 42)
# MAGIC **Dependencies:** `silver_fact_vehicle_positions`, `gold_trip_timeline_fact`
# MAGIC **Grain:** `(service_date, trip_id, point_seq)`
# MAGIC
# MAGIC **What it does:**
# MAGIC - Joins Silver VP rows to confirmed trips from trip_timeline_fact
# MAGIC - Filters to trips that have actual start data (excludes orphan VP records)
# MAGIC - Assigns a sequential `point_seq` per trip ordered by event_ts
# MAGIC - Produces the ordered position sequence used by the playback map
# MAGIC
# MAGIC **Note:** Silver VP already downsampled to 30-second buckets. No further
# MAGIC downsampling is applied here. ZORDER BY (trip_id) should be run weekly
# MAGIC via OPTIMIZE for efficient playback queries.

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import date, timedelta

# COMMAND ----------

dbutils.widgets.text("target_date", (date.today() - timedelta(days=1)).isoformat())
target_date = dbutils.widgets.get("target_date")
print(f"Processing service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 1 — Get Confirmed Trip IDs from Trip Timeline

# COMMAND ----------

confirmed_trips = (
    spark.table(GOLD_TRIP_TIMELINE_FACT)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
    .filter(F.col("actual_start_ts").isNotNull())
    .select("trip_id")
)

trip_count = confirmed_trips.count()
print(f"Confirmed trips for {target_date}: {trip_count:,}")

if trip_count == 0:
    print("⚠ No confirmed trip data. Run notebook 41 first.")
    dbutils.notebook.exit(f"No data for {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Read Silver VP and Filter to Confirmed Trips
# MAGIC
# MAGIC Using a left semi-join: keeps VP rows whose trip_id exists in confirmed_trips,
# MAGIC discards all others. route_id and direction_id come from Silver VP directly.

# COMMAND ----------

vp = (
    spark.table(SILVER_FACT_VEHICLE_POSITIONS)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
    .select(
        "service_date",
        "trip_id",
        "route_id",
        "direction_id",
        F.col("event_ts").alias("point_ts"),
        "lat",
        "lon",
        "speed_mps",
        "bearing",
    )
)

vp_confirmed = vp.join(confirmed_trips, on="trip_id", how="left_semi")

# Filter out GPS outliers. Phoenix speed limit is 35 mph (15.6 m/s).
# Anything above 20 m/s (~45 mph) is erroneous GPS data and would cause
# vehicles to appear to teleport on the playback map.
vp_confirmed = vp_confirmed.filter(F.col("speed_mps") <= 20.0)

vp_count = vp_confirmed.count()
print(f"VP rows for confirmed trips: {vp_count:,}")

# COMMAND ----------

# MAGIC %md ## Step 3 — Assign point_seq
# MAGIC
# MAGIC row_number() ordered by point_ts within each trip gives a 1-based sequence.
# MAGIC Silver VP is already deduplicated to 30-second buckets, so no ties exist.

# COMMAND ----------

w_path = Window.partitionBy("trip_id").orderBy("point_ts")

gold_path = (
    vp_confirmed
    .withColumn("point_seq", F.row_number().over(w_path).cast("int"))
    .select(
        "service_date",
        "trip_id",
        "route_id",
        "direction_id",
        "point_ts",
        "lat",
        "lon",
        "speed_mps",
        "bearing",
        "point_seq",
    )
)

# COMMAND ----------

# MAGIC %md ## Step 4 — Write

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TRIP_PATH_FACT} (
        service_date   DATE,
        trip_id        STRING,
        route_id       STRING,
        direction_id   INT,
        point_ts       TIMESTAMP,
        lat            DOUBLE,
        lon            DOUBLE,
        speed_mps      DOUBLE,
        bearing        DOUBLE,
        point_seq      INT
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

(
    gold_path
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"service_date = '{target_date}'")
    .option("overwriteSchema", "false")
    .saveAsTable(GOLD_TRIP_PATH_FACT)
)

print(f"✓ Written to {GOLD_TRIP_PATH_FACT} for service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 5 — Quality Report

# COMMAND ----------

written = (
    spark.table(GOLD_TRIP_PATH_FACT)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

total_points   = written.count()
distinct_trips = written.select("trip_id").distinct().count()
avg_pts        = total_points / distinct_trips if distinct_trips > 0 else 0

print(f"""
=== Gold Trip Path Fact: {target_date} ===
  Total position points   : {total_points:>10,}
  Distinct trips          : {distinct_trips:>10,}
  Avg points per trip     : {avg_pts:>10.1f}
""")

print("Coverage by route (top 10):")
(
    written
    .groupBy("route_id")
    .agg(
        F.countDistinct("trip_id").alias("trips"),
        F.count("*").alias("points"),
    )
    .orderBy(F.desc("trips"))
    .limit(10)
    .show()
)

print("Speed stats (m/s, moving vehicles):")
written.filter(F.col("speed_mps") > 0).select(
    F.percentile_approx("speed_mps", 0.5).alias("p50_mps"),
    F.percentile_approx("speed_mps", 0.9).alias("p90_mps"),
    F.max("speed_mps").alias("max_mps"),
    (F.percentile_approx("speed_mps", 0.5) * 2.23694).alias("p50_mph"),
    (F.max("speed_mps") * 2.23694).alias("max_mph"),
).show()
