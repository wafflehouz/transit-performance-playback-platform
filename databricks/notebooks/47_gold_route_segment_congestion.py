# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 47 — Gold: Route Segment Congestion (Nightly Batch)
# MAGIC
# MAGIC **Schedule:** Nightly, after notebook 43 completes
# MAGIC **Dependencies:** `gold_trip_path_fact`
# MAGIC **Grain:** `(service_date, time_bucket_15min, h3_index)`
# MAGIC
# MAGIC **What it does:**
# MAGIC - Reads VP points from `gold_trip_path_fact` (already GPS-cleaned, ≤20 m/s)
# MAGIC - Assigns each point an H3 index at resolution 9 (~150m hexagons)
# MAGIC - Buckets point_ts into 15-minute windows
# MAGIC - Aggregates speed stats and vehicle count per hex+bucket
# MAGIC - Classifies congestion level from p10 speed (slowest 10% of readings)
# MAGIC
# MAGIC **Why p10 speed for congestion classification:**
# MAGIC Using the 10th-percentile speed rather than average prevents a few fast
# MAGIC outliers from masking genuine congestion. If 10% of readings in a hex
# MAGIC are below the congestion threshold, the corridor is congested.
# MAGIC
# MAGIC **H3 Resolution 9:**
# MAGIC ~150m hex edge length. Fine enough to distinguish city blocks, coarse
# MAGIC enough that a 15-min bucket accumulates enough VP points per hex to be
# MAGIC statistically meaningful for Valley Metro's ~100 active routes.
# MAGIC
# MAGIC **Congestion thresholds (Phoenix operating context):**
# MAGIC | Level      | Speed       | Operational meaning              |
# MAGIC |------------|-------------|----------------------------------|
# MAGIC | free_flow  | ≥ 11 m/s    | > 25 mph — unconstrained flow    |
# MAGIC | moderate   | 7–11 m/s    | 15–25 mph — light friction       |
# MAGIC | congested  | 3–7 m/s     | 7–15 mph — heavy congestion      |
# MAGIC | severe     | < 3 m/s     | < 7 mph — near-gridlock          |
# MAGIC
# MAGIC **Frontend use:**
# MAGIC Deck.gl `H3HexagonLayer` consumes `h3_index` + `congestion_level` directly.
# MAGIC The playback UI filters by `service_date` + `time_bucket_15min` to render
# MAGIC the traffic overlay at each scrubber position alongside bus positions.

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
print(f"Building congestion grid for service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 1 — Read Trip Path Points

# COMMAND ----------

path = (
    spark.table(GOLD_TRIP_PATH_FACT)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
    .filter(F.col("speed_mps").isNotNull())
    .select("service_date", "trip_id", "point_ts", "lat", "lon", "speed_mps")
)

point_count = path.count()
print(f"VP points for {target_date}: {point_count:,}")

if point_count == 0:
    print("⚠ No trip path data — run notebook 43 first.")
    dbutils.notebook.exit(f"No data for {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Assign H3 Index and 15-min Time Bucket
# MAGIC
# MAGIC `h3_longlatash3(longitude, latitude, resolution)` is a Databricks built-in
# MAGIC SQL function. Note argument order: longitude first, then latitude.
# MAGIC
# MAGIC 15-min bucket: floor unix timestamp to nearest 900 seconds.

# COMMAND ----------

enriched = (
    path
    .withColumn(
        "h3_index",
        F.expr("h3_longlatash3(lon, lat, 9)")
    )
    .withColumn(
        "time_bucket_15min",
        F.to_timestamp(
            (F.unix_timestamp("point_ts") / 900).cast("long") * 900
        )
    )
)

# COMMAND ----------

# MAGIC %md ## Step 3 — Aggregate to (service_date, time_bucket_15min, h3_index)
# MAGIC
# MAGIC - `vehicle_count` = distinct trips observed in this hex+bucket
# MAGIC   (one trip generates many VP points; distinct trip_id avoids inflation)
# MAGIC - `p10_speed_mps` = 10th-percentile speed — congestion signal
# MAGIC - `avg_speed_mps` = average speed — trend reference

# COMMAND ----------

congestion = (
    enriched
    .groupBy("service_date", "time_bucket_15min", "h3_index")
    .agg(
        F.countDistinct("trip_id").cast("int").alias("vehicle_count"),
        F.round(F.avg("speed_mps"), 2).alias("avg_speed_mps"),
        F.round(
            F.percentile_approx("speed_mps", 0.1, accuracy=100), 2
        ).alias("p10_speed_mps"),
    )
    # Require at least 2 distinct vehicles to reduce single-trip noise
    .filter(F.col("vehicle_count") >= 2)
    .withColumn(
        "congestion_level",
        F.when(F.col("p10_speed_mps") >= 11.0, F.lit("free_flow"))
         .when(F.col("p10_speed_mps") >= 7.0,  F.lit("moderate"))
         .when(F.col("p10_speed_mps") >= 3.0,  F.lit("congested"))
         .otherwise(F.lit("severe"))
    )
)

hex_count = congestion.count()
print(f"Hex+bucket rows for {target_date}: {hex_count:,}")

if hex_count == 0:
    print("⚠ No congestion rows produced — check trip path data.")
    dbutils.notebook.exit(f"No congestion data for {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 4 — Write

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_ROUTE_SEGMENT_CONGESTION} (
        service_date       DATE,
        time_bucket_15min  TIMESTAMP,
        h3_index           STRING,
        vehicle_count      INT,
        avg_speed_mps      DOUBLE,
        p10_speed_mps      DOUBLE,
        congestion_level   STRING
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

(
    congestion
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"service_date = '{target_date}'")
    .option("overwriteSchema", "false")
    .saveAsTable(GOLD_ROUTE_SEGMENT_CONGESTION)
)

print(f"✓ Written to {GOLD_ROUTE_SEGMENT_CONGESTION} for service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 5 — Quality Report

# COMMAND ----------

written = (
    spark.table(GOLD_ROUTE_SEGMENT_CONGESTION)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

total_hexes   = written.count()
total_buckets = written.select("time_bucket_15min").distinct().count()

print(f"""
=== Route Segment Congestion: {target_date} ===
  Total hex+bucket rows : {total_hexes:>10,}
  Distinct time buckets : {total_buckets:>10,}
  Avg rows per bucket   : {total_hexes // max(total_buckets, 1):>10,}
""")

print("Congestion level distribution:")
(
    written
    .groupBy("congestion_level")
    .count()
    .withColumn("pct", F.round(F.col("count") * 100.0 / total_hexes, 1))
    .orderBy("congestion_level")
    .show()
)

print("Congestion by hour (Phoenix local):")
(
    written
    .groupBy(
        F.expr("MOD(HOUR(TIMESTAMPADD(HOUR, -7, time_bucket_15min)) + 24, 24)").alias("phoenix_hour"),
        "congestion_level"
    )
    .count()
    .orderBy("phoenix_hour", "congestion_level")
    .show(48)
)

print("Sample — most congested hexes (avg speed, all buckets):")
(
    written
    .groupBy("h3_index")
    .agg(
        F.round(F.avg("avg_speed_mps"), 2).alias("avg_speed_mps"),
        F.round(F.avg("p10_speed_mps"), 2).alias("avg_p10_speed_mps"),
        F.sum("vehicle_count").alias("total_vehicle_obs"),
    )
    .orderBy("avg_p10_speed_mps")
    .limit(15)
    .show(truncate=False)
)
