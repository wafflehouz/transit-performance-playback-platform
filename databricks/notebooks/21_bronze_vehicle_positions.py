# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 21 — Bronze: Vehicle Positions (AutoLoader Streaming)
# MAGIC
# MAGIC **Type:** Long-running streaming job (Databricks Workflow, continuous cluster)
# MAGIC
# MAGIC **What it does:**
# MAGIC - Uses AutoLoader to detect new NDJSON snapshot files from the VP poller (nb 11)
# MAGIC - Normalizes all position fields into flat columns
# MAGIC - Computes an `event_hash` based on (vehicle_id, lat/lon rounded to 5 decimal
# MAGIC   places, bearing) — Silver will use this to downsample to state changes only
# MAGIC - Appends to `bronze_vehicle_position_events`, partitioned by `service_date`
# MAGIC
# MAGIC **Note on speed units:**
# MAGIC The mecatran feed provides `speed` in m/s (GTFS-RT standard). Gold and UI will
# MAGIC display in mph. Conversion: mph = m/s × 2.23694.

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# COMMAND ----------

# MAGIC %md ## Schema Definition

# COMMAND ----------

vehicle_inner_schema = StructType([
    StructField("id",    StringType(), True),
    StructField("label", StringType(), True),   # nearest stop name at report time
])

trip_schema = StructType([
    StructField("tripId",      StringType(),  True),
    StructField("routeId",     StringType(),  True),
    StructField("directionId", IntegerType(), True),
])

position_schema = StructType([
    StructField("latitude",  DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("bearing",   DoubleType(), True),
    StructField("speed",     DoubleType(), True),   # m/s
    StructField("odometer",  DoubleType(), True),
])

vehicle_outer_schema = StructType([
    StructField("trip",               trip_schema,          True),
    StructField("position",           position_schema,      True),
    StructField("currentStopSequence", IntegerType(),       True),
    StructField("currentStatus",       StringType(),        True),
    StructField("timestamp",           StringType(),        True),
    StructField("stopId",              StringType(),        True),
    StructField("vehicle",             vehicle_inner_schema, True),
])

file_schema = StructType([
    StructField("feed_ts",     StringType(),        True),
    StructField("snapshot_ts", StringType(),        True),
    StructField("id",          StringType(),        True),
    StructField("vehicle",     vehicle_outer_schema, True),
])

# COMMAND ----------

# MAGIC %md ## AutoLoader Read Stream

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{VEHICLE_POSITIONS_CHECKPOINT}/schema")
    .schema(file_schema)
    .option("cloudFiles.includeExistingFiles", "true")
    .load(VEHICLE_POSITIONS_SNAPSHOTS_PATH)
    # Note: use _metadata.file_path (not input_file_name()) for Unity Catalog compatibility
)

# COMMAND ----------

# MAGIC %md ## Transform

# COMMAND ----------

def transform_vehicle_positions(df):
    """
    Flatten vehicle position entity fields.
    Produces one row per (snapshot, vehicle).
    """
    return (
        df
        # Safety filter — poller already filters, but be defensive
        .filter(F.col("vehicle.trip").isNotNull())
        .filter(F.col("vehicle.vehicle.id").isNotNull())

        .select(
            # ── Ingestion metadata ──────────────────────────────────────────
            F.to_timestamp(F.col("feed_ts").cast("long")).alias("feed_ts"),
            F.to_timestamp(F.col("snapshot_ts")).alias("ingest_ts"),
            F.col("_metadata.file_path").alias("source_file"),

            # ── Vehicle / Trip context ──────────────────────────────────────
            F.col("vehicle.vehicle.id").alias("vehicle_id"),
            F.col("vehicle.trip.tripId").alias("trip_id"),
            F.col("vehicle.trip.routeId").alias("route_id"),
            F.col("vehicle.trip.directionId").alias("direction_id"),

            # ── Position ────────────────────────────────────────────────────
            F.col("vehicle.position.latitude").alias("lat"),
            F.col("vehicle.position.longitude").alias("lon"),
            F.col("vehicle.position.bearing").alias("bearing"),
            F.col("vehicle.position.speed").alias("speed_mps"),   # m/s
            F.col("vehicle.position.odometer").alias("odometer"),

            # ── Stop context ────────────────────────────────────────────────
            F.col("vehicle.currentStopSequence").alias("current_stop_sequence"),
            F.col("vehicle.currentStatus").alias("current_status"),
            F.col("vehicle.stopId").alias("stop_id"),

            # ── Derive service_date from vehicle-reported timestamp ──────────
            # vehicle.timestamp is epoch seconds (UTC). Convert to Phoenix local
            # time (America/Phoenix = UTC-7, no DST) before extracting the date,
            # so service_date matches the GTFS startDate used in TripUpdates.
            F.to_date(
                F.from_utc_timestamp(
                    F.from_unixtime(F.col("vehicle.timestamp").cast("long")),
                    "America/Phoenix"
                )
            ).alias("service_date"),

            F.to_timestamp(
                F.col("vehicle.timestamp").cast("long")
            ).alias("vehicle_ts"),

            # ── Event hash ──────────────────────────────────────────────────
            # Rounded lat/lon (5 decimal places ≈ 1.1m precision) to avoid
            # hash flicker from floating-point jitter in a parked vehicle.
            F.sha2(
                F.concat_ws("|",
                    F.col("vehicle.vehicle.id"),
                    F.round(F.col("vehicle.position.latitude"),  5).cast("string"),
                    F.round(F.col("vehicle.position.longitude"), 5).cast("string"),
                    F.coalesce(F.col("vehicle.position.bearing").cast("string"), F.lit("")),
                    F.coalesce(F.col("vehicle.currentStatus"), F.lit("")),
                ),
                256
            ).alias("event_hash"),
        )
        .filter(F.col("vehicle_id").isNotNull())
        .filter(F.col("trip_id").isNotNull())
    )

# COMMAND ----------

# MAGIC %md ## Write to Bronze Delta

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_VEHICLE_POSITIONS} (
        feed_ts              TIMESTAMP,
        ingest_ts            TIMESTAMP,
        source_file          STRING,
        vehicle_id           STRING,
        trip_id              STRING,
        route_id             STRING,
        direction_id         INT,
        lat                  DOUBLE,
        lon                  DOUBLE,
        bearing              DOUBLE,
        speed_mps            DOUBLE,
        odometer             DOUBLE,
        current_stop_sequence INT,
        current_status       STRING,
        stop_id              STRING,
        service_date         DATE,
        vehicle_ts           TIMESTAMP,
        event_hash           STRING
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

# COMMAND ----------

TRIGGER_MODE = "availableNow"   # ← change to "continuous" for production

if TRIGGER_MODE == "availableNow":
    trigger_opts = {"availableNow": True}
else:
    trigger_opts = {"processingTime": "60 seconds"}

query = (
    raw_stream
    .transform(transform_vehicle_positions)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", VEHICLE_POSITIONS_CHECKPOINT)
    .trigger(**trigger_opts)
    .toTable(BRONZE_VEHICLE_POSITIONS)
)

query.awaitTermination()
print(f"✓ Bronze VehiclePositions stream complete. Table: {BRONZE_VEHICLE_POSITIONS}")

# COMMAND ----------

# MAGIC %md ## Quick Validation

# COMMAND ----------

bronze_df = spark.table(BRONZE_VEHICLE_POSITIONS)
row_count = bronze_df.count()
print(f"Total rows in {BRONZE_VEHICLE_POSITIONS}: {row_count:,}")

if row_count > 0:
    print("\nPartitions (service_date):")
    bronze_df.groupBy("service_date").count().orderBy("service_date").show()

    print("\nActive routes in Bronze (sample):")
    (
        bronze_df
        .groupBy("route_id").count()
        .orderBy(F.desc("count"))
        .limit(20)
        .show()
    )

    print("\nSpeed distribution (m/s):")
    bronze_df.filter(F.col("speed_mps") > 0).select(
        F.min("speed_mps").alias("min"),
        F.avg("speed_mps").alias("avg"),
        F.max("speed_mps").alias("max"),
        F.percentile_approx("speed_mps", 0.5).alias("p50"),
        F.percentile_approx("speed_mps", 0.9).alias("p90"),
    ).show()
