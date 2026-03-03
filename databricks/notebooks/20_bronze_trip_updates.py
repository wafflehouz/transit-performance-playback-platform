# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 20 — Bronze: Trip Updates (AutoLoader Streaming)
# MAGIC
# MAGIC **Type:** Long-running streaming job (Databricks Workflow, continuous cluster)
# MAGIC
# MAGIC **What it does:**
# MAGIC - Uses AutoLoader (`cloudFiles`) to detect new NDJSON snapshot files written
# MAGIC   by the poller (notebook 10)
# MAGIC - Explodes each entity's `stopTimeUpdate` array → one row per (trip, stop)
# MAGIC - Computes an `event_hash` for each row to support downstream deduplication
# MAGIC - Appends to `bronze_trip_updates_events`, partitioned by `service_date`
# MAGIC
# MAGIC **Trigger mode:**
# MAGIC - Run with `trigger(availableNow=True)` to process all pending files and stop
# MAGIC   (use this for backfill or testing)
# MAGIC - Run with `trigger(processingTime="60 seconds")` for continuous streaming
# MAGIC   (use this for the production Workflow job)
# MAGIC
# MAGIC **Bronze philosophy:** Append-only, no deduplication here. All raw data is
# MAGIC preserved. Silver is where we deduplicate and filter state changes only.

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, MapType
)

# COMMAND ----------

# MAGIC %md ## Schema Definition
# MAGIC
# MAGIC Explicit schema prevents AutoLoader from needing a schema inference scan.
# MAGIC Matches the NDJSON format written by the poller.

# COMMAND ----------

stop_time_update_schema = ArrayType(
    StructType([
        StructField("stopSequence",          IntegerType(), True),
        StructField("stopId",                StringType(),  True),
        StructField("scheduleRelationship",  StringType(),  True),
        StructField("arrival", StructType([
            StructField("delay", IntegerType(), True),
            StructField("time",  StringType(),  True),
        ]), True),
        StructField("departure", StructType([
            StructField("delay", IntegerType(), True),
            StructField("time",  StringType(),  True),
        ]), True),
    ])
)

trip_update_schema = StructType([
    StructField("trip", StructType([
        StructField("tripId",               StringType(),  True),
        StructField("startDate",            StringType(),  True),   # "YYYYMMDD"
        StructField("routeId",              StringType(),  True),
        StructField("directionId",          IntegerType(), True),
        StructField("scheduleRelationship", StringType(),  True),
    ]), True),
    StructField("vehicle", StructType([
        StructField("id",    StringType(), True),
        StructField("label", StringType(), True),
    ]), True),
    StructField("stopTimeUpdate", stop_time_update_schema, True),
    StructField("timestamp",      StringType(), True),
    StructField("tripProperties", MapType(StringType(), StringType()), True),
])

file_schema = StructType([
    StructField("feed_ts",     StringType(), True),
    StructField("snapshot_ts", StringType(), True),
    StructField("id",          StringType(), True),
    StructField("tripUpdate",  trip_update_schema, True),
])

# COMMAND ----------

# MAGIC %md ## AutoLoader Read Stream

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{TRIP_UPDATES_CHECKPOINT}/schema")
    # Provide explicit schema — faster startup, no inference scan
    .schema(file_schema)
    # Include file metadata (path, modification time) for lineage
    .option("cloudFiles.includeExistingFiles", "true")
    .load(TRIP_UPDATES_SNAPSHOTS_PATH)
)

# COMMAND ----------

# MAGIC %md ## Transform: Explode stopTimeUpdate → one row per stop

# COMMAND ----------

def transform_trip_updates(df):
    """
    Explode stopTimeUpdate array and extract all fields to flat columns.
    Produces one row per (snapshot, trip, stop_sequence).
    """
    return (
        df
        # Explode the stopTimeUpdate array
        .withColumn("stu", F.explode(F.col("tripUpdate.stopTimeUpdate")))

        # Drop trips with no stop-time updates (shouldn't happen but be safe)
        .filter(F.col("stu").isNotNull())

        .select(
            # ── Ingestion metadata ──────────────────────────────────────────
            F.to_timestamp(F.col("feed_ts").cast("long")).alias("feed_ts"),
            F.to_timestamp(F.col("snapshot_ts")).alias("ingest_ts"),
            F.col("_metadata.file_path").alias("source_file"),

            # ── Trip context ────────────────────────────────────────────────
            F.to_date(
                F.col("tripUpdate.trip.startDate"), "yyyyMMdd"
            ).alias("service_date"),
            F.col("tripUpdate.trip.tripId").alias("trip_id"),
            F.col("tripUpdate.trip.routeId").alias("route_id"),
            F.col("tripUpdate.trip.directionId").alias("direction_id"),
            F.col("tripUpdate.trip.scheduleRelationship").alias("trip_schedule_relationship"),
            F.col("tripUpdate.vehicle.id").alias("vehicle_id"),

            # ── Stop-time update ─────────────────────────────────────────────
            F.col("stu.stopSequence").alias("stop_sequence"),
            F.col("stu.stopId").alias("stop_id"),
            F.col("stu.scheduleRelationship").alias("stop_schedule_relationship"),

            # Arrival
            F.col("stu.arrival.delay").alias("arrival_delay_seconds"),
            F.to_timestamp(
                F.col("stu.arrival.time").cast("long")
            ).alias("arrival_ts"),

            # Departure
            F.col("stu.departure.delay").alias("departure_delay_seconds"),
            F.to_timestamp(
                F.col("stu.departure.time").cast("long")
            ).alias("departure_ts"),

            # ── Event hash (for downstream dedup / state-change detection) ──
            # Hash over the fields that define a meaningful state change.
            # If this hash is the same as a previous snapshot for the same
            # (trip_id, stop_sequence), nothing changed.
            F.sha2(
                F.concat_ws("|",
                    F.col("tripUpdate.trip.tripId"),
                    F.col("stu.stopSequence").cast("string"),
                    F.coalesce(F.col("stu.arrival.time"),  F.lit("")),
                    F.coalesce(F.col("stu.departure.time"), F.lit("")),
                    F.coalesce(F.col("stu.arrival.delay").cast("string"), F.lit("")),
                    F.coalesce(F.col("stu.scheduleRelationship"), F.lit("")),
                ),
                256
            ).alias("event_hash"),
        )
        # Drop rows with no trip_id (malformed entities)
        .filter(F.col("trip_id").isNotNull())
    )

# COMMAND ----------

# MAGIC %md ## Write to Bronze Delta

# COMMAND ----------

# Create the table with partitioning if it doesn't already exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TRIP_UPDATES} (
        feed_ts                      TIMESTAMP,
        ingest_ts                    TIMESTAMP,
        source_file                  STRING,
        service_date                 DATE,
        trip_id                      STRING,
        route_id                     STRING,
        direction_id                 INT,
        trip_schedule_relationship   STRING,
        vehicle_id                   STRING,
        stop_sequence                INT,
        stop_id                      STRING,
        stop_schedule_relationship   STRING,
        arrival_delay_seconds        INT,
        arrival_ts                   TIMESTAMP,
        departure_delay_seconds      INT,
        departure_ts                 TIMESTAMP,
        event_hash                   STRING
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

# COMMAND ----------

# Change to trigger(processingTime="60 seconds") for continuous production streaming.
# Use trigger(availableNow=True) to drain the backlog and stop (testing / backfill).
TRIGGER_MODE = "availableNow"   # ← change to "continuous" for production

if TRIGGER_MODE == "availableNow":
    trigger_opts = {"availableNow": True}
else:
    trigger_opts = {"processingTime": "60 seconds"}

query = (
    raw_stream
    .transform(transform_trip_updates)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", TRIP_UPDATES_CHECKPOINT)
    .trigger(**trigger_opts)
    .toTable(BRONZE_TRIP_UPDATES)
)

query.awaitTermination()
print(f"✓ Bronze TripUpdates stream complete. Table: {BRONZE_TRIP_UPDATES}")

# COMMAND ----------

# MAGIC %md ## Quick Validation

# COMMAND ----------

bronze_df = spark.table(BRONZE_TRIP_UPDATES)
row_count = bronze_df.count()
print(f"Total rows in {BRONZE_TRIP_UPDATES}: {row_count:,}")

if row_count > 0:
    print("\nPartitions (service_date):")
    bronze_df.groupBy("service_date").count().orderBy("service_date").show()

    print("Sample rows:")
    bronze_df.orderBy(F.desc("ingest_ts")).limit(5).show(truncate=False)
