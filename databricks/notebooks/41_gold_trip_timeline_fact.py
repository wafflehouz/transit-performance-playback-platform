# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 41 — Gold: Trip Timeline Fact (Nightly Batch)
# MAGIC
# MAGIC **Schedule:** Nightly, after notebook 40 completes
# MAGIC **Dependencies:** `gold_stop_dwell_fact`
# MAGIC **Grain:** `(service_date, trip_id)`
# MAGIC
# MAGIC **What it does:**
# MAGIC - Aggregates stop_dwell_fact to the trip level
# MAGIC - Derives planned/actual start and end timestamps using first/last stop logic
# MAGIC - Computes late_start_seconds, total_trip_delay_seconds, dwell_total
# MAGIC - Flags trips with no actual data (schedule-only) for downstream filtering

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

# MAGIC %md ## Step 1 — Read Gold Stop Dwell Fact

# COMMAND ----------

dwell = (
    spark.table(GOLD_STOP_DWELL_FACT)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

row_count = dwell.count()
print(f"Stop dwell rows for {target_date}: {row_count:,}")

if row_count == 0:
    print("⚠ No stop dwell data. Run notebook 40 first.")
    dbutils.notebook.exit(f"No data for {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Pre-compute Last Stop Delay
# MAGIC
# MAGIC `total_trip_delay_seconds` = arrival_delay at the last stop.
# MAGIC Cannot be derived with a simple aggregation — requires knowing which
# MAGIC stop_sequence is highest per trip, then extracting its delay.

# COMMAND ----------

w_last = Window.partitionBy("trip_id").orderBy(F.desc("stop_sequence"))

dwell_with_last = dwell.withColumn(
    "last_stop_delay",
    F.when(
        F.row_number().over(w_last) == 1,
        F.col("arrival_delay_seconds")
    ).otherwise(F.lit(None).cast("int"))
)

# COMMAND ----------

# MAGIC %md ## Step 3 — Aggregate to Trip Grain
# MAGIC
# MAGIC - `planned_start_ts`: min of COALESCE(scheduled_arrival, scheduled_departure)
# MAGIC   First stop has null scheduled_arrival, so coalesce falls back to departure.
# MAGIC - `actual_start_ts`: same pattern with actual timestamps.
# MAGIC - `planned_end_ts`: max of COALESCE(scheduled_departure, scheduled_arrival)
# MAGIC   Last stop has null scheduled_departure, so coalesce falls back to arrival.
# MAGIC - `actual_end_ts`: same pattern.

# COMMAND ----------

timeline = (
    dwell_with_last
    .groupBy("service_date", "trip_id", "route_id", "direction_id")
    .agg(
        F.min(
            F.coalesce("scheduled_arrival_ts", "scheduled_departure_ts")
        ).alias("planned_start_ts"),

        F.min(
            F.coalesce("actual_arrival_ts", "actual_departure_ts")
        ).alias("actual_start_ts"),

        F.max(
            F.coalesce("scheduled_departure_ts", "scheduled_arrival_ts")
        ).alias("planned_end_ts"),

        F.max(
            F.coalesce("actual_departure_ts", "actual_arrival_ts")
        ).alias("actual_end_ts"),

        F.first("last_stop_delay", ignorenulls=True).alias("total_trip_delay_seconds"),

        F.sum(
            F.coalesce(F.col("actual_dwell_seconds"), F.lit(0))
        ).cast("int").alias("dwell_total_seconds"),

        F.count("stop_sequence").cast("int").alias("stop_count"),
    )
)

# COMMAND ----------

# MAGIC %md ## Step 4 — Compute late_start and data_quality_flag

# COMMAND ----------

timeline = (
    timeline
    .withColumn(
        "late_start_seconds",
        F.when(
            F.col("actual_start_ts").isNotNull() & F.col("planned_start_ts").isNotNull(),
            (
                F.unix_timestamp("actual_start_ts") - F.unix_timestamp("planned_start_ts")
            ).cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )
    .withColumn(
        "data_quality_flag",
        F.when(F.col("actual_start_ts").isNull(),  F.lit("NO_ACTUAL_DATA"))
         .when(F.col("stop_count") < 2,            F.lit("SINGLE_STOP_TRIP"))
         .otherwise(F.lit(None).cast("string"))
    )
)

# COMMAND ----------

# MAGIC %md ## Step 5 — Final Select and Write

# COMMAND ----------

gold_timeline = timeline.select(
    "service_date",
    "trip_id",
    "route_id",
    "direction_id",
    "planned_start_ts",
    "actual_start_ts",
    "planned_end_ts",
    "actual_end_ts",
    "late_start_seconds",
    "total_trip_delay_seconds",
    "dwell_total_seconds",
    "stop_count",
    "data_quality_flag",
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TRIP_TIMELINE_FACT} (
        service_date                DATE,
        trip_id                     STRING,
        route_id                    STRING,
        direction_id                INT,
        planned_start_ts            TIMESTAMP,
        actual_start_ts             TIMESTAMP,
        planned_end_ts              TIMESTAMP,
        actual_end_ts               TIMESTAMP,
        late_start_seconds          INT,
        total_trip_delay_seconds    INT,
        dwell_total_seconds         INT,
        stop_count                  INT,
        data_quality_flag           STRING
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

(
    gold_timeline
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"service_date = '{target_date}'")
    .option("overwriteSchema", "false")
    .saveAsTable(GOLD_TRIP_TIMELINE_FACT)
)

print(f"✓ Written to {GOLD_TRIP_TIMELINE_FACT} for service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 6 — Quality Report

# COMMAND ----------

written = (
    spark.table(GOLD_TRIP_TIMELINE_FACT)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

total_trips    = written.count()
with_actuals   = written.filter(F.col("actual_start_ts").isNotNull()).count()
flagged        = written.filter(F.col("data_quality_flag").isNotNull()).count()

print(f"""
=== Gold Trip Timeline Fact: {target_date} ===
  Total trips             : {total_trips:>10,}
  Trips with actual data  : {with_actuals:>10,}
  Flagged (no data/other) : {flagged:>10,}
""")

print("Trip delay distribution (total_trip_delay_seconds, non-null):")
written.filter(F.col("total_trip_delay_seconds").isNotNull()).select(
    F.min("total_trip_delay_seconds").alias("min"),
    F.percentile_approx("total_trip_delay_seconds", 0.5).alias("p50"),
    F.percentile_approx("total_trip_delay_seconds", 0.9).alias("p90"),
    F.max("total_trip_delay_seconds").alias("max"),
).show()

print("Late start distribution (seconds, non-null):")
written.filter(F.col("late_start_seconds").isNotNull()).select(
    F.min("late_start_seconds").alias("min"),
    F.percentile_approx("late_start_seconds", 0.5).alias("p50"),
    F.percentile_approx("late_start_seconds", 0.9).alias("p90"),
    F.max("late_start_seconds").alias("max"),
).show()

print("Trips by route (top 10):")
(
    written
    .filter(F.col("data_quality_flag").isNull())
    .groupBy("route_id", "direction_id")
    .agg(
        F.count("*").alias("trip_count"),
        F.avg("total_trip_delay_seconds").alias("avg_delay_secs"),
    )
    .orderBy(F.desc("trip_count"))
    .limit(10)
    .show()
)
