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

final_tu = (
    spark.table(SILVER_FACT_TRIP_UPDATES)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
    .filter(F.col("is_final") == True)
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

# MAGIC %md ## Step 2 — Join to Static Schedule

# COMMAND ----------

schedule = (
    spark.table(SILVER_FACT_STOP_SCHEDULE)
    .select(
        "trip_id",
        "stop_sequence",
        "scheduled_arrival_secs",
        "scheduled_departure_secs",
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
        dwell_delta_seconds      INT
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
    .option("overwriteSchema", "false")
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
