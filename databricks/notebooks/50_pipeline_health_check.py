# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 50 — Pipeline Health Check
# MAGIC
# MAGIC **Schedule:** Run after nightly Gold jobs complete (e.g. 6:00 AM), or on-demand
# MAGIC **Purpose:** Verifies that every Silver and Gold table was updated with yesterday's
# MAGIC data and that the RT pollers are actively writing snapshot files.
# MAGIC
# MAGIC **Checks performed:**
# MAGIC 1. `MAX(service_date)` vs. yesterday for every Silver and Gold fact table
# MAGIC 2. Row count for yesterday's partition in each table
# MAGIC 3. NDJSON file count written in the last 2 hours to both poller volumes
# MAGIC
# MAGIC **Exit behaviour:**
# MAGIC - All tables current → exits with `"HEALTHY"` message
# MAGIC - Any table stale, empty, or poller silent → raises an exception so the Databricks
# MAGIC   workflow marks the run as Failed and sends a failure notification

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import date, timedelta, datetime, timezone
import pytz

# COMMAND ----------

# Widget: how many days behind before a table is considered stale.
# Default 1 means any table that hasn't loaded yesterday's data is flagged.
dbutils.widgets.text("alert_threshold_days", "1")
alert_threshold_days = int(dbutils.widgets.get("alert_threshold_days"))

# Determine "yesterday" in Phoenix local time — service_date is always Phoenix local.
phoenix_tz = pytz.timezone("America/Phoenix")
now_phoenix = datetime.now(tz=phoenix_tz)
yesterday_phoenix = (now_phoenix - timedelta(days=1)).date()
yesterday_str = yesterday_phoenix.isoformat()

print(f"Health check run at (UTC):    {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Phoenix local now:            {now_phoenix.strftime('%Y-%m-%d %H:%M:%S %Z')}")
print(f"Checking for service_date:    {yesterday_str}")
print(f"Alert threshold (days late):  {alert_threshold_days}")

# COMMAND ----------

# MAGIC %md ## Step 1 — Table Freshness and Row Counts

# COMMAND ----------

# Tables to check: (display_name, table_constant)
TABLES_TO_CHECK = [
    ("silver_fact_trip_updates",       SILVER_FACT_TRIP_UPDATES),
    ("silver_fact_vehicle_positions",  SILVER_FACT_VEHICLE_POSITIONS),
    ("gold_stop_dwell_fact",           GOLD_STOP_DWELL_FACT),
    ("gold_stop_dwell_inferred",       GOLD_STOP_DWELL_INFERRED),
    ("gold_trip_timeline_fact",        GOLD_TRIP_TIMELINE_FACT),
    ("gold_route_metrics_15min",       GOLD_ROUTE_METRICS_15MIN),
    ("gold_route_metrics_baseline",    GOLD_ROUTE_METRICS_BASELINE),
    ("gold_anomaly_events",            GOLD_ANOMALY_EVENTS),
]

results = []

for display_name, table_fqn in TABLES_TO_CHECK:
    try:
        df = spark.table(table_fqn)

        # MAX(service_date) across the whole table
        latest_row = df.agg(F.max("service_date").alias("max_date")).collect()[0]
        latest_date = latest_row["max_date"]  # Python date or None

        # Row count for yesterday specifically
        yesterday_count = (
            df
            .filter(F.col("service_date") == F.lit(yesterday_str).cast("date"))
            .count()
        )

        if latest_date is None:
            status = "EMPTY"
        else:
            days_behind = (yesterday_phoenix - latest_date).days
            if days_behind > alert_threshold_days:
                status = "STALE"
            elif yesterday_count == 0:
                status = "EMPTY"
            else:
                status = "OK"

        results.append({
            "table":          display_name,
            "latest_date":    str(latest_date) if latest_date else "—",
            "yesterday_rows": yesterday_count,
            "status":         status,
        })

    except Exception as e:
        results.append({
            "table":          display_name,
            "latest_date":    "ERROR",
            "yesterday_rows": -1,
            "status":         f"ERROR: {e}",
        })

# COMMAND ----------

# MAGIC %md ## Step 2 — Poller File Activity (Last 2 Hours)

# COMMAND ----------

# Files written in the last 2 hours indicate the pollers are active.
# dbutils.fs.ls returns FileInfo objects with modificationTime (epoch ms).
POLLER_PATHS = {
    "trip_updates":       TRIP_UPDATES_SNAPSHOTS_PATH,
    "vehicle_positions":  VEHICLE_POSITIONS_SNAPSHOTS_PATH,
}

cutoff_epoch_ms = (datetime.now(tz=timezone.utc).timestamp() - 2 * 3600) * 1000

poller_results = []

for feed_name, vol_path in POLLER_PATHS.items():
    try:
        files = dbutils.fs.ls(vol_path)
        recent = [f for f in files if f.modificationTime >= cutoff_epoch_ms]
        count_recent = len(recent)
        count_total  = len(files)
        p_status = "OK" if count_recent > 0 else "SILENT"
    except Exception as e:
        count_recent = -1
        count_total  = -1
        p_status = f"ERROR: {e}"

    poller_results.append({
        "feed":           feed_name,
        "files_2h":       count_recent,
        "total_files":    count_total,
        "status":         p_status,
    })

# COMMAND ----------

# MAGIC %md ## Step 3 — Health Report

# COMMAND ----------

# ── Table Health ──────────────────────────────────────────────────────────────
header = f"{'TABLE':<35} {'LATEST DATE':<14} {'YESTERDAY ROWS':>15} {'STATUS'}"
divider = "-" * len(header)

print("\n" + "=" * 70)
print(f"  PIPELINE HEALTH CHECK — service_date target: {yesterday_str}")
print("=" * 70)
print("\n[ TABLE FRESHNESS ]\n")
print(header)
print(divider)

table_problems = []
for r in results:
    row = (
        f"{r['table']:<35} "
        f"{r['latest_date']:<14} "
        f"{r['yesterday_rows']:>15,} "
        f"{r['status']}"
    )
    print(row)
    if r["status"] != "OK":
        table_problems.append(r)

# ── Poller Activity ───────────────────────────────────────────────────────────
print("\n[ POLLER ACTIVITY (last 2 hours) ]\n")
p_header = f"{'FEED':<25} {'FILES (2 H)':>12} {'TOTAL FILES':>12} {'STATUS'}"
print(p_header)
print("-" * len(p_header))

poller_problems = []
for p in poller_results:
    row = (
        f"{p['feed']:<25} "
        f"{p['files_2h']:>12} "
        f"{p['total_files']:>12} "
        f"{p['status']}"
    )
    print(row)
    if p["status"] != "OK":
        poller_problems.append(p)

print()

# COMMAND ----------

# MAGIC %md ## Step 4 — Pass / Fail Decision

# COMMAND ----------

all_problems = []

if table_problems:
    all_problems.append(
        "STALE/EMPTY tables: " + ", ".join(r["table"] for r in table_problems)
    )

if poller_problems:
    all_problems.append(
        "Silent pollers: " + ", ".join(p["feed"] for p in poller_problems)
    )

if all_problems:
    summary_msg = "PIPELINE UNHEALTHY — " + "; ".join(all_problems)
    print(f"[FAIL] {summary_msg}")
    # Raising an exception causes Databricks to mark the run as Failed,
    # which triggers the workflow failure notification.
    raise RuntimeError(summary_msg)
else:
    summary_msg = f"HEALTHY — all tables current for {yesterday_str}, pollers active"
    print(f"[OK] {summary_msg}")
    dbutils.notebook.exit(summary_msg)
