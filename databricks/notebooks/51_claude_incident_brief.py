# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 51 — Claude: AI Incident Brief Generator
# MAGIC
# MAGIC **Schedule:** Nightly, after notebook 46 (anomaly detection) completes
# MAGIC **Dependencies:** `gold_anomaly_events`, `gold_route_metrics_15min`, `gold_route_metrics_baseline`
# MAGIC **Output:** `gold_incident_reports` — one row per service_date
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Reads anomaly events for the target date
# MAGIC 2. Gathers supporting context from route metrics and baseline tables
# MAGIC 3. Builds a structured prompt summarising the day's performance anomalies
# MAGIC 4. Calls the Databricks Foundation Model API (Llama 3.3 70B Instruct) to generate
# MAGIC    an actionable incident brief — billed to the workspace, no personal key needed
# MAGIC 5. Writes the brief and token usage metadata to `gold_incident_reports`
# MAGIC
# MAGIC **No secrets required** — uses the notebook's Databricks PAT automatically.
# MAGIC
# MAGIC **Installing the OpenAI SDK (one-time, in a cluster init script or notebook):**
# MAGIC ```
# MAGIC %pip install openai
# MAGIC ```

# COMMAND ----------

%pip install openai --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import date, timedelta
import json

# COMMAND ----------

# MAGIC %md ## Step 1 — Widget Setup

# COMMAND ----------

dbutils.widgets.text("target_date", (date.today() - timedelta(days=1)).isoformat())
target_date = dbutils.widgets.get("target_date")
print(f"Generating incident brief for service_date = {target_date}")

# Day-of-week label for the prompt (service_date is Phoenix local, which is correct
# for human-readable reporting regardless of UTC offset).
target_date_obj = date.fromisoformat(target_date)
day_of_week = target_date_obj.strftime("%A")  # e.g. "Monday"
print(f"Day of week: {day_of_week}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Read Anomaly Events
# MAGIC
# MAGIC If there are no anomalies for the target date we exit cleanly — no brief needed.

# COMMAND ----------

anomalies_df = (
    spark.table(GOLD_ANOMALY_EVENTS)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

anomaly_count  = anomalies_df.count()
print(f"Total anomaly buckets for {target_date}: {anomaly_count:,}")

if anomaly_count == 0:
    msg = f"No anomalies detected for {target_date} — no brief generated"
    print(msg)
    dbutils.notebook.exit(msg)

# Severity breakdown
severity_counts = (
    anomalies_df
    .groupBy("severity")
    .count()
    .collect()
)
severity_map = {row["severity"]: row["count"] for row in severity_counts}
warning_count  = severity_map.get("warning",  0)
critical_count = severity_map.get("critical", 0)

print(f"  Warning:  {warning_count:,}")
print(f"  Critical: {critical_count:,}")

# COMMAND ----------

# MAGIC %md ## Step 3 — Build Context Summary
# MAGIC
# MAGIC We pull three pieces of supporting context:
# MAGIC - **Top routes by severity** — which routes had the most critical/warning buckets
# MAGIC - **Time-of-day pattern** — which UTC hour had the most anomaly buckets
# MAGIC - **Baseline comparison** — worst z-scores and OTP drops to show how unusual the day was

# COMMAND ----------

# ── Top 5 routes by severity (critical first, then by bucket count) ────────────
top_routes_df = (
    anomalies_df
    .groupBy("route_id", "severity")
    .agg(
        F.count("*").alias("flagged_buckets"),
        F.round(F.avg("avg_delay_seconds"),       0).alias("avg_delay_s"),
        F.round(F.max("avg_delay_seconds"),       0).alias("max_delay_s"),
        F.round(F.avg("delay_vs_baseline_seconds"), 0).alias("avg_vs_baseline_s"),
        F.round(F.avg("delay_z_score"),           2).alias("avg_z_score"),
        F.round(F.avg("otp_drop_pp"),             1).alias("avg_otp_drop_pp"),
    )
    .orderBy(
        F.when(F.col("severity") == "critical", 0).otherwise(1),
        F.desc("flagged_buckets"),
    )
    .limit(5)
)

top_routes = top_routes_df.collect()

print("Top affected routes:")
top_routes_df.show(truncate=False)

# COMMAND ----------

# ── Time-of-day pattern — which UTC hour had the most flagged buckets ──────────
# Note: UTC hour - 7 ≈ Phoenix standard time (MST, no DST)
hourly_pattern_df = (
    anomalies_df
    .withColumn("utc_hour", F.hour("time_bucket_15min"))
    .groupBy("utc_hour")
    .agg(F.count("*").alias("flagged_buckets"))
    .orderBy(F.desc("flagged_buckets"))
    .limit(5)
)

hourly_pattern = hourly_pattern_df.collect()

print("Anomaly concentration by UTC hour (subtract 7 for Phoenix MST):")
hourly_pattern_df.show()

# ── Worst single buckets by delay z-score ─────────────────────────────────────
worst_buckets_df = (
    anomalies_df
    .select(
        "route_id",
        "direction_id",
        F.date_format("time_bucket_15min", "HH:mm").alias("bucket_utc"),
        "severity",
        F.round("avg_delay_seconds",         0).alias("avg_delay_s"),
        F.round("delay_vs_baseline_seconds", 0).alias("vs_baseline_s"),
        F.round("delay_z_score",             2).alias("z_score"),
        F.round("otp_drop_pp",               1).alias("otp_drop_pp"),
    )
    .orderBy(F.col("z_score").desc_nulls_last())
    .limit(5)
)

worst_buckets = worst_buckets_df.collect()

print("Worst buckets by z-score:")
worst_buckets_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md ## Step 4 — Read Supporting Metrics
# MAGIC
# MAGIC Pull the day's route metrics to give Claude time-of-day delay context beyond
# MAGIC just the anomaly-flagged buckets.

# COMMAND ----------

# Overall route-level summary for the target date
route_summary_df = (
    spark.table(GOLD_ROUTE_METRICS_15MIN)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
    .groupBy("route_id")
    .agg(
        F.round(F.avg("avg_delay_seconds"), 0).alias("day_avg_delay_s"),
        F.round(F.avg("pct_on_time") * 100, 1).alias("day_avg_otp_pct"),
        F.count("*").alias("total_buckets"),
    )
    .orderBy(F.desc("day_avg_delay_s"))
    .limit(10)
)

route_summary = route_summary_df.collect()
print("Route-level day summary (top 10 by avg delay):")
route_summary_df.show()

# COMMAND ----------

# MAGIC %md ## Step 5 — Build the Claude Prompt

# COMMAND ----------

# ── Format top routes as a readable block ─────────────────────────────────────
def format_route_list(rows):
    lines = []
    for r in rows:
        otp_str = (
            f", OTP drop {r['avg_otp_drop_pp']:.1f} pp"
            if r["avg_otp_drop_pp"] is not None
            else ""
        )
        z_str = (
            f", z={r['avg_z_score']:.2f}"
            if r["avg_z_score"] is not None
            else ""
        )
        lines.append(
            f"  - Route {r['route_id']} ({r['severity'].upper()}): "
            f"{r['flagged_buckets']} flagged buckets, "
            f"avg delay {int(r['avg_delay_s'])}s (max {int(r['max_delay_s'])}s), "
            f"{int(r['avg_vs_baseline_s'])}s above baseline"
            f"{z_str}{otp_str}"
        )
    return "\n".join(lines) if lines else "  (none)"

def format_hourly(rows):
    lines = []
    for r in rows:
        phoenix_hour = (r["utc_hour"] - 7) % 24
        lines.append(
            f"  - UTC {r['utc_hour']:02d}:xx  (Phoenix ~{phoenix_hour:02d}:xx): "
            f"{r['flagged_buckets']} flagged buckets"
        )
    return "\n".join(lines) if lines else "  (no pattern data)"

def format_worst_buckets(rows):
    lines = []
    for r in rows:
        z_str = f", z={r['z_score']:.2f}" if r["z_score"] is not None else ""
        otp_str = (
            f", OTP drop {r['otp_drop_pp']:.1f} pp"
            if r["otp_drop_pp"] is not None
            else ""
        )
        lines.append(
            f"  - Route {r['route_id']} dir {r['direction_id']} "
            f"at {r['bucket_utc']} UTC ({r['severity']}): "
            f"avg delay {int(r['avg_delay_s'])}s, "
            f"{int(r['vs_baseline_s'])}s above baseline"
            f"{z_str}{otp_str}"
        )
    return "\n".join(lines) if lines else "  (none)"

# ── Assemble the user prompt ───────────────────────────────────────────────────
user_prompt = f"""
Transit Performance Incident Brief Request
==========================================

Date:         {target_date} ({day_of_week})
Agency:       Valley Metro, Phoenix AZ
Data source:  Automated real-time feed analysis (GTFS-RT)

ANOMALY SUMMARY
---------------
Total flagged route-time-buckets: {anomaly_count}
  Warning level:  {warning_count}
  Critical level: {critical_count}

TOP AFFECTED ROUTES (by flagged bucket count, critical first)
--------------------------------------------------------------
{format_route_list(top_routes)}

TIME-OF-DAY PATTERN (peak anomaly hours)
-----------------------------------------
Valley Metro operates on MST (UTC-7, no DST).
{format_hourly(hourly_pattern)}

WORST INDIVIDUAL BUCKETS (highest z-scores vs baseline)
-------------------------------------------------------
{format_worst_buckets(worst_buckets)}

NOTES ON THRESHOLDS
-------------------
- Warning fires when avg_delay ≥ 180 s AND > baseline + 1.5×stddev, OR OTP drops ≥ 15 pp
- Critical fires when avg_delay ≥ 360 s AND > baseline + 2.0×stddev, OR OTP drops ≥ 25 pp
- Baseline is a rolling 28-day window for the same day-type (weekday / Saturday / Sunday)

TASK
----
Please produce an incident brief with the following sections:

1. **Executive Summary** (2–3 sentences): What happened on {target_date}? How significant
   was this relative to baseline? Was it widespread or isolated?

2. **Affected Routes**: A concise list of the most impacted routes with their severity,
   delay magnitude, and OTP impact.

3. **Likely Contributing Factors**: Based on the time-of-day pattern, the routes affected,
   and the delay magnitudes, what are the most probable operational causes?
   (e.g. peak-hour congestion, specific corridor issues, schedule adherence problems)

4. **Recommended Follow-up Actions**: Specific, actionable steps for transit planners
   (e.g. schedule review for specific routes, operator feedback, passenger communication).

Keep the brief concise and actionable. Use plain language suitable for transit operations
managers, not data engineers. Avoid restating all the numbers — synthesise them into
insights.
""".strip()

system_prompt = (
    "You are a transit operations analyst assistant for Valley Metro in Phoenix, AZ. "
    "Generate concise, actionable incident briefs for transit planners based on performance data. "
    "Focus on operational insights and practical recommendations, not data methodology."
)

print(f"Prompt length: {len(user_prompt):,} characters")

# COMMAND ----------

# MAGIC %md ## Step 6 — Call Databricks Foundation Model API
# MAGIC
# MAGIC Uses the Databricks-hosted Llama 3.3 70B Instruct endpoint via the OpenAI-compatible
# MAGIC serving API. Billed to the workspace — no personal API key required.
# MAGIC The Databricks PAT is retrieved automatically from the notebook context.

# COMMAND ----------

try:
    from openai import OpenAI
except ImportError:
    raise ImportError(
        "The 'openai' package is not installed in this cluster.\n"
        "Install it by running in a notebook cell:\n"
        "  %pip install openai\n"
        "Then detach and re-attach this notebook (or restart the cluster)."
    )

# Retrieve PAT and workspace URL from notebook context — no secrets setup needed
token         = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")

MODEL_VERSION = "databricks-meta-llama-3-3-70b-instruct"
MAX_TOKENS    = 1024

print(f"Calling {MODEL_VERSION} via Databricks FMAPI (max_tokens={MAX_TOKENS}) ...")

client = OpenAI(
    api_key=token,
    base_url=f"https://{workspace_url}/serving-endpoints",
)

response = client.chat.completions.create(
    model=MODEL_VERSION,
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_prompt},
    ],
    max_tokens=MAX_TOKENS,
)

brief_text        = response.choices[0].message.content
prompt_tokens     = response.usage.prompt_tokens
completion_tokens = response.usage.completion_tokens

print(f"Response received — {prompt_tokens} prompt tokens, {completion_tokens} completion tokens")

# COMMAND ----------

# MAGIC %md ## Step 7 — Print the Brief

# COMMAND ----------

print("\n" + "=" * 70)
print(f"  INCIDENT BRIEF — {target_date} ({day_of_week})")
print("=" * 70)
print(brief_text)
print("=" * 70 + "\n")

# COMMAND ----------

# MAGIC %md ## Step 8 — Write to `gold_incident_reports`

# COMMAND ----------

# Build a JSON array of top affected routes for the structured column.
top_routes_json = json.dumps([
    {
        "route_id":        r["route_id"],
        "severity":        r["severity"],
        "flagged_buckets": r["flagged_buckets"],
        "avg_delay_s":     int(r["avg_delay_s"]) if r["avg_delay_s"] is not None else None,
        "avg_vs_baseline_s": int(r["avg_vs_baseline_s"]) if r["avg_vs_baseline_s"] is not None else None,
        "avg_otp_drop_pp": float(r["avg_otp_drop_pp"]) if r["avg_otp_drop_pp"] is not None else None,
    }
    for r in top_routes
])

# COMMAND ----------

# Create the table if it does not already exist.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_INCIDENT_REPORTS} (
        service_date          DATE,
        generated_ts          TIMESTAMP,
        model_version         STRING,
        anomaly_count         INT,
        warning_count         INT,
        critical_count        INT,
        top_routes_affected   STRING,
        brief_text            STRING,
        prompt_tokens         INT,
        completion_tokens     INT
    )
    USING DELTA
    PARTITIONED BY (service_date)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

print(f"Table {GOLD_INCIDENT_REPORTS} ready")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DateType, TimestampType, StringType, IntegerType
from datetime import datetime, timezone

report_schema = StructType([
    StructField("service_date",        DateType(),      False),
    StructField("generated_ts",        TimestampType(), False),
    StructField("model_version",       StringType(),    False),
    StructField("anomaly_count",       IntegerType(),   False),
    StructField("warning_count",       IntegerType(),   False),
    StructField("critical_count",      IntegerType(),   False),
    StructField("top_routes_affected", StringType(),    True),
    StructField("brief_text",          StringType(),    False),
    StructField("prompt_tokens",       IntegerType(),   False),
    StructField("completion_tokens",   IntegerType(),   False),
])

report_row = [(
    target_date_obj,                         # service_date  (Python date → DateType)
    datetime.now(tz=timezone.utc).replace(tzinfo=None),  # generated_ts (naive UTC)
    MODEL_VERSION,
    int(anomaly_count),
    int(warning_count),
    int(critical_count),
    top_routes_json,
    brief_text,
    int(prompt_tokens),
    int(completion_tokens),
)]

report_df = spark.createDataFrame(report_row, schema=report_schema)

(
    report_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"service_date = '{target_date}'")
    .option("overwriteSchema", "false")
    .saveAsTable(GOLD_INCIDENT_REPORTS)
)

print(f"Written to {GOLD_INCIDENT_REPORTS} for service_date = {target_date}")

# COMMAND ----------

# MAGIC %md ## Step 9 — Quality Check

# COMMAND ----------

written = (
    spark.table(GOLD_INCIDENT_REPORTS)
    .filter(F.col("service_date") == F.lit(target_date).cast("date"))
)

written_count = written.count()
print(f"\n=== Incident Report Write Summary ===")
print(f"Rows written:              {written_count}")
print(f"service_date:              {target_date}")
print(f"Model:                     {MODEL_VERSION}")
print(f"Anomalies summarised:      {anomaly_count} ({warning_count} warning, {critical_count} critical)")
print(f"Token usage:               {prompt_tokens} prompt + {completion_tokens} completion = {prompt_tokens + completion_tokens} total")
print(f"Brief length (chars):      {len(brief_text):,}")

if written_count != 1:
    raise RuntimeError(
        f"Expected 1 row in {GOLD_INCIDENT_REPORTS} for {target_date}, found {written_count}"
    )

print(f"\nDone — incident brief for {target_date} ({day_of_week}) written successfully.")
