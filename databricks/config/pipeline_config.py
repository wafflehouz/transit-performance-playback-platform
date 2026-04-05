# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Transit Pipeline — Shared Configuration
# MAGIC
# MAGIC Import this in every notebook:
# MAGIC ```python
# MAGIC %run ../config/pipeline_config
# MAGIC ```
# MAGIC
# MAGIC ## Secrets Setup (one-time, in your terminal)
# MAGIC ```bash
# MAGIC databricks secrets create-scope --scope transit
# MAGIC databricks secrets put-secret --scope transit --key mecatran_api_key
# MAGIC # Paste the key when prompted — never commit it to git
# MAGIC ```

# COMMAND ----------

# ── Catalog / Schema ──────────────────────────────────────────────────────────
# All Delta tables land in a single schema using bronze_/silver_/gold_ prefixes.
# Change SCHEMA if your bootcamp workspace gives you a dedicated schema.

CATALOG = "tabular"
SCHEMA  = "dataexpert"

# ── Volume Paths ──────────────────────────────────────────────────────────────
BASE_PATH = "/Volumes/tabular/dataexpert/josh_wafflehouz/transit_performance"

RAW_STATIC_PATH   = f"{BASE_PATH}/raw/static"
RAW_RT_PATH       = f"{BASE_PATH}/raw/rt_snapshots"
CHECKPOINT_PATH   = f"{BASE_PATH}/checkpoints"
CURATED_PATH      = f"{BASE_PATH}/curated"
LOGS_PATH         = f"{BASE_PATH}/logs"

# RT snapshot sub-paths (pollers write here; AutoLoader reads here)
TRIP_UPDATES_SNAPSHOTS_PATH       = f"{RAW_RT_PATH}/trip_updates"
VEHICLE_POSITIONS_SNAPSHOTS_PATH  = f"{RAW_RT_PATH}/vehicle_positions"

# AutoLoader checkpoint sub-paths
TRIP_UPDATES_CHECKPOINT      = f"{CHECKPOINT_PATH}/trip_updates"
VEHICLE_POSITIONS_CHECKPOINT = f"{CHECKPOINT_PATH}/vehicle_positions"

# ── API Endpoints ─────────────────────────────────────────────────────────────
_MECATRAN_BASE = "https://mna.mecatran.com/utw/ws/gtfsfeed"
TRIP_UPDATES_URL      = f"{_MECATRAN_BASE}/realtime/valleymetro"
VEHICLE_POSITIONS_URL = f"{_MECATRAN_BASE}/vehicles/valleymetro"
SERVICE_ALERTS_URL    = f"{_MECATRAN_BASE}/alerts/valleymetro"

GTFS_STATIC_URL = (
    "https://www.phoenixopendata.com/dataset/"
    "3eae9a4a-98b9-40c8-8df7-8c00c1756235/resource/"
    "28ccc0a5-49c8-495c-b91f-193de5ce2cb7/download/googletransit.zip"
)

# ── API Key ───────────────────────────────────────────────────────────────────
# Secret stored in Databricks Secrets:
#   scope: transit-api
#   key:   MECATRAN_API_KEY

SECRETS_SCOPE = "transit-api"
SECRETS_KEY   = "MECATRAN_API_KEY"

def get_api_key():
    """Retrieve mecatran API key from Databricks Secrets."""
    return dbutils.secrets.get(scope=SECRETS_SCOPE, key=SECRETS_KEY)

# ── Table Name Helpers ────────────────────────────────────────────────────────
def tbl(name: str) -> str:
    """Return fully-qualified Delta table name."""
    return f"{CATALOG}.{SCHEMA}.{name}"

# Bronze
BRONZE_TRIP_UPDATES       = tbl("bronze_trip_updates_events")
BRONZE_VEHICLE_POSITIONS  = tbl("bronze_vehicle_position_events")
BRONZE_ALERTS             = tbl("bronze_alerts_events")

# Silver — dimensions
SILVER_DIM_ROUTE           = tbl("silver_dim_route")
SILVER_DIM_STOP            = tbl("silver_dim_stop")
SILVER_DIM_TRIP            = tbl("silver_dim_trip")
SILVER_DIM_CALENDAR        = tbl("silver_dim_calendar")
SILVER_DIM_CALENDAR_DATES  = tbl("silver_dim_calendar_dates")

# Silver — facts
SILVER_FACT_STOP_SCHEDULE       = tbl("silver_fact_stop_schedule")
SILVER_FACT_SHAPE_POINTS        = tbl("silver_fact_shape_points")
SILVER_FACT_TRIP_UPDATES        = tbl("silver_fact_trip_updates")
SILVER_FACT_VEHICLE_POSITIONS   = tbl("silver_fact_vehicle_positions")

# Gold
GOLD_TRIP_TIMELINE_FACT       = tbl("gold_trip_timeline_fact")
GOLD_STOP_DWELL_FACT          = tbl("gold_stop_dwell_fact")
GOLD_TRIP_PATH_FACT           = tbl("gold_trip_path_fact")
GOLD_ROUTE_METRICS_15MIN      = tbl("gold_route_metrics_15min")
GOLD_ROUTE_METRICS_BASELINE   = tbl("gold_route_metrics_baseline")
GOLD_ROUTE_SEGMENT_CONGESTION = tbl("gold_route_segment_congestion")
GOLD_RAIL_STOP_ACTUALS        = tbl("gold_rail_stop_actuals")
GOLD_ANOMALY_EVENTS           = tbl("gold_anomaly_events")
GOLD_INCIDENT_REPORTS         = tbl("gold_incident_reports")
GOLD_STOP_DWELL_INFERRED      = tbl("gold_stop_dwell_inferred")
GOLD_GTFS_VERSION_LOG         = tbl("gold_gtfs_version_log")
GOLD_ROUTE_GROUPS             = tbl("gold_route_groups")
GOLD_FEED_HEALTH_LOG          = tbl("gold_feed_health_log")
GOLD_WEEKLY_REPORT_LOG        = tbl("gold_weekly_report_log")

# ── Pipeline Constants ────────────────────────────────────────────────────────
BRONZE_RETENTION_DAYS = 90     # 90 days: covers 28-day baseline window with margin for gold rebuilds
SILVER_RETENTION_DAYS = 365
GOLD_RETENTION_DAYS   = 365

POLL_INTERVAL_SECONDS       = 20    # target cadence for pollers; matches Mecatran's ~23s VP update frequency
VP_DOWNSAMPLE_SECONDS       = 30    # keep at most 1 VP row per vehicle per this window
VP_STOP_RADIUS_METERS       = 40    # haversine radius to match VP point to a stop
VP_STATIONARY_THRESHOLD_MPS = 1.0   # speed below this = vehicle considered stationary

# Rail route IDs processed by Gold 48 (VP stop-sequence transition actuals).
# A = Valley Metro Rail (East/West line), B = Valley Metro Rail (METRO line).
# S (Tempe Streetcar) is excluded — trip_id reused across daily runs breaks
# the first-VP-ping signal; S is grouped with bus routes in Gold 40 / Swiftly.
RAIL_ROUTE_IDS = ["A", "B"]

print(f"Config loaded — catalog={CATALOG}, schema={SCHEMA}")
print(f"Base path: {BASE_PATH}")
