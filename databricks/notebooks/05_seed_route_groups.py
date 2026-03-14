# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 — Seed: Route Groups (Swiftly Import)
# MAGIC
# MAGIC **Run:** Once (or re-run to refresh when Swiftly route groups change)
# MAGIC **Output:** `gold_route_groups` — one row per (group_name, route_id)
# MAGIC
# MAGIC Source: Swiftly Route Groups table exported 2026-03-14.
# MAGIC Groups mirror Valley Metro's operational groupings used in Swiftly for
# MAGIC filtering dashboards, reports, and anomaly views.
# MAGIC
# MAGIC **Schema:**
# MAGIC | Column       | Type   | Notes                          |
# MAGIC |--------------|--------|--------------------------------|
# MAGIC | group_name   | STRING | Swiftly group display name     |
# MAGIC | route_id     | STRING | Matches silver_dim_route.route_id |
# MAGIC | source       | STRING | 'swiftly_import'               |
# MAGIC | last_edited  | DATE   | Date from Swiftly UI           |
# MAGIC | edited_by    | STRING | Email from Swiftly UI          |

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import date

# COMMAND ----------

# MAGIC %md ## Route Groups Data (parsed from Swiftly HTML, 2026-03-14)

# COMMAND ----------

# Each entry: (group_name, [route_ids], last_edited, edited_by)
# Dates and editors parsed directly from the Swiftly Route Groups UI.

_GROUPS = [
    (
        "Valley Metro Routes",
        ["30","40","45","48","56","61","62","66","72","77","81","96","104","108",
         "112","120","128","136","140","156","184","514","521","522","531","533",
         "535","542","562","563","571","573","575","GAL","DBUZ","EART","FBUZ",
         "FLSH","JUPI","MARS","MERC","STRN","VENU"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "Valley Metro Rail",
        ["A","B","S","RAIL"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "EV- Local & Circulator - Valley Metro",
        ["30","40","45","48","56","61","62","66","72","77","81","96","104","108",
         "112","120","128","136","140","156","184","DBUZ","EART","FBUZ","FLSH",
         "JUPI","MARS","MERC","STRN","VENU"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "EV All Mesa Routes",
        ["30","40","45","61","96","104","108","112","120","128","136","140","156",
         "184","533","535","DBUZ","FBUZ"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "EV MESA KEOLIS - Express Routes ONLY",
        ["533","535"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "City of Phoenix Routes",
        ["0","1","3","7","8","0A","10","12","13","15","16","17","19","27","28",
         "29","32","35","39","41","43","44","50","51","52","59","60","67","70",
         "75","7s","80","83","90","106","122","138","154","16s","170","17s","186",
         "32s","52s","59s","I17","SME","SMW","ALEX","DASH","I10E","I10W","MARY",
         "SMRT","SR51"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "Valley Metro Express",
        ["514","521","522","531","533","535","542","562","563","571","573","575"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "East Valley All Routes",
        ["30","40","45","48","56","61","62","66","72","77","81","96","104","108",
         "112","120","128","136","140","156","184","514","521","522","531","533",
         "535","542","DBUZ","EART","FBUZ","FLSH","JUPI","MARS","MERC","STRN","VENU"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "National Express - Express Routes Only",
        ["562","563","571","573","575"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "EV ALL Local Routes - Valley Metro",
        ["30","40","45","48","56","61","62","66","72","77","81","96","104","108",
         "112","120","128","136","140","156","184"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "National Express/Total Ride",
        ["562","563","571","573","575","GAL"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "EV - School Routes",
        ["61s","62s","81s","MARz"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "West Transit Facility Routes",
        ["3","13","17","29","41","43","51","59","67","75","83","17s","59s","MARY"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "North Transit Facility",
        ["0","7","8","12","15","16","19","27","32","35","39","44","50","52","60",
         "70","80","90","106","122","138","154","170","186","I17","I10W","SMRT","SR51"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "South Transit Facility",
        ["0","1","7","8","0A","10","12","15","16","19","27","28","32","35","44",
         "50","52","60","70","80","106","16s","32s","52s","I17","SME","SMW","ALEX",
         "DASH","I10E","I10W","SR51"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "Phoenix Local Routes",
        ["1","3","7","8","10","12","13","15","16","17","19","27","28","29","32",
         "35","39","41","43","44","50","51","52","59","60","67","70","75","80",
         "83","90","106","122","138","154","170","186"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "Scottsdale Routes",
        ["17","29","41","50","72","80","81","154","170","514","68CM","MLHD","MSTG"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "East Valley Express",
        ["514","521","522","531","533","535","542"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "East Valley Circulators",
        ["DBUZ","EART","FBUZ","FLSH","JUPI","MARS","MERC","STRN","VENU"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "East Valley Local",
        ["30","40","45","48","56","61","62","66","72","77","81","96","104","108",
         "112","120","128","136","140","156","184"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "EV MESA KEOLIS - Local Routes ONLY",
        ["30","40","45","61","96","104","108","112","120","128","136","140","156","184"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "EV TEMPE KEOLIS - Express Routes ONLY",
        ["514","521","522","531","542"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "EV MESA KEOLIS - Circulator Routes ONLY",
        ["DBUZ","FBUZ"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "EV TEMPE KEOLIS - FLSH & Orbit Circulator Routes ONLY",
        ["EART","FLSH","JUPI","MARS","MERC","STRN","VENU"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "EV TEMPE Keolis - Local Routes ONLY",
        ["30","45","48","56","61","62","66","72","77","81","108","140","156"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "EV ALL - Keolis - School Routes",
        ["61s","62s","81s","MARz"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "PHX- All Operated Routes",
        ["0","1","3","7","8","0A","10","12","13","15","16","17","19","27","28",
         "29","32","35","39","41","43","44","50","51","52","59","60","67","70",
         "75","7s","80","83","90","106","122","138","154","16s","170","17s","186",
         "32s","52s","59s","I17","SME","SMW","ALEX","DASH","I10E","I10W","MARY",
         "SMRT","SR51"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "PHX - School Trippers",
        ["7s","16s","17s","32s","52s","59s"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "PHX - RAPIDS",
        ["I17","SME","SMW","I10E","I10W","SR51"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "PHX - High Frequency",
        ["3","7","0A","16","17","19","29","35","41","50","70","7s","16s","DASH"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "PHX - Circulators",
        ["ALEX","DASH","MARY","SMRT"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "PHNS - All Routes",
        ["0","1","7","8","0A","10","12","15","16","19","27","28","32","35","39",
         "44","50","52","60","70","7s","80","90","106","122","138","154","16s",
         "170","186","32s","52s","I17","SME","SMW","ALEX","DASH","I10E","I10W",
         "SMRT","SR51"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "PHW - All Routes",
        ["3","13","17","29","41","43","51","59","67","75","83","17s","59s","MARY"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "First Focus Group",
        ["45","61"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
    (
        "EV All Tempe Routes",
        ["30","45","48","56","61","62","66","72","77","81","108","140","156","514",
         "521","522","531","542","EART","FLSH","JUPI","MARS","MARz","MERC","STRN","VENU"],
        date(2025, 3, 14), "ops@valleymetro.org",
    ),
]

# COMMAND ----------

# MAGIC %md ## Explode to one row per (group_name, route_id)

# COMMAND ----------

_schema = StructType([
    StructField("group_name",  StringType(), nullable=False),
    StructField("route_id",    StringType(), nullable=False),
    StructField("source",      StringType(), nullable=False),
    StructField("last_edited", DateType(),   nullable=True),
    StructField("edited_by",   StringType(), nullable=True),
])

_rows = [
    Row(
        group_name  = group_name,
        route_id    = route_id,
        source      = "swiftly_import",
        last_edited = last_edited,
        edited_by   = edited_by,
    )
    for group_name, route_ids, last_edited, edited_by in _GROUPS
    for route_id in route_ids
]

df = spark.createDataFrame(_rows, schema=_schema)

print(f"Total rows: {df.count():,}   Groups: {len(_GROUPS)}   Distinct routes: {df.select('route_id').distinct().count()}")

# COMMAND ----------

# MAGIC %md ## Write — full overwrite (seed is authoritative)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_ROUTE_GROUPS} (
        group_name   STRING  NOT NULL,
        route_id     STRING  NOT NULL,
        source       STRING  NOT NULL,
        last_edited  DATE,
        edited_by    STRING
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

(
    df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")
    .saveAsTable(GOLD_ROUTE_GROUPS)
)

print(f"✓ Written to {GOLD_ROUTE_GROUPS}")

# COMMAND ----------

# MAGIC %md ## Quality check

# COMMAND ----------

written = spark.table(GOLD_ROUTE_GROUPS)

print("Row count by group:")
(
    written
    .groupBy("group_name")
    .count()
    .orderBy("group_name")
    .show(50, truncate=False)
)

print("\nSample — routes in 'PHX - High Frequency':")
(
    written
    .filter(written.group_name == "PHX - High Frequency")
    .select("route_id")
    .orderBy("route_id")
    .show()
)
