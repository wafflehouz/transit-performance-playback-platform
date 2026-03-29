# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 53 — Weekly Route Performance Report (Sunday Night)
# MAGIC
# MAGIC **Schedule:** Weekly — Sunday 11 PM Phoenix time (Monday 06:00 UTC)
# MAGIC **Job:** `transit-weekly-reports`
# MAGIC **Dependencies:** gold_stop_dwell_fact, gold_stop_dwell_inferred, gold_route_metrics_15min, silver_dim_route, silver_dim_stop
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Fetches active subscriptions from Supabase REST API
# MAGIC 2. For each subscribed route, queries the last 7 days of gold data
# MAGIC 3. Generates an AI narrative via Databricks FMAPI (Llama 3.3 70B)
# MAGIC 4. Sends an HTML email report via Resend API
# MAGIC 5. Logs each send to gold_weekly_report_log
# MAGIC
# MAGIC **Secrets required (scope: transit-api):**
# MAGIC   - SUPABASE_URL            : your project URL (https://xxx.supabase.co)
# MAGIC   - SUPABASE_SERVICE_KEY    : service role key (bypasses RLS for server reads)
# MAGIC   - RESEND_API_KEY          : from resend.com

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

# MAGIC %pip install openai requests --quiet

# COMMAND ----------

import requests
import json
from datetime import date, timedelta, datetime, timezone
from openai import OpenAI
from pyspark.sql import functions as F

# ── Date range: last 7 complete days ─────────────────────────────────────────
end_date   = date.today() - timedelta(days=1)          # yesterday (last complete day)
start_date = end_date - timedelta(days=6)              # 7 days total
week_label = f"{start_date.isoformat()} → {end_date.isoformat()}"

print(f"Report week: {week_label}")

# ── Secrets ───────────────────────────────────────────────────────────────────
SUPABASE_URL     = dbutils.secrets.get("transit-api", "SUPABASE_URL")
SUPABASE_SVC_KEY = dbutils.secrets.get("transit-api", "SUPABASE_SERVICE_KEY")
RESEND_API_KEY   = dbutils.secrets.get("transit-api", "RESEND_API_KEY")

# Databricks FMAPI endpoint (same pattern as notebook 51)
FMAPI_TOKEN    = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
FMAPI_BASE_URL = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    .apiUrl().get().replace("https://", "https://") + "/serving-endpoints"
)

# COMMAND ----------

# MAGIC %md ## Step 1 — Fetch Active Subscriptions from Supabase

# COMMAND ----------

resp = requests.get(
    f"{SUPABASE_URL}/rest/v1/route_subscriptions",
    headers={
        "apikey":        SUPABASE_SVC_KEY,
        "Authorization": f"Bearer {SUPABASE_SVC_KEY}",
    },
    params={"active": "eq.true", "select": "user_id,route_id,route_name"},
)
resp.raise_for_status()
subscriptions = resp.json()
print(f"Active subscriptions: {len(subscriptions)}")

if not subscriptions:
    print("No active subscriptions — exiting.")
    dbutils.notebook.exit("No active subscriptions")

# COMMAND ----------

# MAGIC %md ## Step 2 — Fetch User Emails from Supabase Auth

# COMMAND ----------

user_ids = list({s["user_id"] for s in subscriptions})

# Supabase admin API: GET /auth/v1/admin/users returns all users
# We page through and build a lookup dict
email_map = {}
page = 1
while True:
    r = requests.get(
        f"{SUPABASE_URL}/auth/v1/admin/users",
        headers={
            "apikey":        SUPABASE_SVC_KEY,
            "Authorization": f"Bearer {SUPABASE_SVC_KEY}",
        },
        params={"page": page, "per_page": 50},
    )
    r.raise_for_status()
    data = r.json()
    users_page = data.get("users", [])
    if not users_page:
        break
    for u in users_page:
        if u["id"] in user_ids:
            email_map[u["id"]] = u["email"]
    if len(users_page) < 50:
        break
    page += 1

print(f"Resolved emails for {len(email_map)} user(s)")

# COMMAND ----------

# MAGIC %md ## Step 3 — Pull Gold Data per Route

# COMMAND ----------

def get_route_data(route_id: str) -> dict:
    """Query all gold tables for a single route over the report week."""
    sid = route_id.replace("'", "''")
    sd, ed = start_date.isoformat(), end_date.isoformat()

    EARLY   = "arrival_delay_seconds < -60 AND COALESCE(pickup_type, 0) = 0"
    ON_TIME = "(arrival_delay_seconds BETWEEN -60 AND 360 OR (arrival_delay_seconds < -60 AND COALESCE(pickup_type, 0) = 1))"
    LATE    = "arrival_delay_seconds > 360"

    # ── System summary ───────────────────────────────────────────────────────
    summary = spark.sql(f"""
        SELECT
          r.route_short_name,
          r.route_long_name,
          COUNT(*)                                                              AS total_stops,
          COUNT(DISTINCT f.trip_id)                                             AS total_trips,
          ROUND(AVG(CASE WHEN {ON_TIME} THEN 1.0 ELSE 0.0 END) * 100, 1)      AS on_time_pct,
          ROUND(AVG(CASE WHEN {EARLY}   THEN 1.0 ELSE 0.0 END) * 100, 1)      AS early_pct,
          ROUND(AVG(CASE WHEN {LATE}    THEN 1.0 ELSE 0.0 END) * 100, 1)      AS late_pct,
          ROUND(AVG(f.arrival_delay_seconds) / 60.0, 1)                        AS avg_delay_min,
          ROUND(PERCENTILE_APPROX(f.arrival_delay_seconds, 0.9) / 60.0, 1)    AS p90_delay_min
        FROM gold_stop_dwell_fact f
        JOIN silver_dim_route r ON f.route_id = r.route_id
        WHERE f.route_id = '{sid}'
          AND f.service_date >= '{sd}' AND f.service_date <= '{ed}'
          AND f.actual_arrival_ts IS NOT NULL
    """).collect()

    # ── Daily OTP trend ──────────────────────────────────────────────────────
    trend = spark.sql(f"""
        SELECT
          service_date,
          ROUND(AVG(CASE WHEN {ON_TIME} THEN 1.0 ELSE 0.0 END) * 100, 1) AS on_time_pct,
          COUNT(*) AS stops
        FROM gold_stop_dwell_fact
        WHERE route_id = '{sid}'
          AND service_date >= '{sd}' AND service_date <= '{ed}'
          AND actual_arrival_ts IS NOT NULL
        GROUP BY service_date
        ORDER BY service_date
    """).collect()

    # ── Top 5 most delayed stops ─────────────────────────────────────────────
    top_delayed = spark.sql(f"""
        SELECT
          COALESCE(s.stop_name, f.stop_id) AS stop_name,
          COUNT(*)                          AS observations,
          ROUND(AVG(f.arrival_delay_seconds) / 60.0, 1) AS avg_delay_min,
          ROUND(AVG(CASE WHEN {LATE} THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct
        FROM gold_stop_dwell_fact f
        LEFT JOIN silver_dim_stop s ON f.stop_id = s.stop_id
        WHERE f.route_id = '{sid}'
          AND f.service_date >= '{sd}' AND f.service_date <= '{ed}'
          AND f.actual_arrival_ts IS NOT NULL
        GROUP BY COALESCE(s.stop_name, f.stop_id)
        HAVING observations >= 5
        ORDER BY avg_delay_min DESC
        LIMIT 5
    """).collect()

    # ── Best and worst 3-hour window (Phoenix local, UTC-7) ───────────────────
    time_windows = spark.sql(f"""
        SELECT
          FLOOR((HOUR(TIMESTAMPADD(HOUR, -7, actual_arrival_ts))) / 3) * 3 AS window_start_hr,
          ROUND(AVG(CASE WHEN {ON_TIME} THEN 1.0 ELSE 0.0 END) * 100, 1)  AS on_time_pct,
          COUNT(*) AS stops
        FROM gold_stop_dwell_fact
        WHERE route_id = '{sid}'
          AND service_date >= '{sd}' AND service_date <= '{ed}'
          AND actual_arrival_ts IS NOT NULL
        GROUP BY window_start_hr
        HAVING stops >= 10
        ORDER BY on_time_pct DESC
    """).collect()

    # ── Dwell summary (inferred) ─────────────────────────────────────────────
    dwell = spark.sql(f"""
        SELECT
          ROUND(PERCENTILE_APPROX(dwell_seconds, 0.5), 0) AS p50_dwell_sec,
          ROUND(PERCENTILE_APPROX(dwell_seconds, 0.9), 0) AS p90_dwell_sec,
          COUNT(*) AS dwell_observations
        FROM gold_stop_dwell_inferred
        WHERE route_id = '{sid}'
          AND service_date >= '{sd}' AND service_date <= '{ed}'
    """).collect()

    return {
        "summary":       summary[0].asDict()    if summary       else {},
        "trend":         [r.asDict() for r in trend],
        "top_delayed":   [r.asDict() for r in top_delayed],
        "time_windows":  [r.asDict() for r in time_windows],
        "dwell":         dwell[0].asDict()      if dwell         else {},
        "best_window":   time_windows[0].asDict()  if time_windows  else None,
        "worst_window":  time_windows[-1].asDict() if time_windows  else None,
    }

def fmt_window(hr: int) -> str:
    """Convert 0-23 hour to e.g. '6 AM–9 AM'."""
    def fmt(h):
        return f"{h % 12 or 12} {'AM' if h < 12 else 'PM'}"
    return f"{fmt(hr)}–{fmt(hr + 3)}"

# COMMAND ----------

# MAGIC %md ## Step 4 — Generate AI Narrative via FMAPI

# COMMAND ----------

fmapi_client = OpenAI(
    api_key=FMAPI_TOKEN,
    base_url=FMAPI_BASE_URL,
)

def generate_narrative(route_name: str, data: dict) -> str:
    s = data["summary"]
    dw = data["dwell"]

    trend_lines = "\n".join(
        f"  {r['service_date']}: {r['on_time_pct']}% on-time ({r['stops']} stop observations)"
        for r in data["trend"]
    )
    delay_lines = "\n".join(
        f"  {r['stop_name']}: avg +{r['avg_delay_min']}min, {r['late_pct']}% late ({r['observations']} obs)"
        for r in data["top_delayed"]
    )
    best  = fmt_window(int(data["best_window"]["window_start_hr"]))  if data["best_window"]  else "N/A"
    worst = fmt_window(int(data["worst_window"]["window_start_hr"])) if data["worst_window"] else "N/A"

    prompt = f"""You are a transit performance analyst writing a weekly report for a transit planner or operations manager at Valley Metro in Phoenix, Arizona.

Route: {route_name}
Report period: {week_label}

PERFORMANCE DATA:
- Overall OTP: {s.get('on_time_pct', 'N/A')}% on-time, {s.get('early_pct', 'N/A')}% early, {s.get('late_pct', 'N/A')}% late
- Average delay: {s.get('avg_delay_min', 'N/A')} min | P90 delay: {s.get('p90_delay_min', 'N/A')} min
- Total trips observed: {s.get('total_trips', 'N/A')} | Stop observations: {s.get('total_stops', 'N/A')}
- Dwell time: P50={dw.get('p50_dwell_sec', 'N/A')}s, P90={dw.get('p90_dwell_sec', 'N/A')}s ({dw.get('dwell_observations', 0)} observations)
- Best performing time window: {best}
- Most challenging time window: {worst}

DAILY OTP TREND:
{trend_lines}

TOP 5 MOST DELAYED STOPS:
{delay_lines}

Write a professional 3-paragraph summary:
1. Overall performance assessment for the week — is this route healthy, concerning, or improving?
2. Key problem areas — which stops and time windows need attention and why?
3. Actionable recommendations — what should operations staff investigate or adjust?

Be specific, concise, and direct. Avoid generic phrases. Do not repeat the raw numbers verbatim — synthesize them into insight."""

    response = fmapi_client.chat.completions.create(
        model="databricks-meta-llama-3-3-70b-instruct",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=600,
        temperature=0.4,
    )
    return response.choices[0].message.content.strip()

# COMMAND ----------

# MAGIC %md ## Step 5 — Build and Send HTML Emails

# COMMAND ----------

def build_html(route_name: str, data: dict, narrative: str, user_email: str) -> str:
    s   = data["summary"]
    dw  = data["dwell"]

    otp_color = (
        "#4db6ac" if float(s.get("on_time_pct", 0)) >= 80
        else "#ffb74d" if float(s.get("on_time_pct", 0)) >= 60
        else "#e57373"
    )

    trend_rows = "".join(
        f"<tr><td style='padding:4px 8px;color:#9ca3af;font-size:12px;'>{r['service_date']}</td>"
        f"<td style='padding:4px 8px;color:#f9fafb;font-size:12px;text-align:right;'>{r['on_time_pct']}%</td>"
        f"<td style='padding:4px 8px;font-size:12px;'>"
        f"<div style='background:#374151;border-radius:4px;overflow:hidden;width:120px;height:8px;'>"
        f"<div style='background:{otp_color};width:{r[\"on_time_pct\"]}%;height:100%;'></div></div></td></tr>"
        for r in data["trend"]
    )

    delay_rows = "".join(
        f"<tr><td style='padding:4px 8px;color:#f9fafb;font-size:12px;'>{r['stop_name']}</td>"
        f"<td style='padding:4px 8px;color:#e57373;font-size:12px;text-align:right;'>+{r['avg_delay_min']}m</td>"
        f"<td style='padding:4px 8px;color:#9ca3af;font-size:12px;text-align:right;'>{r['late_pct']}% late</td></tr>"
        for r in data["top_delayed"]
    )

    best  = fmt_window(int(data["best_window"]["window_start_hr"]))  if data["best_window"]  else "N/A"
    worst = fmt_window(int(data["worst_window"]["window_start_hr"])) if data["worst_window"] else "N/A"

    narrative_html = "".join(
        f"<p style='margin:0 0 12px;color:#d1d5db;font-size:14px;line-height:1.6;'>{p.strip()}</p>"
        for p in narrative.split("\n\n") if p.strip()
    )

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#030712;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
  <div style="max-width:600px;margin:0 auto;padding:32px 16px;">

    <!-- Header -->
    <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:24px;margin-bottom:16px;">
      <p style="margin:0 0 4px;color:#6b7280;font-size:12px;text-transform:uppercase;letter-spacing:.08em;">Weekly Performance Report</p>
      <h1 style="margin:0 0 4px;color:#f9fafb;font-size:22px;font-weight:700;">Route {route_name}</h1>
      <p style="margin:0;color:#6b7280;font-size:13px;">{week_label}</p>
    </div>

    <!-- KPIs -->
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:12px;margin-bottom:16px;">
      {''.join(f"""
      <div style="background:#111827;border:1px solid #1f2937;border-radius:10px;padding:16px;text-align:center;">
        <p style="margin:0 0 4px;color:#6b7280;font-size:10px;text-transform:uppercase;letter-spacing:.06em;">{label}</p>
        <p style="margin:0;color:{color};font-size:20px;font-weight:700;">{value}</p>
      </div>""" for label, value, color in [
          ("On-Time", f"{s.get('on_time_pct', '—')}%", otp_color),
          ("Avg Delay", f"+{s.get('avg_delay_min', '—')}m", "#9ca3af"),
          ("Dwell P50", f"{dw.get('p50_dwell_sec', '—')}s", "#9ca3af"),
          ("Dwell P90", f"{dw.get('p90_dwell_sec', '—')}s", "#9ca3af"),
      ])}
    </div>

    <!-- AI Narrative -->
    <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:24px;margin-bottom:16px;">
      <p style="margin:0 0 16px;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.08em;">AI Performance Brief</p>
      {narrative_html}
    </div>

    <!-- Daily trend -->
    <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:24px;margin-bottom:16px;">
      <p style="margin:0 0 12px;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.08em;">Daily OTP</p>
      <table style="width:100%;border-collapse:collapse;">{trend_rows}</table>
    </div>

    <!-- Top delayed stops -->
    <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:24px;margin-bottom:16px;">
      <p style="margin:0 0 12px;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.08em;">Top Delayed Stops</p>
      <table style="width:100%;border-collapse:collapse;">{delay_rows}</table>
    </div>

    <!-- Time windows -->
    <div style="background:#111827;border:1px solid #1f2937;border-radius:12px;padding:24px;margin-bottom:16px;">
      <p style="margin:0 0 12px;color:#6b7280;font-size:11px;text-transform:uppercase;letter-spacing:.08em;">Time of Day</p>
      <div style="display:flex;gap:16px;">
        <div style="flex:1;background:#052e16;border:1px solid #166534;border-radius:8px;padding:12px;text-align:center;">
          <p style="margin:0 0 4px;color:#4ade80;font-size:10px;text-transform:uppercase;">Best window</p>
          <p style="margin:0;color:#f9fafb;font-size:14px;font-weight:600;">{best}</p>
        </div>
        <div style="flex:1;background:#450a0a;border:1px solid #991b1b;border-radius:8px;padding:12px;text-align:center;">
          <p style="margin:0 0 4px;color:#f87171;font-size:10px;text-transform:uppercase;">Most challenging</p>
          <p style="margin:0;color:#f9fafb;font-size:14px;font-weight:600;">{worst}</p>
        </div>
      </div>
    </div>

    <!-- Footer -->
    <p style="text-align:center;color:#374151;font-size:11px;margin-top:24px;">
      Sent to {user_email} · Transit Performance Platform ·
      <a href="#" style="color:#6b7280;">Manage subscriptions</a>
    </p>
  </div>
</body>
</html>"""


def send_email(to_email: str, subject: str, html: str) -> bool:
    r = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type":  "application/json",
        },
        json={
            "from":    "Transit Platform <reports@yourdomain.com>",
            "to":      [to_email],
            "subject": subject,
            "html":    html,
        },
    )
    if r.status_code not in (200, 201):
        print(f"  Resend error {r.status_code}: {r.text}")
        return False
    return True

# COMMAND ----------

# MAGIC %md ## Step 6 — Process All Subscriptions

# COMMAND ----------

results = []

for sub in subscriptions:
    user_id    = sub["user_id"]
    route_id   = sub["route_id"]
    route_name = sub["route_name"]
    to_email   = email_map.get(user_id)

    if not to_email:
        print(f"  SKIP {route_name}: no email resolved for user {user_id}")
        results.append({"user_id": user_id, "route_id": route_id, "status": "NO_EMAIL"})
        continue

    print(f"\n→ {route_name} → {to_email}")

    try:
        print(f"  Querying gold tables…")
        data = get_route_data(route_id)

        if not data["summary"]:
            print(f"  No data for this route/period — skipping.")
            results.append({"user_id": user_id, "route_id": route_id, "status": "NO_DATA"})
            continue

        print(f"  Generating AI narrative…")
        narrative = generate_narrative(route_name, data)

        print(f"  Building and sending email…")
        short_name = data["summary"].get("route_short_name", route_name)
        html = build_html(short_name, data, narrative, to_email)
        ok   = send_email(to_email, f"Weekly Report: Route {short_name} — {week_label}", html)

        status = "SENT" if ok else "SEND_FAILED"
        print(f"  {status}")
        results.append({"user_id": user_id, "route_id": route_id, "status": status})

    except Exception as e:
        print(f"  ERROR: {e}")
        results.append({"user_id": user_id, "route_id": route_id, "status": f"ERROR: {e}"})

# COMMAND ----------

# MAGIC %md ## Step 7 — Log Results

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_WEEKLY_REPORT_LOG} (
        logged_at   TIMESTAMP,
        week_start  STRING,
        week_end    STRING,
        user_id     STRING,
        route_id    STRING,
        status      STRING
    )
    USING DELTA
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

if results:
    now_utc = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    log_rows = [
        (now_utc, start_date.isoformat(), end_date.isoformat(), r["user_id"], r["route_id"], r["status"])
        for r in results
    ]
    log_df = spark.createDataFrame(log_rows, ["logged_at", "week_start", "week_end", "user_id", "route_id", "status"])
    log_df.write.format("delta").mode("append").saveAsTable(GOLD_WEEKLY_REPORT_LOG)

sent    = sum(1 for r in results if r["status"] == "SENT")
skipped = len(results) - sent
print(f"\n✓ Done — {sent} sent, {skipped} skipped/failed")
dbutils.notebook.exit(f"{sent} emails sent, {skipped} skipped")
