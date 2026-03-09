# Databricks AI/BI Dashboards

Four Lakeview dashboards mirroring Swiftly transit analytics metrics.

## Dashboards

| File | Swiftly equivalent | Key metrics |
|------|-------------------|-------------|
| `60_route_performance.lvdash.json` | OTP & Schedule Adherence | OTP %, avg/p90 delay, delay heatmap by route×hour |
| `61_anomaly_monitor.lvdash.json` | Service Alerts | Critical/warning events, top offender routes, OTP drop triggers |
| `62_dwell_analysis.lvdash.json` | Dwell Time Analysis | Sched vs actual dwell, top inflation stops, VP-inferred dwell |
| `63_trip_health.lvdash.json` | Trip Reliability | Delay distribution, late start analysis, severe delay drill-down |

## Import into Databricks

1. In the Databricks workspace, go to **Dashboards** (left sidebar)
2. Click **Import** (top right)
3. Upload the `.lvdash.json` file
4. Open the imported dashboard and click **Edit** → set the SQL warehouse
5. Click **Publish**

Repeat for each file. Each dashboard is self-contained (no cross-dashboard dependencies).

## SQL Notes

- All queries target `tabular.dataexpert.*` tables — update catalog/schema if yours differs
- UTC→Phoenix offset is hardcoded as `-7` (Arizona does not observe DST)
- Default lookback windows: 7 days for operational views, 30 days for trend analysis
- OTP definition: FTA standard (-60s early to +299s late)
- Delay severity bands: <1 min=negligible, 1–3=moderate, 3–6=significant, ≥6=severe (CAD/AVL standard)

## Filters to add after import

Databricks Lakeview filters are configured in the UI after import. Recommended:
- **Date range** picker on `service_date` (all dashboards)
- **Route** multi-select on `route_id` (60, 62, 63)
- **Severity** select on `severity` (61)
