# Transit Performance & Playback Platform (Planner-Facing)
## Tentative Build Plan + Architecture + Schemas (MVP Spec for Claude)

**Goal:** Production-grade Databricks-first platform for planners that ingests full-fleet GTFS Static + GTFS-RT (TripUpdates + VehiclePositions), supports trip playback, dwell analysis, congestion ribbon, anomaly detection (route + 15-min buckets), and AI-generated incident briefs.

**Non-goals (MVP):**
- Replacing Swiftly predictions
- Public rider app
- External traffic APIs (derive congestion from fleet speeds)
- Trip-level anomaly detection (too noisy for MVP)

---

## 1) Locked Stack

### Data Plane (Primary)
- **Databricks (bootcamp workspace)**
  - **Delta Lake**: Bronze/Silver/Gold tables
  - **Structured Streaming**: microbatch ingestion (30–60s)
  - **Workflows**: orchestration (streaming + nightly + hourly + weekly jobs)

### App Plane
- **Render**: API + scheduled lightweight tasks (optional; main orchestration in Databricks Workflows)
- **Supabase Postgres**: planner auth + user/config state (NOT analytics)
- **Planner UI**: **Next.js (React + TS)** + **Mapbox GL JS**
  - Playback map and route grid
  - Charts: Recharts/ECharts

### AI
- Deterministic detection + attribution rules
- LLM used for **incident brief generation** (summarize evidence; never “decide”)
  - Either Databricks model serving OR external (Claude/OpenAI) from Render/Databricks job

### Kafka
- **Not required** for GTFS-RT polling MVP (add later only if bootcamp requires it)

---

## 2) Compute / Cluster Architecture (Cost-Conscious)

### Cluster Type
- **Job clusters** (not all-purpose)
- **Auto-terminate**: 10–15 minutes idle
- Start small: **1 driver + 1–2 workers** (scale only if needed)

### Workload Separation
- Streaming job cluster (small, long-running but cheap)
- Nightly reconstruction job cluster (ephemeral)
- Hourly anomaly job cluster (ephemeral; very small)
- Weekly baseline job cluster (ephemeral; moderate)

### Delta Practices
- Partition only by **service_date**
- Avoid partitioning by route_id / timestamps (small-file explosion)
- Use **MERGE** to incrementally update Gold
- **OPTIMIZE** weekly (not daily); **ZORDER** only where proven beneficial (trip_id on playback table)

Retention:
- Bronze: 7–30 days
- Silver/Gold: 12 months (or longer if feasible)

---

## 3) End-to-End Workflow (Jobs)

### Job A — Streaming Ingestion (continuous microbatch)
**Schedule:** continuous, microbatch every 30–60s  
**Inputs:** GTFS-RT TripUpdates + VehiclePositions  
**Writes:** Bronze event tables (structured rows only), checkpoints enabled

Core rules:
- State-change logging (don’t write duplicates)
- Always record new stop_sequence, new arrival/departure timestamps, delay changes, trip status changes
- VehiclePositions may be downsampled in Silver (store full res in Bronze short-term)

### Job B — Static GTFS Loader (daily)
**Schedule:** daily  
**Inputs:** GTFS static ZIP  
**Writes:** dims + reference tables (Silver/Gold as appropriate)

### Job C — Nightly Reconstruction (daily)
**Schedule:** nightly  
**Reads:** Silver events + static dims  
**Writes:** Gold facts/aggregates:
- trip_timeline_fact
- stop_dwell_fact
- route_metrics_15min
- route_segment_congestion (route stop-to-stop baseline index)
- trip_path_fact (downsampled points for playback)

### Job D — Anomaly Detection (hourly)
**Schedule:** hourly  
**Reads:** route_metrics_15min + route_metrics_baseline + route_segment_congestion  
**Writes:** anomaly_events (route + 15-min bucket, congestion-aware classification)

### Job E — Baseline Builder (weekly)
**Schedule:** weekly  
**Reads:** route_metrics_15min (last 30–60 comparable days)  
**Writes:** route_metrics_baseline

### Job F — AI Incident Brief Generator (triggered)
**Schedule:** triggered when anomalies exceed severity threshold  
**Reads:** anomaly_events + supporting metrics snapshot + (optional) GTFS-RT alerts text  
**Writes:** incident_reports (structured + narrative)

---

## 4) MVP Planner UI Surfaces

### 4.1 Route Grid (core “planner wow”)
- Select route + direction + service_date
- Grid rows = trips; columns = stop sequence/time buckets
- Cell color = lateness vs schedule (configurable)
- Under each trip row: **congestion ribbon gradient** derived from route_segment_congestion

### 4.2 Playback Map
- Select trip (from grid)
- Show polyline path + animated marker
- Timeline slider
- Optional: segment overlay colored by congestion level

### 4.3 Dwell Analysis
- Route/stop dwell heatmap (stop vs time-of-day)
- Top dwell inflation stops + drilldown

### 4.4 Incidents
- List active anomalies
- Click → view AI brief + supporting metrics and baseline comparison

---

## 5) Key Modeling Decisions (Locked)

### 5.1 Congestion Strategy
- **No external traffic API**
- Compute route congestion from **fleet speeds**:
  - Use **stop-to-stop segments** (planner-friendly)
  - Build baseline speeds by segment/time_bucket (weekly)
  - Compute congestion_index nightly: current_speed / baseline_speed
- Use congestion in:
  - anomaly classification
  - AI briefs
- Do NOT modify OTP definition in MVP

### 5.2 Anomaly Grain
- route_id + direction_id + **15-minute time bucket**
- Add weekday_type (weekday/weekend/holiday) to baseline

---

## 6) Data Contracts (App/API)

### Render API (reads Gold only)
Suggested endpoints:
- `GET /routes` (static route list)
- `GET /route/{route_id}/grid?date=YYYY-MM-DD&direction=0|1`
- `GET /trip/{trip_id}/playback?date=YYYY-MM-DD`
- `GET /route/{route_id}/dwell?date=YYYY-MM-DD`
- `GET /incidents?date=YYYY-MM-DD`
- `GET /incident/{incident_id}`
- `POST /scenario` (what-if config; stored in Supabase)
- `GET /scenario/{id}/results`

### Supabase (planner state)
- users, roles
- saved views / pinned routes
- scenario configs and comments
- incident acknowledgements

---

# 7) Schemas (Tentative)

> Naming convention: `bronze.*`, `silver.*`, `gold.*` Delta tables.

## 7.1 Bronze

### `bronze.gtfs_static_raw`
- ingest_ts (timestamp)
- source_url (string)
- zip_bytes_path (string) OR file reference
- feed_version (string, nullable)

### `bronze.trip_updates_events`
**Grain:** (event_ts, trip_id, stop_sequence) where available  
- ingest_ts (timestamp)
- feed_ts (timestamp) — timestamp from feed header
- service_date (date)
- agency_id (string, nullable)
- route_id (string, nullable)
- trip_id (string)
- vehicle_id (string, nullable)
- stop_id (string, nullable)
- stop_sequence (int, nullable)
- arrival_ts (timestamp, nullable)
- departure_ts (timestamp, nullable)
- delay_seconds (int, nullable)
- schedule_relationship (string, nullable)
- event_hash (string)  // for dedupe/state-change logging
- raw_minimal_json (string, optional)
Partition: service_date

### `bronze.vehicle_position_events`
**Grain:** (event_ts, vehicle_id)  
- ingest_ts (timestamp)
- feed_ts (timestamp)
- service_date (date)
- vehicle_id (string)
- trip_id (string, nullable)
- route_id (string, nullable)
- lat (double)
- lon (double)
- bearing (double, nullable)
- speed (double, nullable)
- odometer (double, nullable)
- event_hash (string)
Partition: service_date

### `bronze.alerts_events` (optional MVP+)
- ingest_ts, feed_ts, service_date
- alert_id, route_id, stop_id, trip_id
- effect, cause, severity_level
- header_text, description_text
Partition: service_date

---

## 7.2 Silver (Conformed)

### `silver.dim_route`
- route_id (string) PK
- route_short_name, route_long_name
- route_type (int)
- agency_id (string)

### `silver.dim_stop`
- stop_id (string) PK
- stop_name
- lat, lon
- parent_station (string, nullable)

### `silver.dim_trip`
- trip_id (string) PK
- route_id (string)
- service_id (string)
- direction_id (int, nullable)
- shape_id (string, nullable)
- trip_headsign (string, nullable)

### `silver.fact_stop_schedule`
**Grain:** (service_date, trip_id, stop_sequence)  
- service_date (date)
- trip_id (string)
- stop_id (string)
- stop_sequence (int)
- scheduled_arrival_ts (timestamp)
- scheduled_departure_ts (timestamp)
Partition: service_date

### `silver.fact_trip_updates`
**Grain:** (service_date, trip_id, stop_sequence, feed_ts)  
- service_date
- feed_ts
- trip_id
- route_id
- vehicle_id
- stop_id
- stop_sequence
- arrival_ts
- departure_ts
- delay_seconds
- schedule_relationship
- is_state_change (boolean)
Partition: service_date

### `silver.fact_vehicle_positions`
**Grain:** (service_date, vehicle_id, event_ts_bucket)  
- service_date
- event_ts (timestamp)
- vehicle_id
- trip_id
- route_id
- lat, lon
- bearing, speed
- is_downsampled (boolean)
Partition: service_date

---

## 7.3 Gold (Analytics + Planner Features)

### `gold.trip_timeline_fact`
**Grain:** (service_date, trip_id)  
- service_date
- trip_id
- route_id
- direction_id
- planned_start_ts
- actual_start_ts (nullable)
- planned_end_ts
- actual_end_ts (nullable)
- late_start_seconds (int, nullable)
- total_trip_delay_seconds (int, nullable)
- dwell_total_seconds (int, nullable)
- enroute_delay_seconds (int, nullable)
- stop_count (int)
- data_quality_flag (string, nullable)
Partition: service_date

### `gold.stop_dwell_fact`
**Grain:** (service_date, trip_id, stop_sequence)  
- service_date
- trip_id
- route_id
- direction_id
- stop_id
- stop_sequence
- scheduled_dwell_seconds (int, nullable)
- actual_dwell_seconds (int, nullable)
- dwell_delta_seconds (int, nullable)
- arrival_delay_seconds (int, nullable)
- departure_delay_seconds (int, nullable)
Partition: service_date

### `gold.trip_path_fact`
**Grain:** (service_date, trip_id, point_seq)  
- service_date
- trip_id
- route_id
- direction_id
- point_ts (timestamp)
- lat, lon
- speed (double, nullable)
- point_seq (int)
Partition: service_date

### `gold.route_metrics_15min`
**Grain:** (service_date, route_id, direction_id, time_bucket_15min)  
- service_date
- route_id
- direction_id
- time_bucket_15min (timestamp)
- trip_count (int)
- avg_delay_seconds (double)
- p90_delay_seconds (double)
- avg_dwell_delta_seconds (double, nullable)
- avg_late_start_seconds (double, nullable)
- congestion_index_avg (double, nullable)
Partition: service_date

### `gold.route_metrics_baseline`
**Grain:** (route_id, direction_id, time_bucket_15min_of_day, weekday_type)  
- route_id
- direction_id
- time_bucket_15min_of_day (string) // "07:30"
- weekday_type (string) // weekday/weekend/holiday
- baseline_p90_delay_seconds (double)
- baseline_congestion_index (double, nullable)
- baseline_dwell_delta_seconds (double, nullable)
- baseline_late_start_seconds (double, nullable)

### `gold.route_segment_congestion`
**Grain:** (service_date, route_id, direction_id, segment_id, time_bucket_5min)  
- service_date
- route_id
- direction_id
- segment_id (string) // "{stop_id_a}->{stop_id_b}"
- stop_id_a
- stop_id_b
- time_bucket_5min (timestamp)
- avg_speed_mps (double)
- baseline_speed_mps (double)
- congestion_index (double)
- congestion_level (string) // green/yellow/orange/red
Partition: service_date

### `gold.anomaly_events`
- event_id (string, uuid)
- detected_ts (timestamp)
- service_date (date)
- route_id
- direction_id
- time_bucket_15min
- severity_score (double)
- delay_delta_vs_baseline_seconds (double)
- congestion_delta_vs_baseline (double, nullable)
- likely_cause (string) // traffic/dwell/late_start/mixed
- confidence (double)
- metrics_snapshot_json (string)
- status (string) // open/closed/ack
Partition: service_date

### `gold.incident_reports`
- incident_id (string, uuid)
- created_ts (timestamp)
- anomaly_event_id (string)
- route_id
- direction_id
- service_date
- time_bucket_15min
- title (string)
- narrative (string)
- bullets_json (string)
- model_name (string, nullable)
- prompt_version (string, nullable)
Partition: service_date

---

# 8) Build Plan (MVP → Enhanced)

## Phase 0 — Repo + Dev Workflow (0.5–1 day)
- Create mono-repo with `/databricks`, `/api`, `/frontend`, `/docs`
- Databricks Repo connected to GitHub
- Local dev in Cursor/VS Code; Claude used for generation/review

## Phase 1 — Ingestion MVP (2–4 days)
- TripUpdates streaming to Bronze + checkpointing
- VehiclePositions streaming to Bronze
- State-change logging (TripUpdates) + basic dedupe (positions)
- Load GTFS static to dims + stop schedule fact

## Phase 2 — Nightly Reconstruction MVP (3–6 days)
- Build `gold.trip_timeline_fact`, `gold.stop_dwell_fact`
- Build `gold.trip_path_fact` (downsampled)
- Build `gold.route_metrics_15min`

## Phase 3 — Congestion Ribbon (3–6 days)
- Stop-to-stop segments
- Compute `gold.route_segment_congestion`
- UI: render gradient ribbon under trip rows

## Phase 4 — Baselines + Anomalies (3–5 days)
- Weekly baseline builder
- Hourly anomaly detector + cause classification
- Incident list in UI

## Phase 5 — AI Incident Briefs (2–4 days)
- Prompt + wiring
- Generate briefs for severe anomalies
- UI shows brief + evidence

## Phase 6 — What-if Simulation (Stretch; 4–8 days)
- Store scenarios in Supabase
- Batch compute “remove dwell” / “remove late start” impacts
- UI compare view

---

# 9) Known Risks / Assumptions
- GTFS-RT completeness varies (trip_id/stop_sequence missing)
- VehiclePositions storage needs downsampling
- Map-matching to shapes is complex; MVP uses stop-to-stop segments first
- OTP definition must be documented (thresholds, timepoints)

---

# 10) MVP Definition (“Done”)
- Streaming ingestion for TripUpdates + VehiclePositions running
- Nightly Gold tables for dwell + playback + route metrics
- Planner UI: route grid + congestion ribbon + playback map
- Incidents panel (AI briefs optional if time constrained)
- Docs: architecture + schemas + runbook

