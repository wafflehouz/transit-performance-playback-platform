# Capstone Proposal: Transit Performance & Playback Platform

**Submitted by:** Josh (wafflehouz)
**Bootcamp:** Data Engineering with Databricks
**Date:** March 2026

---

## Problem Statement

Valley Metro (Phoenix) operates over 100 bus routes serving a metro area of 5 million people. Planners currently rely on a commercial CAD/AVL vendor (Clever Devices) for performance data, but that system delivers batch exports, not near-real-time insight. When a route degrades — buses running 6+ minutes late, stops being skipped, dwell time spiking — planners learn about it hours or days after the fact.

This project builds a **planner-facing performance intelligence platform** that ingests live GTFS-RT feeds, constructs a historical analytics foundation, detects anomalies in near-real-time, and uses generative AI to surface actionable incident briefs — turning raw telemetry into decisions.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      DATA PLANE (Databricks)                │
│                                                             │
│  GTFS-RT APIs ──► Pollers ──► Raw NDJSON Snapshots (Volume) │
│  GTFS Static  ──────────────────────────────────────────┐   │
│                                                         ▼   │
│  AutoLoader  ──► Bronze Delta  ──►  Silver Delta  ──►  Gold │
│  (append-only)   (event audit)     (dedup/enrich)   (facts) │
│                                                             │
│  Workflows:  poller (hourly) │ silver (2:30 AM) │ gold (4 AM) │
└─────────────────────────────────────────────────────────────┘
           │ DBSQL / REST API
           ▼
┌──────────────────────┐     ┌────────────────────────────┐
│   Next.js + Mapbox   │────►│  Claude API (Anthropic)    │
│   Planner UI         │     │  Incident Brief Generator  │
└──────────────────────┘     └────────────────────────────┘
```

---

## Data Sources

| Source | Feed | Cadence |
|--------|------|---------|
| Mecatran / Valley Metro | GTFS-RT TripUpdates (JSON) | Polled every 60 s |
| Mecatran / Valley Metro | GTFS-RT VehiclePositions (JSON) | Polled every 60 s |
| Phoenix Open Data | GTFS Static ZIP (routes, stops, trips, schedules) | Daily, ETag-checked |

---

## Medallion Architecture

### Bronze — Raw Event Store
- AutoLoader ingests NDJSON snapshot files written by pollers
- Append-only; full audit trail retained 30 days
- Tables: `bronze_trip_updates_events`, `bronze_vehicle_position_events`

### Silver — Cleaned & Enriched
- **TripUpdates:** state-change deduplication via `lag()` window; only meaningful transitions written
- **VehiclePositions:** downsampled to one position per vehicle per 30-second bucket
- **Dimensions:** `silver_dim_route`, `silver_dim_stop`, `silver_dim_trip`, `silver_fact_stop_schedule`

### Gold — Analytics-Ready Facts
| Table | Description |
|-------|-------------|
| `gold_stop_dwell_fact` | Scheduled vs actual dwell delta per stop per trip (GTFS-RT based) |
| `gold_trip_timeline_fact` | Trip-level summary: actual start/end, total delay, late-start seconds |
| `gold_trip_path_fact` | Ordered position sequence for playback map (30 s resolution) |
| `gold_route_metrics_15min` | Route+direction bucketed into 15-min windows: avg/p90 delay, OTP, dwell delta |
| `gold_stop_dwell_inferred` | Physical dwell inferred from VP trajectory + haversine stop proximity |
| `gold_route_metrics_baseline` | Rolling 28-day baseline by route, service type, and time-of-day bucket |
| `gold_anomaly_events` | Flagged buckets where delay or OTP deviates significantly from baseline |

---

## Pipeline Orchestration

Three Databricks Workflow jobs with explicit task dependencies:

```
transit-poller (hourly, continuous loop)
  └── Notebooks 10, 11 (TripUpdates + VP pollers, parallel, self-loop 55 min)

transit-silver-nightly (2:30 AM)
  └── 20 & 21 (Bronze AutoLoader) → 30 & 31 (Silver, parallel)

transit-gold-nightly (4:00 AM)
  └── 40 (stop dwell)
      → 41 (trip timeline)
      → 42 (route metrics 15-min) ──→ 45 (baseline) → 46 (anomaly events)
         43 (trip path)          ──┘ (parallel with 45/46)
         44 (dwell inferred)     ──┘
```

All jobs run on serverless compute. Full nightly gold chain completes by ~4:45 AM.

---

## Anomaly Detection

Detection is entirely deterministic — no ML model required for the MVP. A 15-minute route+direction bucket is flagged when:

| Severity | Condition |
|----------|-----------|
| `warning` | avg delay ≥ 3 min AND > baseline avg + 1.5 × stddev |
| `critical` | avg delay ≥ 6 min AND > baseline avg + 2.0 × stddev |
| `warning` | OTP dropped ≥ 15 percentage points below baseline |
| `critical` | OTP dropped ≥ 25 percentage points below baseline |

Thresholds are derived from Valley Metro's own CAD/AVL runtime analysis parameters (Clever Devices deviation band classification: <1 min / 1–3 min / 3–6 min / >6 min). Baselines are stratified by service type (weekday / saturday / sunday) so fast and slow routes each calibrate independently.

**On-Time Performance** follows the FTA standard: a stop observation is on-time if it falls within −60 s (early tolerance) to +299 s (late tolerance). Validated against Swiftly industry benchmarks for Route 41 — our stop-level OTP (69.4%) brackets Swiftly's published figure (68.1%).

---

## AI Integration — Incident Brief Generator

When `gold_anomaly_events` contains `critical` rows for a service date, an LLM-powered notebook generates a structured plain-language brief for planners.

**Architecture:**

```
gold_anomaly_events  ──►  Brief Builder Notebook (Python)
gold_route_metrics_15min      │
gold_stop_dwell_inferred       │  Structured context payload
                               ▼
                     Claude API (claude-sonnet-4-6)
                               │
                               ▼
                     gold_incident_reports  (Delta)
                     + Planner UI notification
```

**What the AI does:**
- Summarizes which routes degraded, when, and by how much
- Highlights whether dwell, travel time, or scheduling is the primary driver
- Identifies stops with persistent high dwell as potential schedule revision candidates
- Produces a brief in plain language a planner can share directly in a standup or email

**What the AI does not do:**
- Make operational decisions
- Diagnose root causes that aren't in the data (incidents, weather are not ingested)
- Replace the planner's judgment

The prompt feeds structured evidence (delay z-scores, OTP drop, dwell spikes, affected stop list) and instructs the model to reason transparently and flag uncertainty when sample sizes are small.

---

## Front-End (Planner UI)

**Stack:** Next.js (React + TypeScript) · Mapbox GL JS · Recharts
**Hosting:** Render (or Databricks Apps)
**Auth:** Supabase (planner login only; analytics state lives in Delta)

### Views

**1. Route Grid**
Heat-map table of routes × 15-min time buckets. Color-encoded by severity. Click a cell → anomaly detail drawer showing delay trend, OTP, and inferred dwell for that bucket.

**2. Trip Playback Map**
Select a service date + route. Animated vehicle markers replay movement along the corridor at configurable speed. Marker color encodes speed relative to schedule; stop markers pulse when inferred dwell > threshold.

**3. Incident Briefs Feed**
Chronological list of AI-generated briefs. Each brief links to the supporting anomaly rows and the raw metric charts that evidence it.

**Data access:** Databricks SQL warehouse REST API (or Databricks Apps embedded queries). No separate backend needed for read-only analytics queries.

---

## Validation Approach

| Metric | Our figure | External benchmark | Method |
|--------|-----------|-------------------|--------|
| Stop-level OTP | 69.4% | Swiftly: 68.1% | Route 41, same date |
| Inferred dwell p50 | 30 s | CAD/AVL model assumption: 30 s/stop | Route 29, 35th Ave area |
| Inferred dwell (high-activity stops) | 71–98 s avg | CAD/AVL CAD: 120–240 s | Route 29, 35th Ave |

The gap in dwell (our 71–98 s vs CAD/AVL 120–240 s) is explained by methodology: we detect stationary VP points (speed < 1 m/s) within 40 m of a stop, at 30-second resolution. The CAD/AVL system records door-open/close events directly from the farebox. Our figures systematically undercount by approximately one bucket (30 s) and miss dwell periods shorter than 30 s — acceptable accuracy for anomaly detection purposes.

---

## Technology Justification

| Choice | Rationale |
|--------|-----------|
| **Delta Lake** | ACID writes, partition pruning, `replaceWhere` for idempotent nightly rewrites |
| **Databricks Workflows** | Native dependency graph, serverless compute, no infrastructure to manage |
| **AutoLoader** | Schema inference + exactly-once file ingestion from Volume without custom state management |
| **PySpark (no UDFs for geometry)** | Haversine computed as native column expressions — no serialization overhead, runs on all executors |
| **Claude API** | Best-in-class instruction-following for structured evidence summarization; transparent reasoning |
| **Mapbox GL JS** | Vector tiles handle full-fleet playback without tiling pre-computation |

---

## Current Status

| Phase | Notebooks | Status |
|-------|-----------|--------|
| 0 — Setup | 00, 01 | ✅ Complete |
| 1 — Ingestion | 10, 11, 20, 21 | ✅ Complete, running in production |
| 2 — Silver | 30, 31 | ✅ Complete |
| 3 — Gold facts | 40, 41, 42, 43, 44 | ✅ Complete, validated against Swiftly |
| 4 — Anomaly | 45, 46 | ✅ Complete, pending 7-day data accumulation |
| 5 — AI briefs | 47 | 🔲 Next |
| 6 — UI | Next.js app | 🔲 Planned |

Live pipeline has been collecting Valley Metro data since early March 2026. Gold tables currently hold validated data for multiple service dates.
