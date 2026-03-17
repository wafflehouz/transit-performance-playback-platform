// ── Databricks Gold table shapes ──────────────────────────────────────────────

export interface RouteMetrics15Min {
  service_date: string
  route_id: string
  direction_id: number
  time_bucket_15min: string
  trip_count: number
  avg_delay_seconds: number
  p90_delay_seconds: number
  pct_on_time: number
  avg_dwell_delta_seconds: number | null
  avg_late_start_seconds: number | null
}

export interface AnomalyEvent {
  event_id: string
  detected_ts: string
  service_date: string
  route_id: string
  direction_id: number
  time_bucket_15min: string
  severity: string
  trigger_flags: string
  delay_z_score: number | null
  otp_drop_pp: number | null
  delay_vs_baseline_seconds: number | null
}

export interface IncidentReport {
  incident_id: string
  created_ts: string
  anomaly_event_id: string
  route_id: string
  direction_id: number
  service_date: string
  time_bucket_15min: string
  title: string
  narrative: string
  bullets_json: string
  model_name: string | null
}

export interface TripPathPoint {
  trip_id: string
  point_seq: number
  point_ts: string
  lat: number
  lon: number
  speed: number | null
  bearing: number | null
}

export interface StopDwellRow {
  service_date: string
  route_id: string
  stop_id: string
  stop_name: string | null
  hour_of_day: number
  avg_dwell_seconds: number
  p90_dwell_seconds: number
  sample_count: number
}

export interface DimRoute {
  route_id: string
  route_short_name: string
  route_long_name: string
  route_type: number
}

// ── Supabase table shapes ──────────────────────────────────────────────────────

export interface RouteSubscription {
  id: string
  user_id: string
  route_id: string | null
  group_name: string | null
  frequency: 'weekly'
  created_at: string
}

export interface UserProfile {
  id: string
  email: string
  full_name: string | null
  role: 'planner' | 'admin'
  created_at: string
}
