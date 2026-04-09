// ── GPS Playback queries ───────────────────────────────────────────────────────
//
// Sources:
//   gold_trip_path_fact      — VP breadcrumb trail per trip (partitioned by service_date)
//   gold_stop_dwell_fact     — scheduled vs actual arrivals per stop-visit
//   gold_rail_stop_actuals   — sub-minute VP-derived actuals for rail routes A/B/S
//                              (preferred over gold_stop_dwell_fact for rail via COALESCE)
//   silver_dim_trip          — route_id, direction_id, trip_headsign per trip
//   silver_dim_stop          — stop_name, lat, lon
// ─────────────────────────────────────────────────────────────────────────────

// Trips that have path data for a given route + service date.
// first_timepoint_scheduled_ts = scheduled time at the first GTFS timepoint (timepoint=1),
// matching the first column of the printed timetable. Falls back to first_stop_scheduled_ts
// (min scheduled time across all observed stops) when no timepoint data exists.
export function playbackTripListSql(routeId: string) {
  const id = routeId.replace(/'/g, "''")
  return `
    SELECT
      p.trip_id,
      t.direction_id,
      t.trip_headsign,
      MIN(p.point_ts)                                                         AS first_ts,
      MAX(p.point_ts)                                                         AS last_ts,
      COUNT(*)                                                                AS point_count,
      MIN(f.scheduled_arrival_ts)                                             AS first_stop_scheduled_ts,
      MIN(CASE WHEN ss.timepoint = 1 THEN f.scheduled_arrival_ts END)        AS first_timepoint_scheduled_ts
    FROM gold_trip_path_fact p
    INNER JOIN silver_dim_trip t ON p.trip_id = t.trip_id AND t.route_id = '${id}'
    LEFT JOIN gold_stop_dwell_fact f
      ON f.trip_id = p.trip_id AND f.service_date = p.service_date
    LEFT JOIN silver_fact_stop_schedule ss
      ON ss.trip_id = f.trip_id AND ss.stop_sequence = f.stop_sequence
    WHERE p.service_date = :serviceDate
    GROUP BY p.trip_id, t.direction_id, t.trip_headsign
    ORDER BY COALESCE(first_timepoint_scheduled_ts, first_stop_scheduled_ts), first_ts
  `
}

// Vehicle(s) that operated a trip — ordered by number of VP pings (most = primary)
export function playbackVehiclesSql(tripId: string) {
  const id = tripId.replace(/'/g, "''")
  return `
    SELECT vehicle_id, COUNT(*) AS ping_count
    FROM silver_fact_vehicle_positions
    WHERE service_date = :serviceDate
      AND trip_id = '${id}'
      AND vehicle_id IS NOT NULL
    GROUP BY vehicle_id
    ORDER BY ping_count DESC
  `
}

// All VP path points for a specific trip on a service date (ordered by sequence)
export function playbackPathSql(tripId: string) {
  const id = tripId.replace(/'/g, "''")
  return `
    SELECT point_seq, point_ts, lat, lon, speed_mps, bearing
    FROM gold_trip_path_fact
    WHERE service_date = :serviceDate
      AND trip_id = '${id}'
    ORDER BY point_seq
  `
}

// H3 congestion overlay for the trip's corridor on a service date.
// h3Indices derived client-side from the VP path (latLngToCell at resolution 9).
// Returns all 15-min time buckets so the client can scrub through them.
export function trafficCongestionSql(h3Indices: string[]) {
  if (h3Indices.length === 0) return null
  // h3-js returns hex strings ("8929b6d02c7ffff") but Databricks stores
  // h3_longlatash3(...).cast("string") as decimal integers ("617726991403581439").
  // Convert each hex index to its decimal string representation for the IN clause.
  const inList = h3Indices.map((h) => `'${BigInt('0x' + h).toString()}'`).join(', ')
  return `
    SELECT
      UNIX_TIMESTAMP(time_bucket_15min) * 1000  AS bucket_ms,
      h3_index,
      congestion_level,
      avg_speed_mps,
      p10_speed_mps
    FROM gold_route_segment_congestion
    WHERE service_date = :serviceDate
      AND h3_index IN (${inList})
    ORDER BY bucket_ms
  `
}

// Stop-level schedule vs actual for a specific trip — for OTP coloring and stop list
export function playbackStopsSql(tripId: string) {
  const id = tripId.replace(/'/g, "''")
  return `
    SELECT
      f.stop_id,
      COALESCE(s.stop_name, f.stop_id)                       AS stop_name,
      f.stop_sequence,
      s.lat,
      s.lon,
      f.scheduled_arrival_ts,
      COALESCE(r.actual_arrival_ts,     f.actual_arrival_ts)     AS actual_arrival_ts,
      COALESCE(r.arrival_delay_seconds, f.arrival_delay_seconds) AS arrival_delay_seconds,
      COALESCE(f.pickup_type, 0)                             AS pickup_type,
      d.dwell_seconds
    FROM gold_stop_dwell_fact f
    LEFT JOIN silver_dim_stop s ON f.stop_id = s.stop_id
    LEFT JOIN gold_stop_dwell_inferred d
      ON d.service_date = f.service_date
     AND d.trip_id = f.trip_id
     AND d.stop_sequence = f.stop_sequence
    LEFT JOIN gold_rail_stop_actuals r
      ON r.service_date = f.service_date
     AND r.trip_id = f.trip_id
     AND r.stop_sequence = f.stop_sequence
    WHERE f.service_date = :serviceDate
      AND f.trip_id = '${id}'
    ORDER BY f.stop_sequence
  `
}
