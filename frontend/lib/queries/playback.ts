// ── GPS Playback queries ───────────────────────────────────────────────────────
//
// Sources:
//   gold_trip_path_fact   — VP breadcrumb trail per trip (partitioned by service_date)
//   gold_stop_dwell_fact  — scheduled vs actual arrivals per stop-visit
//   silver_dim_trip       — route_id, direction_id, trip_headsign per trip
//   silver_dim_stop       — stop_name, lat, lon
// ─────────────────────────────────────────────────────────────────────────────

// Trips that have path data for a given route + service date
export function playbackTripListSql(routeId: string) {
  const id = routeId.replace(/'/g, "''")
  return `
    SELECT
      p.trip_id,
      t.direction_id,
      t.trip_headsign,
      MIN(p.point_ts)  AS first_ts,
      MAX(p.point_ts)  AS last_ts,
      COUNT(*)         AS point_count
    FROM gold_trip_path_fact p
    INNER JOIN silver_dim_trip t ON p.trip_id = t.trip_id AND t.route_id = '${id}'
    WHERE p.service_date = :serviceDate
    GROUP BY p.trip_id, t.direction_id, t.trip_headsign
    ORDER BY first_ts
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
      COALESCE(s.stop_name, f.stop_id)  AS stop_name,
      f.stop_sequence,
      s.lat,
      s.lon,
      f.scheduled_arrival_ts,
      f.actual_arrival_ts,
      f.arrival_delay_seconds,
      COALESCE(f.pickup_type, 0)         AS pickup_type
    FROM gold_stop_dwell_fact f
    LEFT JOIN silver_dim_stop s ON f.stop_id = s.stop_id
    WHERE f.service_date = :serviceDate
      AND f.trip_id = '${id}'
    ORDER BY f.stop_sequence
  `
}
