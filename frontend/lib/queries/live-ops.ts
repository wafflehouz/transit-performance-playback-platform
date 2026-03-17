export interface LiveVehicle {
  vehicle_id: string
  trip_id: string | null
  route_id: string | null
  route_short_name: string | null
  direction_id: number | null
  lat: number
  lon: number
  bearing: number | null
  speed_mps: number | null
  event_ts: string
}

export interface LiveStats {
  total_vehicles: number
  routes_active: number
  avg_speed_mps: number | null
  last_updated: string | null
}

export const LIVE_VEHICLES_SQL = `
  WITH latest AS (
    SELECT
      v.vehicle_id,
      v.trip_id,
      v.route_id,
      v.lat,
      v.lon,
      v.bearing,
      v.speed_mps,
      v.event_ts,
      ROW_NUMBER() OVER (PARTITION BY v.vehicle_id ORDER BY v.event_ts DESC) AS rn
    FROM silver_fact_vehicle_positions v
    WHERE v.service_date = TO_DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'America/Phoenix'))
  )
  SELECT
    l.vehicle_id,
    l.trip_id,
    l.route_id,
    r.route_short_name,
    t.direction_id,
    l.lat,
    l.lon,
    l.bearing,
    l.speed_mps,
    CAST(l.event_ts AS STRING) AS event_ts
  FROM latest l
  LEFT JOIN silver_dim_route r ON l.route_id = r.route_id
  LEFT JOIN silver_dim_trip t ON l.trip_id = t.trip_id
  WHERE l.rn = 1
    AND l.lat IS NOT NULL
    AND l.lon IS NOT NULL
`

export const LIVE_STATS_SQL = `
  WITH latest AS (
    SELECT vehicle_id, route_id, speed_mps, event_ts,
      ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY event_ts DESC) AS rn
    FROM silver_fact_vehicle_positions
    WHERE service_date = TO_DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'America/Phoenix'))
  )
  SELECT
    COUNT(*)                          AS total_vehicles,
    COUNT(DISTINCT route_id)          AS routes_active,
    ROUND(AVG(speed_mps), 2)          AS avg_speed_mps,
    CAST(MAX(event_ts) AS STRING)     AS last_updated
  FROM latest WHERE rn = 1
`
