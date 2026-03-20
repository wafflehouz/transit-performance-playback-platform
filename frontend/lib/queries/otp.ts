// ── OTP Dashboard queries ──────────────────────────────────────────────────────
//
// Primary source: gold_stop_dwell_fact
//   Columns: route_id, direction_id, trip_id, stop_id, stop_sequence,
//            service_date, scheduled_arrival_ts, actual_arrival_ts,
//            arrival_delay_seconds, pickup_type, drop_off_type
//   Filter:  actual_arrival_ts IS NOT NULL (unobserved stops excluded)
//
// OTP window (Swiftly standard):
//   Early   = arrival_delay_seconds < -60  AND stop is a regular pickup stop
//   On-Time = arrival_delay_seconds BETWEEN -60 AND 360
//             OR stop is drop-off only (pickup_type=1) and arrived early
//             (matches Swiftly "Include earlies as on time" for drop-off-only stops)
//   Late    = arrival_delay_seconds > 360
// ─────────────────────────────────────────────────────────────────────────────

// Drop-off-only stops (pickup_type=1): early arrivals reclassified as on-time.
// Requires pickup_type column in gold_stop_dwell_fact (added 2026-03-19).
const EARLY   = `arrival_delay_seconds < -60 AND COALESCE(pickup_type, 0) = 0`
const ON_TIME = `(arrival_delay_seconds BETWEEN -60 AND 360 OR (arrival_delay_seconds < -60 AND COALESCE(pickup_type, 0) = 1))`
const LATE    = `arrival_delay_seconds > 360`

// When timepointOnly=true, restrict to stops flagged as timepoints in GTFS
function timepointWhere(timepointOnly: boolean, alias = 'f'): string {
  if (!timepointOnly) return ''
  return `AND ${alias}.stop_id IN (
    SELECT DISTINCT stop_id FROM silver_fact_stop_schedule WHERE timepoint = 1
  )`
}

function otpCols(alias = 'f') {
  const a = alias
  const early   = `${a}.arrival_delay_seconds < -60 AND COALESCE(${a}.pickup_type, 0) = 0`
  const onTime  = `(${a}.arrival_delay_seconds BETWEEN -60 AND 360 OR (${a}.arrival_delay_seconds < -60 AND COALESCE(${a}.pickup_type, 0) = 1))`
  const late    = `${a}.arrival_delay_seconds > 360`
  return `
    ROUND(AVG(CASE WHEN ${early}  THEN 1.0 ELSE 0.0 END) * 100, 1) AS early_pct,
    ROUND(AVG(CASE WHEN ${onTime} THEN 1.0 ELSE 0.0 END) * 100, 1) AS on_time_pct,
    ROUND(AVG(CASE WHEN ${late}   THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
    ROUND(AVG(${a}.arrival_delay_seconds) / 60.0, 1)                 AS avg_delay_min,
    ROUND(PERCENTILE_APPROX(${a}.arrival_delay_seconds, 0.9) / 60.0, 1) AS p90_delay_min`
}

// ── Static data ───────────────────────────────────────────────────────────────

export const ROUTES_WITH_DATA_SQL = `
  SELECT DISTINCT
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type
  FROM silver_dim_route r
  INNER JOIN gold_stop_dwell_fact f ON r.route_id = f.route_id
  ORDER BY
    CASE WHEN r.route_short_name RLIKE '^[0-9]+$'
         THEN CAST(r.route_short_name AS INT) ELSE 9999 END,
    r.route_short_name
`

export const ROUTE_GROUPS_SQL = `
  SELECT DISTINCT group_name
  FROM gold_route_groups
  ORDER BY group_name
`

// ── Summary (all routes or group) ──────────────────────────────────────────────

export function summaryAllSql(groupName: string | null, timepointOnly: boolean) {
  const groupJoin = groupName
    ? `INNER JOIN gold_route_groups g ON f.route_id = g.route_id AND g.group_name = '${groupName.replace(/'/g, "''")}'`
    : ''
  return `
    SELECT
      COUNT(DISTINCT f.route_id)  AS route_count,
      COUNT(*)                    AS total_stops,
      ${otpCols('f')}
    FROM gold_stop_dwell_fact f
    ${groupJoin}
    WHERE f.service_date >= :startDate
      AND f.service_date <= :endDate
      AND f.actual_arrival_ts IS NOT NULL
    ${timepointWhere(timepointOnly, 'f')}
  `
}

export function otpTrendSql(groupName: string | null, timepointOnly: boolean) {
  const groupJoin = groupName
    ? `INNER JOIN gold_route_groups g ON f.route_id = g.route_id AND g.group_name = '${groupName.replace(/'/g, "''")}'`
    : ''
  return `
    SELECT
      f.service_date,
      ROUND(AVG(CASE WHEN ${EARLY}   THEN 1.0 ELSE 0.0 END) * 100, 1) AS early_pct,
      ROUND(AVG(CASE WHEN ${ON_TIME} THEN 1.0 ELSE 0.0 END) * 100, 1) AS on_time_pct,
      ROUND(AVG(CASE WHEN ${LATE}    THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
      COUNT(*) AS total_stops
    FROM gold_stop_dwell_fact f
    ${groupJoin}
    WHERE f.service_date >= :startDate
      AND f.service_date <= :endDate
      AND f.actual_arrival_ts IS NOT NULL
    ${timepointWhere(timepointOnly, 'f')}
    GROUP BY f.service_date
    ORDER BY f.service_date
  `
}

export function routesTableSql(groupName: string | null, direction: 0 | 1 | 'both', timepointOnly: boolean) {
  const groupJoin = groupName
    ? `INNER JOIN gold_route_groups g ON f.route_id = g.route_id AND g.group_name = '${groupName.replace(/'/g, "''")}'`
    : ''
  const dirFilter = direction === 'both' ? '' : `AND f.direction_id = ${direction}`
  return `
    SELECT
      f.route_id,
      r.route_short_name,
      r.route_long_name,
      r.route_type,
      COUNT(*) AS total_stops,
      ${otpCols('f')}
    FROM gold_stop_dwell_fact f
    JOIN silver_dim_route r ON f.route_id = r.route_id
    ${groupJoin}
    WHERE f.service_date >= :startDate
      AND f.service_date <= :endDate
      AND f.actual_arrival_ts IS NOT NULL
    ${dirFilter}
    ${timepointWhere(timepointOnly, 'f')}
    GROUP BY f.route_id, r.route_short_name, r.route_long_name, r.route_type
    ORDER BY on_time_pct ASC
  `
}

// ── Single route ───────────────────────────────────────────────────────────────

export function singleRouteSummarySql(routeId: string, direction: 0 | 1 | 'both', timepointOnly: boolean) {
  const dirFilter = direction === 'both' ? '' : `AND f.direction_id = ${direction}`
  const id = routeId.replace(/'/g, "''")
  return `
    SELECT
      COUNT(*) AS total_stops,
      ${otpCols('f')}
    FROM gold_stop_dwell_fact f
    WHERE f.service_date >= :startDate
      AND f.service_date <= :endDate
      AND f.route_id = '${id}'
      AND f.actual_arrival_ts IS NOT NULL
    ${dirFilter}
    ${timepointWhere(timepointOnly, 'f')}
  `
}

// Headsign per direction for a single route
export function routeHeadsignSql(routeId: string) {
  const id = routeId.replace(/'/g, "''")
  return `
    SELECT direction_id, trip_headsign
    FROM silver_dim_trip
    WHERE route_id = '${id}'
      AND trip_headsign IS NOT NULL
    GROUP BY direction_id, trip_headsign
    ORDER BY direction_id
  `
}

// Time-of-day: 15-min buckets in Phoenix local time (UTC-7, no DST)
export function timeOfDaySql(routeId: string, direction: 0 | 1 | 'both', timepointOnly: boolean) {
  const dirFilter = direction === 'both' ? '' : `AND f.direction_id = ${direction}`
  const id = routeId.replace(/'/g, "''")
  return `
    SELECT
      DATE_FORMAT(
        TIMESTAMPADD(MINUTE,
          FLOOR(MINUTE(TIMESTAMPADD(HOUR, -7, f.actual_arrival_ts)) / 15) * 15
            - MINUTE(TIMESTAMPADD(HOUR, -7, f.actual_arrival_ts)),
          TIMESTAMPADD(HOUR, -7, f.actual_arrival_ts)),
        'HH:mm')                                                       AS time_bucket,
      SUM(CASE WHEN ${EARLY}   THEN 1 ELSE 0 END)                      AS early_count,
      SUM(CASE WHEN ${ON_TIME} THEN 1 ELSE 0 END)                      AS on_time_count,
      SUM(CASE WHEN ${LATE}    THEN 1 ELSE 0 END)                      AS late_count,
      COUNT(*)                                                          AS total_count
    FROM gold_stop_dwell_fact f
    WHERE f.service_date >= :startDate
      AND f.service_date <= :endDate
      AND f.route_id = '${id}'
      AND f.actual_arrival_ts IS NOT NULL
    ${dirFilter}
    ${timepointWhere(timepointOnly)}
    GROUP BY time_bucket
    ORDER BY time_bucket
  `
}

// Stop-level OTP sorted by stop sequence, with timepoint flag for display
export function stopOtpSql(routeId: string, direction: 0 | 1 | 'both', timepointOnly: boolean) {
  const dirFilter = direction === 'both' ? '' : `AND f.direction_id = ${direction}`
  const id = routeId.replace(/'/g, "''")
  const tpFilter = timepointOnly
    ? 'HAVING observations >= 3 AND COALESCE(tp.timepoint, 0) = 1'
    : 'HAVING observations >= 3'
  return `
    WITH tp AS (
      SELECT stop_id, MAX(timepoint) AS timepoint
      FROM silver_fact_stop_schedule
      GROUP BY stop_id
    )
    SELECT
      f.direction_id,
      f.stop_id,
      COALESCE(s.stop_name, f.stop_id)                                  AS stop_name,
      COALESCE(tp.timepoint, 0)                                          AS timepoint,
      MIN(f.stop_sequence)                                               AS stop_order,
      COUNT(*)                                                           AS observations,
      ROUND(AVG(CASE WHEN ${EARLY}   THEN 1.0 ELSE 0.0 END) * 100, 1)  AS early_pct,
      ROUND(AVG(CASE WHEN ${ON_TIME} THEN 1.0 ELSE 0.0 END) * 100, 1)  AS on_time_pct,
      ROUND(AVG(CASE WHEN ${LATE}    THEN 1.0 ELSE 0.0 END) * 100, 1)  AS late_pct,
      ROUND(AVG(f.arrival_delay_seconds) / 60.0, 1)                     AS avg_delay_min,
      ROUND(PERCENTILE_APPROX(f.arrival_delay_seconds, 0.9) / 60.0, 1) AS p90_delay_min
    FROM gold_stop_dwell_fact f
    LEFT JOIN silver_dim_stop s ON f.stop_id = s.stop_id
    LEFT JOIN tp ON f.stop_id = tp.stop_id
    WHERE f.service_date >= :startDate
      AND f.service_date <= :endDate
      AND f.route_id = '${id}'
      AND f.actual_arrival_ts IS NOT NULL
    ${dirFilter}
    GROUP BY f.direction_id, f.stop_id, s.stop_name, tp.timepoint
    ${tpFilter}
    ORDER BY f.direction_id, stop_order
  `
}

// Delay histogram: 1-min bins, capped at -20min to +60min
export function delayHistogramSql(routeId: string, direction: 0 | 1 | 'both', timepointOnly: boolean) {
  const dirFilter = direction === 'both' ? '' : `AND f.direction_id = ${direction}`
  const id = routeId.replace(/'/g, "''")
  return `
    SELECT
      ROUND(f.arrival_delay_seconds / 60.0) AS delay_minute,
      CASE
        WHEN ${EARLY}   THEN 'Early'
        WHEN ${ON_TIME} THEN 'On-Time'
        ELSE 'Late'
      END                                    AS otp_category,
      COUNT(*)                               AS stop_count
    FROM gold_stop_dwell_fact f
    WHERE f.service_date >= :startDate
      AND f.service_date <= :endDate
      AND f.route_id = '${id}'
      AND f.actual_arrival_ts IS NOT NULL
      AND f.arrival_delay_seconds BETWEEN -1200 AND 3600
    ${dirFilter}
    ${timepointWhere(timepointOnly)}
    GROUP BY delay_minute, otp_category
    ORDER BY delay_minute
  `
}

export function singleRouteTrendSql(routeId: string, direction: 0 | 1 | 'both', timepointOnly: boolean) {
  const dirFilter = direction === 'both' ? '' : `AND f.direction_id = ${direction}`
  const id = routeId.replace(/'/g, "''")
  return `
    SELECT
      f.service_date,
      ROUND(AVG(CASE WHEN ${EARLY}   THEN 1.0 ELSE 0.0 END) * 100, 1) AS early_pct,
      ROUND(AVG(CASE WHEN ${ON_TIME} THEN 1.0 ELSE 0.0 END) * 100, 1) AS on_time_pct,
      ROUND(AVG(CASE WHEN ${LATE}    THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
      COUNT(*) AS total_stops
    FROM gold_stop_dwell_fact f
    WHERE f.service_date >= :startDate
      AND f.service_date <= :endDate
      AND f.route_id = '${id}'
      AND f.actual_arrival_ts IS NOT NULL
    ${dirFilter}
    ${timepointWhere(timepointOnly)}
    GROUP BY f.service_date
    ORDER BY f.service_date
  `
}

// ── Schedule pivot (single day, trip × stop heatmap) ──────────────────────────
// Returns one row per (trip, stop) observed on serviceDate.
// canonical_seq anchors stop order to the most-observed trip for the direction.
export function schedulePivotSql(routeId: string, direction: 0 | 1 | 'both', timepointOnly: boolean) {
  const id = routeId.replace(/'/g, "''")
  const dirFilter    = direction === 'both' ? '' : `AND f.direction_id = ${direction}`
  const dirFilterRef = direction === 'both' ? '' : `AND direction_id = ${direction}`
  const tpFilter     = timepointOnly
    ? `AND f.stop_id IN (SELECT DISTINCT stop_id FROM silver_fact_stop_schedule WHERE timepoint = 1)`
    : ''
  return `
    WITH ref_trip AS (
      SELECT direction_id, trip_id FROM (
        SELECT direction_id, trip_id, COUNT(*) AS cnt,
          ROW_NUMBER() OVER (PARTITION BY direction_id ORDER BY COUNT(*) DESC) AS rn
        FROM gold_stop_dwell_fact
        WHERE service_date = :serviceDate
          AND route_id = '${id}'
          ${dirFilterRef}
        GROUP BY direction_id, trip_id
      ) t WHERE rn = 1
    ),
    canonical_seq AS (
      SELECT f.direction_id, f.stop_id, MIN(f.stop_sequence) AS seq
      FROM gold_stop_dwell_fact f
      INNER JOIN ref_trip rt ON f.trip_id = rt.trip_id AND f.direction_id = rt.direction_id
      WHERE f.service_date = :serviceDate
      GROUP BY f.direction_id, f.stop_id
    )
    SELECT
      f.direction_id,
      f.trip_id,
      f.stop_id,
      COALESCE(s.stop_name, f.stop_id)      AS stop_name,
      COALESCE(cs.seq, f.stop_sequence)      AS canonical_seq,
      f.scheduled_arrival_ts,
      f.actual_arrival_ts,
      f.arrival_delay_seconds
    FROM gold_stop_dwell_fact f
    LEFT JOIN silver_dim_stop s ON f.stop_id = s.stop_id
    LEFT JOIN canonical_seq cs
      ON f.stop_id = cs.stop_id AND f.direction_id = cs.direction_id
    WHERE f.service_date = :serviceDate
      AND f.route_id = '${id}'
      AND f.actual_arrival_ts IS NOT NULL
    ${dirFilter}
    ${tpFilter}
    ORDER BY f.direction_id, f.trip_id, canonical_seq
  `
}
