// ── Dwell Analysis queries ─────────────────────────────────────────────────────
//
// Two sources:
//   gold_stop_dwell_fact (f)     — GTFS-RT scheduled vs actual dwell
//     key cols: route_id, direction_id, trip_id, stop_id, stop_sequence,
//               service_date, scheduled_dwell_seconds, actual_dwell_seconds, dwell_delta_seconds
//
//   gold_stop_dwell_inferred (i) — VP proximity dwell (haversine ≤40m, speed ≤1 m/s)
//     key cols: route_id, direction_id, trip_id, stop_id, stop_sequence,
//               service_date, dwell_seconds, inferred_arrival_ts
//
// Terminal exclusion: strips first + last stop_sequence per trip (layover suppression)
// Benchmarks (TCQSM/Valley Metro): fast=15s, normal=30s, high-pax=60s, extreme=120s
// ─────────────────────────────────────────────────────────────────────────────

function esc(s: string) { return s.replace(/'/g, "''") }

// Returns terminal exclusion CTE fragments.
// All fact/inferred queries use :startDate/:endDate params so the CTE scope matches.
function terminals(table: string, alias: string, exclude: boolean) {
  if (!exclude) return { cte: '', join: '', where: '' }
  return {
    cte: `WITH trip_bounds AS (
  SELECT trip_id, MIN(stop_sequence) AS first_seq, MAX(stop_sequence) AS last_seq
  FROM ${table}
  WHERE service_date >= :startDate AND service_date <= :endDate
  GROUP BY trip_id
)`,
    join: `INNER JOIN trip_bounds tb ON ${alias}.trip_id = tb.trip_id`,
    where: `AND ${alias}.stop_sequence != tb.first_seq AND ${alias}.stop_sequence != tb.last_seq`,
  }
}

function groupJoin(groupName: string | null, alias = 'f') {
  if (!groupName) return ''
  return `INNER JOIN gold_route_groups g ON ${alias}.route_id = g.route_id AND g.group_name = '${esc(groupName)}'`
}

function dirWhere(direction: 0 | 1 | 'both', alias = 'f') {
  return direction === 'both' ? '' : `AND ${alias}.direction_id = ${direction}`
}

function timepointWhere(timepointOnly: boolean, alias = 'f') {
  return timepointOnly
    ? `AND ${alias}.stop_id IN (SELECT DISTINCT stop_id FROM silver_fact_stop_schedule WHERE timepoint = 1)`
    : ''
}

// ── Static data ───────────────────────────────────────────────────────────────

export const DWELL_ROUTES_SQL = `
  SELECT DISTINCT r.route_id, r.route_short_name, r.route_long_name, r.route_type
  FROM silver_dim_route r
  INNER JOIN gold_stop_dwell_fact f ON r.route_id = f.route_id
  ORDER BY
    CASE WHEN r.route_short_name RLIKE '^[0-9]+$' THEN CAST(r.route_short_name AS INT) ELSE 9999 END,
    r.route_short_name
`

export const DWELL_GROUPS_SQL = `
  SELECT DISTINCT group_name FROM gold_route_groups ORDER BY group_name
`

// ── Summary KPIs (fact table) ─────────────────────────────────────────────────

export function dwellSummarySql(
  groupName: string | null,
  routeId: string | null,
  direction: 0 | 1 | 'both',
  timepointOnly: boolean,
  excludeTerminals: boolean,
): string {
  const t = terminals('gold_stop_dwell_fact', 'f', excludeTerminals)
  const routeFilter = routeId ? `AND f.route_id = '${esc(routeId)}'` : ''
  const gj = routeId ? '' : groupJoin(groupName)
  return `
    ${t.cte}
    SELECT
      COUNT(DISTINCT f.route_id)                                          AS route_count,
      COUNT(*)                                                            AS observation_count,
      ROUND(AVG(f.actual_dwell_seconds), 0)                              AS avg_actual_sec,
      ROUND(AVG(f.dwell_delta_seconds), 0)                               AS avg_delta_sec,
      ROUND(PERCENTILE_APPROX(f.actual_dwell_seconds, 0.5), 0)           AS p50_actual_sec,
      ROUND(PERCENTILE_APPROX(f.actual_dwell_seconds, 0.9), 0)           AS p90_actual_sec,
      SUM(CASE WHEN f.actual_dwell_seconds > 120 THEN 1 ELSE 0 END)     AS stops_over_2min
    FROM gold_stop_dwell_fact f
    ${t.join}
    ${gj}
    WHERE f.service_date >= :startDate AND f.service_date <= :endDate
      AND f.actual_dwell_seconds IS NOT NULL
    ${routeFilter}
    ${dirWhere(direction)}
    ${timepointWhere(timepointOnly)}
    ${t.where}
  `
}

// ── Dwell distribution buckets (fact table) ───────────────────────────────────

export function dwellBucketSql(
  groupName: string | null,
  routeId: string | null,
  direction: 0 | 1 | 'both',
  timepointOnly: boolean,
  excludeTerminals: boolean,
): string {
  const t = terminals('gold_stop_dwell_fact', 'f', excludeTerminals)
  const routeFilter = routeId ? `AND f.route_id = '${esc(routeId)}'` : ''
  const gj = routeId ? '' : groupJoin(groupName)
  return `
    ${t.cte}
    SELECT
      CASE
        WHEN f.actual_dwell_seconds < 15  THEN 'Fast (<15s)'
        WHEN f.actual_dwell_seconds < 30  THEN 'Normal (15-30s)'
        WHEN f.actual_dwell_seconds < 60  THEN 'Slow (30-60s)'
        WHEN f.actual_dwell_seconds < 120 THEN 'High Pax (60-120s)'
        ELSE 'Outlier (120s+)'
      END                 AS dwell_bucket,
      COUNT(*)            AS stop_events,
      ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
    FROM gold_stop_dwell_fact f
    ${t.join}
    ${gj}
    WHERE f.service_date >= :startDate AND f.service_date <= :endDate
      AND f.actual_dwell_seconds IS NOT NULL
    ${routeFilter}
    ${dirWhere(direction)}
    ${timepointWhere(timepointOnly)}
    ${t.where}
    GROUP BY dwell_bucket
    ORDER BY dwell_bucket
  `
}

// ── Top N stops by dwell inflation (fact table) ───────────────────────────────

export function topInflationStopsSql(
  groupName: string | null,
  routeId: string | null,
  direction: 0 | 1 | 'both',
  timepointOnly: boolean,
  excludeTerminals: boolean,
  limit = 15,
): string {
  const t = terminals('gold_stop_dwell_fact', 'f', excludeTerminals)
  const routeFilter = routeId ? `AND f.route_id = '${esc(routeId)}'` : ''
  const gj = routeId ? '' : groupJoin(groupName)
  return `
    ${t.cte}
    SELECT
      f.stop_id,
      COALESCE(s.stop_name, f.stop_id)                                   AS stop_name,
      f.route_id,
      COUNT(*)                                                            AS observations,
      ROUND(AVG(f.actual_dwell_seconds), 0)                              AS avg_actual_sec,
      ROUND(AVG(f.dwell_delta_seconds), 0)                               AS avg_delta_sec,
      ROUND(PERCENTILE_APPROX(f.actual_dwell_seconds, 0.9), 0)           AS p90_actual_sec
    FROM gold_stop_dwell_fact f
    LEFT JOIN silver_dim_stop s ON f.stop_id = s.stop_id
    ${t.join}
    ${gj}
    WHERE f.service_date >= :startDate AND f.service_date <= :endDate
      AND f.actual_dwell_seconds IS NOT NULL
      AND f.dwell_delta_seconds IS NOT NULL
    ${routeFilter}
    ${dirWhere(direction)}
    ${timepointWhere(timepointOnly)}
    ${t.where}
    GROUP BY f.stop_id, COALESCE(s.stop_name, f.stop_id), f.route_id
    HAVING observations >= 3
    ORDER BY avg_delta_sec DESC
    LIMIT ${limit}
  `
}

// ── By-route aggregate table (fact table) ─────────────────────────────────────

export function dwellByRouteSql(
  groupName: string | null,
  direction: 0 | 1 | 'both',
  timepointOnly: boolean,
  excludeTerminals: boolean,
): string {
  const t = terminals('gold_stop_dwell_fact', 'f', excludeTerminals)
  const gj = groupJoin(groupName)
  return `
    ${t.cte}
    SELECT
      f.route_id,
      r.route_short_name,
      r.route_long_name,
      COUNT(*)                                                            AS total_stops,
      ROUND(AVG(f.actual_dwell_seconds), 0)                              AS avg_actual_sec,
      ROUND(AVG(f.dwell_delta_seconds), 0)                               AS avg_delta_sec,
      ROUND(PERCENTILE_APPROX(f.actual_dwell_seconds, 0.9), 0)           AS p90_actual_sec,
      SUM(CASE WHEN f.actual_dwell_seconds > 120 THEN 1 ELSE 0 END)     AS stops_over_2min
    FROM gold_stop_dwell_fact f
    JOIN silver_dim_route r ON f.route_id = r.route_id
    ${t.join}
    ${gj}
    WHERE f.service_date >= :startDate AND f.service_date <= :endDate
      AND f.actual_dwell_seconds IS NOT NULL
    ${dirWhere(direction)}
    ${timepointWhere(timepointOnly)}
    ${t.where}
    GROUP BY f.route_id, r.route_short_name, r.route_long_name
    ORDER BY avg_delta_sec DESC
  `
}

// ── Stop profile (inferred, single route) ─────────────────────────────────────
// Returns one row per (direction, stop) with avg/p50/p90 dwell, ordered by stop_sequence.

export function stopProfileSql(
  routeId: string,
  direction: 0 | 1 | 'both',
  excludeTerminals: boolean,
): string {
  const id = esc(routeId)
  const dirFilter = dirWhere(direction, 'i')

  const ctes: string[] = []
  if (excludeTerminals) {
    ctes.push(`trip_bounds AS (
    SELECT trip_id, MIN(stop_sequence) AS first_seq, MAX(stop_sequence) AS last_seq
    FROM gold_stop_dwell_inferred
    WHERE service_date >= :startDate AND service_date <= :endDate
    GROUP BY trip_id
  )`)
  }
  ctes.push(`headsign AS (
    SELECT direction_id, trip_headsign,
      ROW_NUMBER() OVER (PARTITION BY direction_id ORDER BY COUNT(*) DESC) AS rn
    FROM silver_dim_trip
    WHERE route_id = '${id}'
    GROUP BY direction_id, trip_headsign
  )`)

  return `
    WITH ${ctes.join(',\n')}
    SELECT
      i.direction_id,
      COALESCE(h.trip_headsign, CONCAT('Dir ', CAST(i.direction_id AS STRING))) AS direction_label,
      i.stop_id,
      COALESCE(s.stop_name, i.stop_id)                                   AS stop_name,
      MIN(i.stop_sequence)                                                AS stop_sequence,
      COUNT(*)                                                            AS observations,
      ROUND(AVG(i.dwell_seconds), 0)                                     AS avg_dwell_sec,
      ROUND(PERCENTILE_APPROX(i.dwell_seconds, 0.5), 0)                  AS p50_dwell_sec,
      ROUND(PERCENTILE_APPROX(i.dwell_seconds, 0.9), 0)                  AS p90_dwell_sec
    FROM gold_stop_dwell_inferred i
    ${excludeTerminals ? 'INNER JOIN trip_bounds tb ON i.trip_id = tb.trip_id' : ''}
    LEFT JOIN silver_dim_stop s ON i.stop_id = s.stop_id
    LEFT JOIN headsign h ON i.direction_id = h.direction_id AND h.rn = 1
    WHERE i.service_date >= :startDate AND i.service_date <= :endDate
      AND i.route_id = '${id}'
    ${dirFilter}
    ${excludeTerminals ? 'AND i.stop_sequence != tb.first_seq AND i.stop_sequence != tb.last_seq' : ''}
    GROUP BY i.direction_id,
      COALESCE(h.trip_headsign, CONCAT('Dir ', CAST(i.direction_id AS STRING))),
      i.stop_id, COALESCE(s.stop_name, i.stop_id)
    HAVING observations >= 3
    ORDER BY i.direction_id, MIN(i.stop_sequence)
  `
}

// ── Trip × Stop matrix (inferred, single route, single date = :endDate) ───────
// Raw rows: (trip_id, direction_id, stop_sequence, stop_name, dwell_seconds)
// Caller pivots client-side. Capped at :endDate only (one service day).

export function tripMatrixSql(
  routeId: string,
  direction: 0 | 1 | 'both',
  excludeTerminals: boolean,
): string {
  const id = esc(routeId)
  const dirFilter = direction === 'both' ? '' : `AND i.direction_id = ${direction}`

  const ctes: string[] = []
  if (excludeTerminals) {
    ctes.push(`trip_bounds AS (
    SELECT trip_id, MIN(stop_sequence) AS first_seq, MAX(stop_sequence) AS last_seq
    FROM gold_stop_dwell_inferred
    WHERE service_date = :endDate
    GROUP BY trip_id
  )`)
  }
  ctes.push(`headsign AS (
    SELECT direction_id, trip_headsign,
      ROW_NUMBER() OVER (PARTITION BY direction_id ORDER BY COUNT(*) DESC) AS rn
    FROM silver_dim_trip
    WHERE route_id = '${id}'
    GROUP BY direction_id, trip_headsign
  )`)

  return `
    WITH ${ctes.join(',\n')}
    SELECT
      i.trip_id,
      i.direction_id,
      COALESCE(h.trip_headsign, CONCAT('Dir ', CAST(i.direction_id AS STRING))) AS direction_label,
      i.stop_sequence,
      i.stop_id,
      COALESCE(s.stop_name, i.stop_id)                                   AS stop_name,
      i.dwell_seconds
    FROM gold_stop_dwell_inferred i
    ${excludeTerminals ? 'INNER JOIN trip_bounds tb ON i.trip_id = tb.trip_id' : ''}
    LEFT JOIN silver_dim_stop s ON i.stop_id = s.stop_id
    LEFT JOIN headsign h ON i.direction_id = h.direction_id AND h.rn = 1
    WHERE i.service_date = :endDate
      AND i.route_id = '${id}'
    ${dirFilter}
    ${excludeTerminals ? 'AND i.stop_sequence != tb.first_seq AND i.stop_sequence != tb.last_seq' : ''}
    ORDER BY i.direction_id, i.trip_id, i.stop_sequence
  `
}

// ── Time of Day (inferred, hourly buckets, Phoenix local UTC-7) ───────────────

export function dwellTodSql(
  groupName: string | null,
  routeId: string | null,
  direction: 0 | 1 | 'both',
  excludeTerminals: boolean,
): string {
  const t = terminals('gold_stop_dwell_inferred', 'i', excludeTerminals)
  const routeFilter = routeId ? `AND i.route_id = '${esc(routeId)}'` : ''
  const gj = routeId ? '' : groupJoin(groupName, 'i')
  return `
    ${t.cte}
    SELECT
      MOD(HOUR(TIMESTAMPADD(HOUR, -7, i.inferred_arrival_ts)) + 24, 24) AS phoenix_hour,
      ROUND(AVG(i.dwell_seconds), 0)                                     AS avg_dwell_sec,
      ROUND(PERCENTILE_APPROX(i.dwell_seconds, 0.9), 0)                  AS p90_dwell_sec,
      COUNT(*)                                                            AS observations
    FROM gold_stop_dwell_inferred i
    ${t.join}
    ${gj}
    WHERE i.service_date >= :startDate AND i.service_date <= :endDate
    ${routeFilter}
    ${dirWhere(direction, 'i')}
    ${t.where}
    GROUP BY phoenix_hour
    HAVING observations >= 5
    ORDER BY phoenix_hour
  `
}

// ── 30-day trend (inferred, daily, anchored at :endDate) ─────────────────────
// Always shows 30 days ending at endDate regardless of startDate filter.

export function dwellTrendSql(
  groupName: string | null,
  routeId: string | null,
  excludeTerminals: boolean,
): string {
  const t = terminals('gold_stop_dwell_inferred', 'i', excludeTerminals)
  const routeFilter = routeId ? `AND i.route_id = '${esc(routeId)}'` : ''
  const gj = routeId ? '' : groupJoin(groupName, 'i')
  return `
    ${t.cte}
    SELECT
      i.service_date,
      ROUND(AVG(i.dwell_seconds), 0)                                     AS avg_dwell_sec,
      ROUND(PERCENTILE_APPROX(i.dwell_seconds, 0.5), 0)                  AS p50_dwell_sec,
      ROUND(PERCENTILE_APPROX(i.dwell_seconds, 0.9), 0)                  AS p90_dwell_sec,
      COUNT(*)                                                            AS events
    FROM gold_stop_dwell_inferred i
    ${t.join}
    ${gj}
    WHERE i.service_date >= DATE_ADD(:endDate, -29) AND i.service_date <= :endDate
    ${routeFilter}
    ${t.where}
    GROUP BY i.service_date
    ORDER BY i.service_date
  `
}
