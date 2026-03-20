import { queryDatabricks } from '@/lib/databricks'
import type { RouteMetrics15Min, DimRoute } from '@/types'

export async function getRoutes(): Promise<DimRoute[]> {
  const result = await queryDatabricks<DimRoute>(`
    SELECT DISTINCT
      r.route_id,
      r.route_short_name,
      r.route_long_name,
      r.route_type,
      r.route_color
    FROM silver_dim_route r
    INNER JOIN gold_route_metrics_15min m ON r.route_id = m.route_id
    ORDER BY
      CASE WHEN r.route_short_name RLIKE '^[0-9]+$' THEN CAST(r.route_short_name AS INT) ELSE 9999 END,
      r.route_short_name
  `)
  return result.rows
}

export async function getRouteMetrics(
  serviceDate: string,
  routeIds?: string[]
): Promise<RouteMetrics15Min[]> {
  const routeFilter =
    routeIds && routeIds.length > 0
      ? `AND route_id IN (${routeIds.map((r) => `'${r.replace(/'/g, "''")}'`).join(', ')})`
      : ''

  const result = await queryDatabricks<RouteMetrics15Min>(`
    SELECT
      service_date,
      route_id,
      direction_id,
      time_bucket_15min,
      trip_count,
      COALESCE(avg_delay_seconds, 0)      AS avg_delay_seconds,
      COALESCE(p90_delay_seconds, 0)      AS p90_delay_seconds,
      COALESCE(pct_on_time, 1.0)          AS pct_on_time,
      avg_dwell_delta_seconds,
      avg_late_start_seconds
    FROM gold_route_metrics_15min
    WHERE service_date = :serviceDate
    ${routeFilter}
    ORDER BY route_id, direction_id, time_bucket_15min
  `, { serviceDate })

  return result.rows
}

/** Build the grid data structure: route+direction → bucket → metrics */
export type GridKey = `${string}|${number}`
export type BucketMap = Map<string, RouteMetrics15Min>
export type GridData = Map<GridKey, BucketMap>

export function buildGridData(rows: RouteMetrics15Min[]): {
  grid: GridData
  buckets: string[]
  routeDirections: GridKey[]
} {
  const grid: GridData = new Map()
  const bucketSet = new Set<string>()
  const rdSet = new Set<GridKey>()

  for (const row of rows) {
    const key: GridKey = `${row.route_id}|${row.direction_id}`
    const bucket = row.time_bucket_15min

    rdSet.add(key)
    bucketSet.add(bucket)

    if (!grid.has(key)) grid.set(key, new Map())
    grid.get(key)!.set(bucket, row)
  }

  // Compute worst-bucket avg delay per route for sorting
  const routeWorstDelay = new Map<string, number>()
  for (const [key, bucketMap] of grid) {
    const [routeId] = key.split('|')
    let worst = routeWorstDelay.get(routeId) ?? 0
    for (const m of bucketMap.values()) {
      if (m.avg_delay_seconds > worst) worst = m.avg_delay_seconds
    }
    routeWorstDelay.set(routeId, worst)
  }

  const buckets = Array.from(bucketSet).sort()
  const routeDirections = Array.from(rdSet).sort((a, b) => {
    const [ra] = a.split('|')
    const [rb] = b.split('|')
    // Primary: worst delay descending (most-delayed routes first)
    const delayDiff = (routeWorstDelay.get(rb) ?? 0) - (routeWorstDelay.get(ra) ?? 0)
    if (delayDiff !== 0) return delayDiff
    // Secondary: route number ascending
    const na = parseInt(ra) || 9999
    const nb = parseInt(rb) || 9999
    return na !== nb ? na - nb : a.localeCompare(b)
  })

  return { grid, buckets, routeDirections }
}
