import type { RouteMetrics15Min } from '@/types'

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
