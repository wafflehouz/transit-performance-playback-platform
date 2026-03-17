/**
 * GTFS-RT Vehicle Positions cache.
 *
 * Polls Mecatran every POLL_INTERVAL_MS. All route queries are served
 * from the in-memory snapshot — Mecatran is never hit per-request.
 */

import fetch from 'node-fetch'

const POLL_INTERVAL_MS = 30_000 // 30 seconds
const VP_URL = 'https://mna.mecatran.com/utw/ws/gtfsfeed/vehicles/valleymetro'

export interface VehiclePosition {
  vehicle_id: string
  trip_id: string | null
  route_id: string | null
  direction_id: number | null
  lat: number
  lon: number
  bearing: number | null
  speed_mps: number | null
  timestamp: number | null
  fetched_at: string
}

interface CacheState {
  vehicles: VehiclePosition[]
  fetched_at: string | null
  feed_ts: string | null
  error: string | null
}

const state: CacheState = {
  vehicles: [],
  fetched_at: null,
  feed_ts: null,
  error: null,
}

// ── GTFS-RT JSON parsing ──────────────────────────────────────────────────────

interface GtfsRtFeed {
  header?: { timestamp?: string | number }
  entity?: GtfsRtEntity[]
}

interface GtfsRtEntity {
  id?: string
  vehicle?: {
    vehicle?: { id?: string; label?: string }
    trip?: { tripId?: string; routeId?: string; directionId?: number }
    position?: {
      latitude?: number
      longitude?: number
      bearing?: number
      speed?: number
    }
    timestamp?: string | number
  }
}

function parseFeed(data: GtfsRtFeed): VehiclePosition[] {
  const entities = data.entity ?? []
  const now = new Date().toISOString()
  const vehicles: VehiclePosition[] = []

  for (const entity of entities) {
    const veh = entity.vehicle
    if (!veh?.trip) continue // skip position-only (no trip context)

    const pos = veh.position
    if (!pos?.latitude || !pos?.longitude) continue

    vehicles.push({
      vehicle_id: veh.vehicle?.id ?? entity.id ?? 'unknown',
      trip_id: veh.trip.tripId ?? null,
      route_id: veh.trip.routeId ?? null,
      direction_id: veh.trip.directionId ?? null,
      lat: pos.latitude,
      lon: pos.longitude,
      bearing: pos.bearing ?? null,
      speed_mps: pos.speed ?? null,
      timestamp: veh.timestamp ? Number(veh.timestamp) : null,
      fetched_at: now,
    })
  }

  return vehicles
}

// ── Poller ────────────────────────────────────────────────────────────────────

async function poll(): Promise<void> {
  const apiKey = process.env.MECATRAN_API_KEY
  if (!apiKey) {
    state.error = 'MECATRAN_API_KEY not set'
    return
  }

  try {
    const url = new URL(VP_URL)
    url.searchParams.set('apiKey', apiKey)
    url.searchParams.set('asJson', 'true')

    const res = await fetch(url.toString(), {
      headers: { 'User-Agent': 'TransitPlatform/1.0' },
      signal: AbortSignal.timeout(15_000),
    })

    if (!res.ok) throw new Error(`HTTP ${res.status}`)

    const data = (await res.json()) as GtfsRtFeed
    const vehicles = parseFeed(data)
    const feedTs = data.header?.timestamp ? String(data.header.timestamp) : null

    state.vehicles = vehicles
    state.fetched_at = new Date().toISOString()
    state.feed_ts = feedTs
    state.error = null

    console.log(`[poll] ${vehicles.length} vehicles cached at ${state.fetched_at}`)
  } catch (err) {
    state.error = err instanceof Error ? err.message : 'Unknown poll error'
    console.error(`[poll] Error: ${state.error}`)
  }
}

export function startPoller(): void {
  // Immediate first fetch
  poll()
  setInterval(poll, POLL_INTERVAL_MS)
  console.log(`[poller] Started — refreshing every ${POLL_INTERVAL_MS / 1000}s`)
}

// ── Public accessors ──────────────────────────────────────────────────────────

export function getVehiclesForRoute(routeId: string): VehiclePosition[] {
  return state.vehicles.filter((v) => v.route_id === routeId)
}

export function getCacheStatus(): {
  vehicle_count: number
  fetched_at: string | null
  feed_ts: string | null
  error: string | null
} {
  return {
    vehicle_count: state.vehicles.length,
    fetched_at: state.fetched_at,
    feed_ts: state.feed_ts,
    error: state.error,
  }
}

export function getActiveRouteIds(): string[] {
  const ids = new Set(state.vehicles.map((v) => v.route_id).filter(Boolean) as string[])
  return Array.from(ids).sort()
}
