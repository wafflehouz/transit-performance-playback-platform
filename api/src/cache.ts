/**
 * GTFS-RT cache — Vehicle Positions + TripUpdates polled together every 30s.
 * VP and TU are joined on trip_id to produce OTP status per vehicle.
 */

import fetch from 'node-fetch'

const POLL_INTERVAL_MS = 30_000
const VP_URL  = 'https://mna.mecatran.com/utw/ws/gtfsfeed/vehicles/valleymetro'
const TU_URL  = 'https://mna.mecatran.com/utw/ws/gtfsfeed/realtime/valleymetro'

export type OtpStatus = 'early' | 'on_time' | 'late' | 'very_late' | 'unknown'

export interface VehiclePosition {
  vehicle_id: string
  headsign: string | null       // vehicle.vehicle.label from Mecatran feed
  trip_id: string | null
  route_id: string | null
  direction_id: number | null
  start_time: string | null     // GTFS-RT TripDescriptor.start_time — "HH:MM:SS" local
  lat: number
  lon: number
  bearing: number | null
  speed_mps: number | null
  timestamp: number | null
  fetched_at: string
  delay_seconds: number | null
  otp_status: OtpStatus
}

interface CacheState {
  vehicles: VehiclePosition[]
  fetched_at: string | null
  fetched_at_ms: number | null   // epoch ms — used by clients for dead reckoning
  feed_ts: string | null
  error: string | null
}

const state: CacheState = {
  vehicles: [],
  fetched_at: null,
  fetched_at_ms: null,
  feed_ts: null,
  error: null,
}

// ── OTP classification ─────────────────────────────────────────────────────────

function classifyOtp(delaySeconds: number | null): OtpStatus {
  if (delaySeconds === null) return 'unknown'
  if (delaySeconds < -60)  return 'early'
  if (delaySeconds <= 299) return 'on_time'
  if (delaySeconds <= 599) return 'late'
  return 'very_late'
}

// ── GTFS-RT type stubs ─────────────────────────────────────────────────────────

interface GtfsRtFeed {
  header?: { timestamp?: string | number }
  entity?: GtfsRtEntity[]
}

interface GtfsRtEntity {
  id?: string
  vehicle?: {
    vehicle?: { id?: string; label?: string }
    trip?: { tripId?: string; routeId?: string; directionId?: number; startTime?: string }
    position?: { latitude?: number; longitude?: number; bearing?: number; speed?: number }
    timestamp?: string | number
  }
  tripUpdate?: {
    trip?: { tripId?: string; routeId?: string }
    stopTimeUpdate?: Array<{
      arrival?: { delay?: number }
      departure?: { delay?: number }
    }>
  }
}

// ── Parsers ────────────────────────────────────────────────────────────────────

function parseVP(data: GtfsRtFeed): Map<string, Omit<VehiclePosition, 'delay_seconds' | 'otp_status'>> {
  const now = new Date().toISOString()
  const map = new Map<string, Omit<VehiclePosition, 'delay_seconds' | 'otp_status'>>()

  for (const entity of data.entity ?? []) {
    const veh = entity.vehicle
    if (!veh?.trip || !veh.position?.latitude || !veh.position?.longitude) continue

    const vehicleId = veh.vehicle?.id ?? entity.id ?? 'unknown'
    map.set(veh.trip.tripId ?? vehicleId, {
      vehicle_id: vehicleId,
      headsign: veh.vehicle?.label ?? null,
      trip_id: veh.trip.tripId ?? null,
      route_id: veh.trip.routeId ?? null,
      direction_id: veh.trip.directionId ?? null,
      start_time: veh.trip.startTime ?? null,
      lat: veh.position.latitude,
      lon: veh.position.longitude,
      bearing: veh.position.bearing ?? null,
      speed_mps: veh.position.speed ?? null,
      timestamp: veh.timestamp ? Number(veh.timestamp) : null,
      fetched_at: now,
    })
  }
  return map
}

/** Returns trip_id → latest delay_seconds from most recent stop time update */
function parseTU(data: GtfsRtFeed): Map<string, number> {
  const delays = new Map<string, number>()

  for (const entity of data.entity ?? []) {
    const tu = entity.tripUpdate
    if (!tu?.trip?.tripId) continue

    const updates = tu.stopTimeUpdate ?? []
    // First future stop update is most representative of current adherence
    const update = updates[0]
    if (!update) continue

    const delay = update.arrival?.delay ?? update.departure?.delay
    if (delay !== undefined) {
      delays.set(tu.trip.tripId, delay)
    }
  }
  return delays
}

// ── Fetch helpers ──────────────────────────────────────────────────────────────

async function fetchFeed(url: string, apiKey: string): Promise<GtfsRtFeed> {
  const u = new URL(url)
  u.searchParams.set('apiKey', apiKey)
  u.searchParams.set('asJson', 'true')

  const res = await fetch(u.toString(), {
    headers: { 'User-Agent': 'TransitPlatform/1.0' },
    signal: AbortSignal.timeout(15_000),
  })
  if (!res.ok) throw new Error(`HTTP ${res.status} from ${url}`)
  return res.json() as Promise<GtfsRtFeed>
}

// ── Poller ─────────────────────────────────────────────────────────────────────

async function poll(): Promise<void> {
  const apiKey = process.env.MECATRAN_API_KEY
  if (!apiKey) { state.error = 'MECATRAN_API_KEY not set'; return }

  try {
    // Fetch both feeds concurrently
    const [vpData, tuData] = await Promise.all([
      fetchFeed(VP_URL, apiKey),
      fetchFeed(TU_URL, apiKey),
    ])

    const vpMap = parseVP(vpData)
    const tuDelays = parseTU(tuData)

    // Join: VP vehicles enriched with TU delay
    const vehicles: VehiclePosition[] = []
    for (const [tripId, vp] of vpMap) {
      const delay = tuDelays.get(tripId) ?? null
      vehicles.push({ ...vp, delay_seconds: delay, otp_status: classifyOtp(delay) })
    }

    const now = new Date()
    state.vehicles = vehicles
    state.fetched_at = now.toISOString()
    state.fetched_at_ms = now.getTime()
    state.feed_ts = vpData.header?.timestamp ? String(vpData.header.timestamp) : null
    state.error = null

    const matched = vehicles.filter(v => v.delay_seconds !== null).length
    console.log(`[poll] ${vehicles.length} vehicles, ${matched} with OTP data`)
  } catch (err) {
    state.error = err instanceof Error ? err.message : 'Unknown poll error'
    console.error(`[poll] Error: ${state.error}`)
  }
}

export function startPoller(): void {
  poll()
  setInterval(poll, POLL_INTERVAL_MS)
  console.log(`[poller] Started — refreshing every ${POLL_INTERVAL_MS / 1000}s`)
}

// ── Public accessors ───────────────────────────────────────────────────────────

export function getVehiclesForRoute(routeId: string): VehiclePosition[] {
  return state.vehicles.filter((v) => v.route_id === routeId)
}

export function getCacheStatus() {
  return {
    vehicle_count: state.vehicles.length,
    fetched_at: state.fetched_at,
    fetched_at_ms: state.fetched_at_ms,
    feed_ts: state.feed_ts,
    error: state.error,
    healthy: state.vehicles.length > 0 && !state.error,
  }
}

export function getActiveRouteIds(): string[] {
  const ids = new Set(state.vehicles.map((v) => v.route_id).filter(Boolean) as string[])
  return Array.from(ids).sort((a, b) => {
    const na = parseInt(a), nb = parseInt(b)
    return !isNaN(na) && !isNaN(nb) ? na - nb : a.localeCompare(b)
  })
}
