'use client'

import { useState, useEffect, useCallback, useRef, useMemo } from 'react'
import { useFilterPanel } from '@/lib/filter-panel-context'
import { FilterSection } from '@/components/ui/FilterControls'
import LiveMap, { type LiveVehicle, type OtpStatus } from '@/components/map/LiveMap'
import type { DimRoute, RouteStop } from '@/types'

const RENDER_API = process.env.NEXT_PUBLIC_RENDER_API_URL ?? 'http://localhost:3001'
const REFRESH_MS = 30_000

// Valley Metro display colors — derived from routes.txt with minor light-map adjustments.
//
// All four non-bus services share route_type 0 (tram/light rail in GTFS), so name-based
// overrides are required to distinguish them.  Bus routes (route_type 3) fall through to
// the type-based default.
//
// GTFS brand hex → dark-map display hex
// Each color is the brand hue shifted to a vivid, light-on-dark variant that
// pops on the dataviz-dark map while remaining clearly recognisable.
//   Bus  #591769 → #A855F7  (same deep purple hue, vivid)
//   A    #1E8ECD → #38BDF8  (sky blue, brightened)
//   B    #B76912 → #F59E0B  (amber, brightened)
//   S    #6D8932 → #84CC16  (olive/lime, same green family, vivid)
//   SKYT #53565F → #94A3B8  (slate, lightened for dark bg legibility)
const VM_NAME_COLORS: Record<string, string> = {
  'A':    '#38BDF8',  // Valley Metro Rail A Line — sky blue
  'B':    '#F59E0B',  // Valley Metro Rail B Line — amber
  'S':    '#84CC16',  // Valley Metro Streetcar — lime green
  'SKYT': '#94A3B8',  // PHX Sky Train — slate
}

const VM_TYPE_COLORS: Record<number, string> = {
  0: '#38BDF8',  // Generic tram/light rail fallback
  1: '#A855F7',  // Metro fallback
  2: '#A855F7',  // Rail fallback
  3: '#A855F7',  // Local & Rapid bus — vivid purple
}

const FALLBACK_ROUTE_COLOR = '#A855F7'

function getCuratedColor(routeType: number, routeShortName?: string): string {
  if (routeShortName) {
    const key = routeShortName.toUpperCase().trim()
    if (VM_NAME_COLORS[key]) return VM_NAME_COLORS[key]
  }
  return VM_TYPE_COLORS[routeType] ?? FALLBACK_ROUTE_COLOR
}

interface FeedResponse {
  route_id: string
  vehicle_count: number
  fetched_at: string | null
  fetched_at_ms: number | null
  vehicles: LiveVehicle[]
}

// OTP color for legend/stats (matches LiveMap)
const OTP_COLOR: Record<OtpStatus, string> = {
  early:     '#ec4899',
  on_time:   '#22c55e',
  late:      '#f59e0b',
  very_late: '#ef4444',
  unknown:   '#6b7280',
}

function formatDelay(s: number | null): string {
  if (s === null) return 'No schedule data'
  if (s < -60) return `${Math.abs(Math.round(s / 60))}m early`
  if (s <= 60)  return 'On time'
  const m = Math.floor(s / 60), sec = s % 60
  return sec > 0 ? `+${m}m ${sec}s late` : `+${m}m late`
}

export default function LivePageClient() {
  const { setContent } = useFilterPanel()
  const setContentRef = useRef(setContent)
  setContentRef.current = setContent

  // Routes — populated immediately from Render, names/types enriched from Databricks
  const [routeIds, setRouteIds] = useState<string[]>([])
  const [routeNames, setRouteNames] = useState<Map<string, string>>(new Map())
  const [routeTypes, setRouteTypes] = useState<Map<string, number>>(new Map())
  const [selectedRouteId, setSelectedRouteId] = useState<string | null>(null)
  const [routesLoading, setRoutesLoading] = useState(true)
  const [routeNamesLoading, setRouteNamesLoading] = useState(false)

  // Selected vehicle
  const [selectedVehicleId, setSelectedVehicleId] = useState<string | null>(null)
  const [tripStartTs, setTripStartTs] = useState<string | null>(null)

  // Feed
  const [feed, setFeed] = useState<FeedResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Route stops (for map overlay) — may include point_type:'shape' | 'stop' tags
  const [routeStops, setRouteStops] = useState<Array<RouteStop & { point_type?: string }>>([])
  // Fetch stops + shape points for selected route.
  // Two separate queries so stops always render even if silver_fact_shape_points doesn't exist yet.
  useEffect(() => {
    if (!selectedRouteId) { setRouteStops([]); return }

    const body = (sql: string) => JSON.stringify({ sql, params: { routeId: selectedRouteId } })
    const headers = { 'Content-Type': 'application/json' }

    const stopsQuery = `
      WITH trip_stop_counts AS (
        SELECT t.trip_id, t.direction_id, COUNT(*) AS n
        FROM silver_dim_trip t
        JOIN silver_fact_stop_schedule ss ON t.trip_id = ss.trip_id
        WHERE t.route_id = :routeId
        GROUP BY t.trip_id, t.direction_id
      ),
      best_trip AS (
        SELECT trip_id, direction_id,
          ROW_NUMBER() OVER (PARTITION BY direction_id ORDER BY n DESC) AS rn
        FROM trip_stop_counts
      ),
      rep_trips AS (SELECT trip_id, direction_id FROM best_trip WHERE rn = 1)
      SELECT ss.stop_sequence, ss.stop_id, s.stop_name, s.lat, s.lon, rt.direction_id
      FROM rep_trips rt
      JOIN silver_fact_stop_schedule ss ON rt.trip_id = ss.trip_id
      JOIN silver_dim_stop s ON ss.stop_id = s.stop_id
      ORDER BY rt.direction_id, ss.stop_sequence
    `

    const shapesQuery = `
      WITH best_trip AS (
        SELECT t.shape_id, t.direction_id,
          ROW_NUMBER() OVER (PARTITION BY t.direction_id ORDER BY COUNT(*) DESC) AS rn
        FROM silver_dim_trip t
        JOIN silver_fact_stop_schedule ss ON t.trip_id = ss.trip_id
        WHERE t.route_id = :routeId AND t.shape_id IS NOT NULL
        GROUP BY t.shape_id, t.direction_id
      )
      SELECT sp.shape_pt_sequence AS stop_sequence,
        sp.shape_id AS stop_id,
        CAST(NULL AS STRING) AS stop_name,
        sp.shape_pt_lat AS lat,
        sp.shape_pt_lon AS lon,
        bt.direction_id
      FROM best_trip bt
      JOIN silver_fact_shape_points sp ON bt.shape_id = sp.shape_id
      WHERE bt.rn = 1
      ORDER BY bt.direction_id, sp.shape_pt_sequence
    `

    const fetchJson = (sql: string) =>
      fetch('/api/databricks/query', { method: 'POST', headers, body: body(sql) })
        .then((r) => r.json())
        .then((d: { rows?: RouteStop[] }) => d.rows ?? [])
        .catch(() => [] as RouteStop[])

    Promise.all([
      fetchJson(stopsQuery),
      fetchJson(shapesQuery),
    ]).then(([stops, shapes]) => {
      type Tagged = RouteStop & { point_type: string }
      const tagged: Tagged[] = [
        ...shapes.map((r) => ({ ...r, point_type: 'shape' })),
        ...stops.map((r) => ({ ...r, point_type: 'stop' })),
      ]
      setRouteStops(tagged)
    })
  }, [selectedRouteId])

  // Fetch trip start time from static GTFS when a vehicle is selected
  useEffect(() => {
    if (!selectedVehicleId) { setTripStartTs(null); return }
    const tripId = feed?.vehicles.find((v) => v.vehicle_id === selectedVehicleId)?.trip_id
    if (!tripId) { setTripStartTs(null); return }

    const safeId = tripId.replace(/'/g, "''")
    fetch('/api/databricks/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: `
          SELECT scheduled_arrival_secs
          FROM silver_fact_stop_schedule
          WHERE trip_id = '${safeId}'
          ORDER BY stop_sequence ASC
          LIMIT 1
        `,
      }),
    })
      .then((r) => r.json())
      .then((d: { rows?: Array<{ scheduled_arrival_secs: number }> }) => {
        const secs = d.rows?.[0]?.scheduled_arrival_secs
        setTripStartTs(secs != null ? fmtGtfsSecs(secs) : null)
      })
      .catch(() => setTripStartTs(null))
  }, [selectedVehicleId, feed?.vehicles])

  // Warmup: ping Databricks on mount so the serverless warehouse starts before Step 2 fires
  useEffect(() => {
    fetch('/api/databricks/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'SELECT 1' }),
    }).catch(() => {})
  }, [])

  // Step 1: get active route IDs from Render immediately (fast — already cached)
  useEffect(() => {
    fetch(`${RENDER_API}/vehicles/routes`)
      .then((r) => r.json())
      .then((d: { route_ids: string[] }) => {
        setRouteIds(d.route_ids ?? [])
        setRouteNamesLoading(true)
      })
      .catch(() => {})
      .finally(() => setRoutesLoading(false))
  }, [])

  // Step 2: enrich with human-readable names from Databricks in background
  useEffect(() => {
    if (routeIds.length === 0) return
    fetch('/api/databricks/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: `
          SELECT route_id, route_short_name, route_long_name, route_type
          FROM silver_dim_route
          WHERE route_id IN (${routeIds.map((id) => `'${id}'`).join(',')})
        `,
      }),
    })
      .then((r) => r.json())
      .then((d: { rows: Array<{ route_id: string; route_short_name: string; route_long_name: string; route_type: number }> }) => {
        const names = new Map<string, string>()
        const types = new Map<string, number>()
        for (const row of d.rows ?? []) {
          names.set(row.route_id, `${row.route_short_name} – ${row.route_long_name}`)
          if (row.route_type != null) types.set(row.route_id, row.route_type)
        }
        setRouteNames(names)
        setRouteTypes(types)
      })
      .catch(() => {})
      .finally(() => setRouteNamesLoading(false))
  }, [routeIds])

  // Build display routes merging IDs + names — memoized to avoid infinite filter panel loop
  const routes = useMemo<DimRoute[]>(
    () => routeIds.map((id) => {
      const name = routeNames.get(id)
      return {
        route_id: id,
        route_short_name: id,
        route_long_name: name ? name.split(' – ')[1] ?? '' : '',
        route_type: routeTypes.get(id) ?? 3,
        route_color: null,
      }
    }),
    [routeIds, routeNames, routeTypes]
  )

  // Fetch vehicles from Render
  const fetchVehicles = useCallback(async (routeId: string) => {
    try {
      const res = await fetch(`${RENDER_API}/vehicles?route_id=${encodeURIComponent(routeId)}`)
      if (!res.ok) throw new Error(`Feed error: ${res.status}`)
      const data: FeedResponse = await res.json()
      setFeed(data)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Feed unavailable')
    }
  }, [])

  // Start/restart polling on route change
  useEffect(() => {
    if (timerRef.current) clearInterval(timerRef.current)
    setFeed(null)             // clear stale vehicles immediately on route change
    setSelectedVehicleId(null) // deselect any vehicle when switching routes
    if (!selectedRouteId) { return }

    fetchVehicles(selectedRouteId)
    timerRef.current = setInterval(() => fetchVehicles(selectedRouteId), REFRESH_MS)
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [selectedRouteId, fetchVehicles])

  // OTP summary counts
  const otpCounts = feed?.vehicles.reduce(
    (acc, v) => { acc[v.otp_status] = (acc[v.otp_status] ?? 0) + 1; return acc },
    {} as Partial<Record<OtpStatus, number>>
  ) ?? {}

  const selectedRoute = routes.find((r) => r.route_id === selectedRouteId)
  // Always derive live vehicle data from the current feed so the panel auto-refreshes
  const selectedVehicle = feed?.vehicles.find((v) => v.vehicle_id === selectedVehicleId) ?? null

  // Inject filter panel — swaps to vehicle detail panel when a vehicle is selected
  useEffect(() => {
    if (selectedVehicle) {
      setContentRef.current(
        <VehicleDetailPanel
          vehicle={selectedVehicle}
          tripStartTs={tripStartTs}
          onBack={() => setSelectedVehicleId(null)}
        />
      )
    } else {
      setContentRef.current(
        <LiveFilters
          routes={routes}
          selectedRouteId={selectedRouteId}
          onSelect={setSelectedRouteId}
          routesLoading={routesLoading}
          routeNamesLoading={routeNamesLoading}
        />
      )
    }
  }, [routes, selectedRouteId, routesLoading, routeNamesLoading, selectedVehicle, tripStartTs])

  return (
    <div className="flex flex-col h-full relative">

      {/* Empty state */}
      {!selectedRouteId && (
        <div className="absolute inset-0 z-10 flex items-center justify-center pointer-events-none">
          <div className="bg-gray-900/90 border border-gray-700 rounded-2xl px-8 py-6 text-center backdrop-blur-sm shadow-2xl">
            <div className="w-12 h-12 rounded-full bg-blue-500/10 flex items-center justify-center mx-auto mb-3">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-6 h-6 text-blue-400">
                <circle cx="12" cy="12" r="3" fill="currentColor" stroke="none" />
                <path strokeLinecap="round" d="M6.3 6.3a8 8 0 000 11.4M17.7 6.3a8 8 0 010 11.4" />
              </svg>
            </div>
            <p className="text-white font-semibold mb-1">Select a route to begin</p>
            <p className="text-gray-400 text-sm">Use the Filters panel on the left</p>
          </div>
        </div>
      )}

      {/* Stats overlay */}
      {feed && selectedRoute && (
        <div className="absolute top-4 right-4 z-10 bg-gray-900/95 border border-gray-700 rounded-xl shadow-2xl p-4 w-64 backdrop-blur-sm">
          <div className="flex items-center justify-between mb-1">
            <span className="text-white font-semibold text-sm">
              Route {selectedRoute.route_short_name}
            </span>
            <span className="flex items-center gap-1.5">
              <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
              <span className="text-emerald-400 text-xs">Live</span>
            </span>
          </div>
          {selectedRoute.route_long_name && (
            <p className="text-gray-500 text-xs mb-3 truncate">{selectedRoute.route_long_name}</p>
          )}

          {/* OTP breakdown */}
          <div className="space-y-1.5">
            {([
              ['on_time',   'On Time'],
              ['late',      'Late'],
              ['very_late', 'Very Late'],
              ['early',     'Early'],
              ['unknown',   'No Data'],
            ] as [OtpStatus, string][]).map(([status, label]) => {
              const count = otpCounts[status] ?? 0
              if (count === 0) return null
              return (
                <div key={status} className="flex items-center justify-between text-xs">
                  <div className="flex items-center gap-2">
                    <div className="w-2.5 h-2.5 rounded-full" style={{ background: OTP_COLOR[status] }} />
                    <span className="text-gray-300 whitespace-nowrap">{label}</span>
                  </div>
                  <span className="text-white font-medium">
                    {count} ({Math.round((count / feed.vehicle_count) * 100)}%)
                  </span>
                </div>
              )
            })}
          </div>

          {error && <p className="text-red-400 text-xs mt-2">{error}</p>}
        </div>
      )}

<LiveMap
  vehicles={feed?.vehicles ?? []}
  fetchedAtMs={feed?.fetched_at_ms ?? null}
  routeStops={routeStops}
  routeColor={selectedRouteId ? getCuratedColor(
    routeTypes.get(selectedRouteId) ?? 3,
    routes.find(r => r.route_id === selectedRouteId)?.route_short_name
  ) : null}
  selectedVehicleId={selectedVehicleId}
  onVehicleSelect={(v) => setSelectedVehicleId(v ? v.vehicle_id : null)}
/>
    </div>
  )
}

// GTFS scheduled_arrival_secs (seconds since Phoenix midnight) → "H:MM AM/PM"
// Handles >86400s values (trips that cross midnight in GTFS notation)
function fmtGtfsSecs(secs: number): string {
  const h = Math.floor(secs / 3600) % 24
  const m = Math.floor((secs % 3600) / 60)
  const h12 = h % 12 || 12
  const ampm = h < 12 ? 'AM' : 'PM'
  return `${h12}:${String(m).padStart(2, '0')} ${ampm}`
}

// ── Vehicle detail panel ───────────────────────────────────────────────────────

function VehicleDetailPanel({ vehicle, tripStartTs, onBack }: {
  vehicle: LiveVehicle
  tripStartTs: string | null
  onBack: () => void
}) {
  const color = OTP_COLOR[vehicle.otp_status]
  const speedMph = vehicle.speed_mps != null ? (vehicle.speed_mps * 2.237).toFixed(0) : null

  const rows: [string, string][] = [
    ['Route', vehicle.route_id  ?? '—'],
    ['To',    vehicle.headsign  ?? '—'],
    ['Start', tripStartTs ?? '…'],   // '…' while Databricks query is in flight
    ['Speed', speedMph ? `${speedMph} mph` : '—'],
  ]

  return (
    <div className="flex flex-col gap-4">
      {/* Back button */}
      <button
        onClick={onBack}
        className="flex items-center gap-1.5 text-xs text-gray-400 hover:text-white transition-colors w-fit"
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-3.5 h-3.5">
          <path strokeLinecap="round" d="M19 12H5M12 5l-7 7 7 7" />
        </svg>
        Back to route filter
      </button>

      {/* Vehicle header */}
      <div>
        <span
          className="inline-block text-xs font-semibold px-2 py-0.5 rounded-full border capitalize mb-2"
          style={{ color, borderColor: `${color}55`, background: `${color}20` }}
        >
          {vehicle.otp_status.replace('_', ' ')}
        </span>
        <h2 className="text-white font-bold text-base leading-tight">
          Vehicle {vehicle.vehicle_id}
        </h2>
      </div>

      {/* Delay — large focal number */}
      <div
        className="rounded-xl px-4 py-3 text-center border"
        style={{ background: `${color}10`, borderColor: `${color}30` }}
      >
        <p className="text-gray-400 text-xs mb-1 uppercase tracking-wide">Current Delay</p>
        <p className="font-bold text-2xl" style={{ color }}>
          {formatDelay(vehicle.delay_seconds)}
        </p>
      </div>

      {/* Detail rows */}
      <div className="space-y-2.5">
        {rows.map(([label, value]) => (
          <div key={label} className="flex items-start justify-between gap-3 text-xs">
            <span className="text-gray-500 shrink-0">{label}</span>
            <span className="text-gray-200 font-medium text-right leading-relaxed">{value}</span>
          </div>
        ))}
      </div>

      {/* Live indicator */}
      <div className="flex items-center gap-1.5 pt-1">
        <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
        <span className="text-gray-600 text-xs">Live — refreshes every 30s</span>
      </div>
    </div>
  )
}

// ── Filter panel ───────────────────────────────────────────────────────────────

function LiveFilters({
  routes, selectedRouteId, onSelect, routesLoading, routeNamesLoading,
}: {
  routes: DimRoute[]
  selectedRouteId: string | null
  onSelect: (id: string | null) => void
  routesLoading: boolean
  routeNamesLoading: boolean
}) {
  const sectionLabel = routesLoading
    ? 'Route'
    : `Route (${routes.length} active)`

  return (
    <FilterSection label={sectionLabel}>
      <SingleRouteFilter
        routes={routes}
        selectedId={selectedRouteId}
        onSelect={onSelect}
        routesLoading={routesLoading}
        routeNamesLoading={routeNamesLoading}
      />
    </FilterSection>
  )
}

function SingleRouteFilter({
  routes, selectedId, onSelect, routesLoading, routeNamesLoading,
}: {
  routes: DimRoute[]
  selectedId: string | null
  onSelect: (id: string | null) => void
  routesLoading: boolean
  routeNamesLoading: boolean
}) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    function handler(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const filtered = routes.filter((r) => {
    const q = search.toLowerCase()
    return r.route_short_name.toLowerCase().includes(q) ||
      r.route_long_name.toLowerCase().includes(q)
  })

  const selected = routes.find((r) => r.route_id === selectedId)
  const label = selected
    ? `Route ${selected.route_short_name}${selected.route_long_name ? ` – ${selected.route_long_name}` : ''}`
    : routesLoading ? 'Loading routes…' : 'Select a route…'

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-left transition-colors hover:border-gray-600"
      >
        <span className={selectedId ? 'text-white truncate pr-2' : 'text-gray-500'}>{label}</span>
        {routesLoading ? (
          <svg className="w-4 h-4 text-gray-500 shrink-0 animate-spin" viewBox="0 0 24 24" fill="none">
            <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth={3} strokeOpacity={0.25} />
            <path d="M12 2a10 10 0 0 1 10 10" stroke="currentColor" strokeWidth={3} strokeLinecap="round" />
          </svg>
        ) : (
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-4 h-4 text-gray-500 shrink-0">
            <path strokeLinecap="round" d="M19 9l-7 7-7-7" />
          </svg>
        )}
      </button>

      {open && (
        <div className="absolute top-full left-0 right-0 mt-1 bg-gray-800 border border-gray-700 rounded-lg shadow-xl z-50 max-h-72 flex flex-col">
          <div className="p-2 border-b border-gray-700">
            <input
              autoFocus
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search routes…"
              className="w-full bg-gray-900 rounded px-2 py-1.5 text-sm text-white placeholder-gray-500 focus:outline-none"
            />
          </div>
          <div className="overflow-y-auto flex-1">
            {routesLoading ? (
              <div className="flex flex-col items-center justify-center py-6 gap-2">
                <svg className="w-5 h-5 text-blue-400 animate-spin" viewBox="0 0 24 24" fill="none">
                  <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth={3} strokeOpacity={0.25} />
                  <path d="M12 2a10 10 0 0 1 10 10" stroke="currentColor" strokeWidth={3} strokeLinecap="round" />
                </svg>
                <p className="text-gray-500 text-xs">Fetching active routes…</p>
              </div>
            ) : (
              <>
                {selectedId && (
                  <button
                    onClick={() => { onSelect(null); setOpen(false) }}
                    className="w-full text-left px-3 py-2 text-xs text-blue-400 hover:bg-gray-700 border-b border-gray-700"
                  >
                    Clear selection
                  </button>
                )}
                {filtered.map((r) => (
                  <button
                    key={r.route_id}
                    onClick={() => { onSelect(r.route_id); setOpen(false) }}
                    className={`w-full text-left px-3 py-2 text-sm flex items-center gap-2 hover:bg-gray-700 transition-colors ${
                      r.route_id === selectedId ? 'text-blue-300 bg-blue-600/10' : 'text-gray-300'
                    }`}
                  >
                    <span className="font-semibold w-8 shrink-0">{r.route_short_name}</span>
                    {r.route_long_name && (
                      <span className="text-gray-500 text-xs truncate">{r.route_long_name}</span>
                    )}
                  </button>
                ))}
                {filtered.length === 0 && (
                  <p className="text-gray-600 text-xs text-center py-4">No routes found</p>
                )}
                {routeNamesLoading && routes.length > 0 && (
                  <div className="flex items-center gap-1.5 px-3 py-2 border-t border-gray-700/60">
                    <svg className="w-3 h-3 text-gray-600 animate-spin shrink-0" viewBox="0 0 24 24" fill="none">
                      <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth={3} strokeOpacity={0.25} />
                      <path d="M12 2a10 10 0 0 1 10 10" stroke="currentColor" strokeWidth={3} strokeLinecap="round" />
                    </svg>
                    <span className="text-gray-600 text-xs">Loading route names…</span>
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
