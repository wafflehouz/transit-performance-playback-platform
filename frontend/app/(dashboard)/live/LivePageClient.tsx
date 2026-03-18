'use client'

import { useState, useEffect, useCallback, useRef, useMemo } from 'react'
import { useFilterPanel } from '@/lib/filter-panel-context'
import { FilterSection } from '@/components/ui/FilterControls'
import LiveMap, { type LiveVehicle, type OtpStatus } from '@/components/map/LiveMap'
import type { DimRoute, RouteStop } from '@/types'

const RENDER_API = process.env.NEXT_PUBLIC_RENDER_API_URL ?? 'http://localhost:3001'
const REFRESH_MS = 30_000

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

export default function LivePageClient() {
  const { setContent } = useFilterPanel()
  const setContentRef = useRef(setContent)
  setContentRef.current = setContent

  // Routes — populated immediately from Render, names/colors enriched from Databricks
  const [routeIds, setRouteIds] = useState<string[]>([])
  const [routeNames, setRouteNames] = useState<Map<string, string>>(new Map())
  const [routeColors, setRouteColors] = useState<Map<string, string>>(new Map())
  const [selectedRouteId, setSelectedRouteId] = useState<string | null>(null)

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

  // Step 1: get active route IDs from Render immediately (fast — already cached)
  useEffect(() => {
    fetch(`${RENDER_API}/vehicles/routes`)
      .then((r) => r.json())
      .then((d: { route_ids: string[] }) => setRouteIds(d.route_ids ?? []))
      .catch(() => {})
  }, [])

  // Step 2: enrich with human-readable names from Databricks in background
  useEffect(() => {
    if (routeIds.length === 0) return
    fetch('/api/databricks/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: `
          SELECT route_id, route_short_name, route_long_name, route_color
          FROM silver_dim_route
          WHERE route_id IN (${routeIds.map((id) => `'${id}'`).join(',')})
        `,
      }),
    })
      .then((r) => r.json())
      .then((d: { rows: Array<{ route_id: string; route_short_name: string; route_long_name: string; route_color: string | null }> }) => {
        const names = new Map<string, string>()
        const colors = new Map<string, string>()
        for (const row of d.rows ?? []) {
          names.set(row.route_id, `${row.route_short_name} – ${row.route_long_name}`)
          if (row.route_color) colors.set(row.route_id, `#${row.route_color}`)
        }
        setRouteNames(names)
        setRouteColors(colors)
      })
      .catch(() => {})
  }, [routeIds])

  // Build display routes merging IDs + names — memoized to avoid infinite filter panel loop
  const routes = useMemo<DimRoute[]>(
    () => routeIds.map((id) => {
      const name = routeNames.get(id)
      return {
        route_id: id,
        route_short_name: id,
        route_long_name: name ? name.split(' – ')[1] ?? '' : '',
        route_type: 3,
      }
    }),
    [routeIds, routeNames]
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
    setFeed(null) // always clear stale vehicles immediately on route change
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

  // Inject filter panel — use ref so setContent never appears in deps
  useEffect(() => {
    setContentRef.current(
      <LiveFilters
        routes={routes}
        selectedRouteId={selectedRouteId}
        onSelect={setSelectedRouteId}
      />
    )
  }, [routes, selectedRouteId])

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
                    <span className="text-gray-300">{label}</span>
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
  routeColor={selectedRouteId ? (routeColors.get(selectedRouteId) ?? null) : null}
/>
    </div>
  )
}

// ── Filter panel ───────────────────────────────────────────────────────────────

function LiveFilters({
  routes, selectedRouteId, onSelect,
}: {
  routes: DimRoute[]
  selectedRouteId: string | null
  onSelect: (id: string | null) => void
}) {
  return (
    <FilterSection label={`Route ${routes.length > 0 ? `(${routes.length} active)` : '—'}`}>
      <SingleRouteFilter routes={routes} selectedId={selectedRouteId} onSelect={onSelect} />
    </FilterSection>
  )
}

function SingleRouteFilter({
  routes, selectedId, onSelect,
}: {
  routes: DimRoute[]
  selectedId: string | null
  onSelect: (id: string | null) => void
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
    : 'Select a route…'

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-left transition-colors hover:border-gray-600"
      >
        <span className={selectedId ? 'text-white truncate pr-2' : 'text-gray-500'}>{label}</span>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-4 h-4 text-gray-500 shrink-0">
          <path strokeLinecap="round" d="M19 9l-7 7-7-7" />
        </svg>
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
          </div>
        </div>
      )}
    </div>
  )
}
