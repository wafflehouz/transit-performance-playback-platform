'use client'

import { useState, useEffect, useCallback, useRef } from 'react'
import { useFilterPanel } from '@/lib/filter-panel-context'
import { FilterSection } from '@/components/ui/FilterControls'
import LiveMap, { type LiveVehicle, type OtpStatus } from '@/components/map/LiveMap'
import type { DimRoute } from '@/types'

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

  // Routes — populated immediately from Render, names enriched from Databricks
  const [routeIds, setRouteIds] = useState<string[]>([])
  const [routeNames, setRouteNames] = useState<Map<string, string>>(new Map())
  const [selectedRouteId, setSelectedRouteId] = useState<string | null>(null)

  // Feed
  const [feed, setFeed] = useState<FeedResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

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
          SELECT route_id, route_short_name, route_long_name
          FROM silver_dim_route
          WHERE route_id IN (${routeIds.map((id) => `'${id}'`).join(',')})
        `,
      }),
    })
      .then((r) => r.json())
      .then((d: { rows: Array<{ route_id: string; route_short_name: string; route_long_name: string }> }) => {
        const m = new Map<string, string>()
        for (const row of d.rows ?? []) {
          m.set(row.route_id, `${row.route_short_name} – ${row.route_long_name}`)
        }
        setRouteNames(m)
      })
      .catch(() => {})
  }, [routeIds])

  // Build display routes merging IDs + names
  const routes: DimRoute[] = routeIds.map((id) => {
    const name = routeNames.get(id)
    return {
      route_id: id,
      route_short_name: id,
      route_long_name: name ? name.split(' – ')[1] ?? '' : '',
      route_type: 3,
    }
  })

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
    if (!selectedRouteId) { setFeed(null); return }

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

  // Inject filter panel
  useEffect(() => {
    setContent(
      <LiveFilters
        routes={routes}
        selectedRouteId={selectedRouteId}
        onSelect={setSelectedRouteId}
      />
    )
  }, [routes, selectedRouteId, setContent])

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

      {/* OTP legend — bottom right */}
      {selectedRouteId && (
        <div className="absolute bottom-8 right-4 z-10 bg-gray-900/90 border border-gray-700 rounded-lg px-3 py-2.5 backdrop-blur-sm">
          <p className="text-gray-500 text-[10px] uppercase tracking-wider mb-2">On-Time Status</p>
          <div className="space-y-1.5">
            {([
              ['on_time',   'On Time'],
              ['early',     'Early'],
              ['late',      'Late (1–10 min)'],
              ['very_late', 'Very Late (10+ min)'],
              ['unknown',   'No schedule data'],
            ] as [OtpStatus, string][]).map(([status, label]) => (
              <div key={status} className="flex items-center gap-2">
                <div className="w-2.5 h-2.5 rounded-full shrink-0" style={{ background: OTP_COLOR[status] }} />
                <span className="text-gray-300 text-xs">{label}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <LiveMap vehicles={feed?.vehicles ?? []} fetchedAtMs={feed?.fetched_at_ms ?? null} />
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
  const [search, setSearch] = useState('')

  const filtered = routes.filter((r) => {
    const q = search.toLowerCase()
    return r.route_short_name.toLowerCase().includes(q) ||
      r.route_long_name.toLowerCase().includes(q)
  })

  return (
    <FilterSection label={`Route ${routes.length > 0 ? `(${routes.length} active)` : '—'}`}>
      <input
        type="text"
        value={search}
        onChange={(e) => setSearch(e.target.value)}
        placeholder="Search routes…"
        className="w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-white placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-blue-500 mb-2"
      />
      <div className="space-y-0.5 max-h-80 overflow-y-auto">
        {filtered.map((r) => (
          <button
            key={r.route_id}
            onClick={() => onSelect(r.route_id === selectedRouteId ? null : r.route_id)}
            className={`w-full text-left px-3 py-2 rounded-lg text-sm transition-colors flex items-center gap-2 ${
              r.route_id === selectedRouteId
                ? 'bg-blue-600/20 text-blue-300'
                : 'text-gray-300 hover:bg-gray-800 hover:text-white'
            }`}
          >
            <span className="font-semibold w-8 shrink-0">{r.route_short_name}</span>
            {r.route_long_name && (
              <span className="text-gray-500 text-xs truncate">{r.route_long_name}</span>
            )}
          </button>
        ))}
        {filtered.length === 0 && (
          <p className="text-gray-600 text-xs text-center py-3">No routes found</p>
        )}
      </div>
      {selectedRouteId && (
        <button
          onClick={() => onSelect(null)}
          className="w-full text-xs text-gray-500 hover:text-gray-300 transition-colors mt-2"
        >
          Clear selection
        </button>
      )}
    </FilterSection>
  )
}
