'use client'

import { useState, useEffect, useCallback, useRef } from 'react'
import { useFilterPanel } from '@/lib/filter-panel-context'
import { FilterSection, RouteFilter } from '@/components/ui/FilterControls'
import LiveMap, { type LiveVehicle } from '@/components/map/LiveMap'
import type { DimRoute } from '@/types'

const RENDER_API = process.env.NEXT_PUBLIC_RENDER_API_URL ?? 'http://localhost:3001'
const REFRESH_MS = 30_000

interface FeedResponse {
  route_id: string
  vehicle_count: number
  fetched_at: string | null
  feed_ts: string | null
  vehicles: LiveVehicle[]
}

export default function LivePageClient() {
  const { setContent } = useFilterPanel()

  // Route selection — single route required
  const [routes, setRoutes] = useState<DimRoute[]>([])
  const [selectedRouteId, setSelectedRouteId] = useState<string | null>(null)

  // Feed data
  const [feed, setFeed] = useState<FeedResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Load route list from Databricks (one-time, small query)
  useEffect(() => {
    fetch('/api/databricks/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: `
          SELECT DISTINCT route_id, route_short_name, route_long_name, route_type
          FROM silver_dim_route
          ORDER BY
            CASE WHEN route_short_name RLIKE '^[0-9]+$'
                 THEN CAST(route_short_name AS INT) ELSE 9999 END,
            route_short_name
        `,
      }),
    })
      .then((r) => r.json())
      .then((d) => setRoutes(d.rows ?? []))
      .catch(() => {})
  }, [])

  // Fetch vehicles from Render service
  const fetchVehicles = useCallback(async (routeId: string) => {
    try {
      const res = await fetch(`${RENDER_API}/vehicles?route_id=${encodeURIComponent(routeId)}`)
      if (!res.ok) throw new Error(`Feed error: ${res.status}`)
      const data: FeedResponse = await res.json()
      setFeed(data)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to reach live feed')
    } finally {
      setLoading(false)
    }
  }, [])

  // Start/restart polling when route changes
  useEffect(() => {
    if (timerRef.current) clearInterval(timerRef.current)
    if (!selectedRouteId) { setFeed(null); return }

    setLoading(true)
    fetchVehicles(selectedRouteId)
    timerRef.current = setInterval(() => fetchVehicles(selectedRouteId), REFRESH_MS)
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [selectedRouteId, fetchVehicles])

  const selectedRoute = routes.find((r) => r.route_id === selectedRouteId)

  // Inject filter panel
  useEffect(() => {
    setContent(
      <LiveFilters
        routes={routes}
        selectedRouteId={selectedRouteId}
        onSelect={setSelectedRouteId}
        feed={feed}
        onRefresh={() => selectedRouteId && fetchVehicles(selectedRouteId)}
      />
    )
  }, [routes, selectedRouteId, feed, fetchVehicles, setContent])

  return (
    <div className="flex flex-col h-full relative">

      {/* No route selected — prompt */}
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
            <p className="text-gray-400 text-sm">Use the Filters panel to choose a route</p>
          </div>
        </div>
      )}

      {/* Stats overlay — shown when route selected */}
      {feed && selectedRoute && (
        <div className="absolute top-4 right-4 z-10 bg-gray-900/95 border border-gray-700 rounded-xl shadow-2xl p-4 w-60 backdrop-blur-sm">
          <div className="flex items-center justify-between mb-2">
            <span className="text-white font-semibold text-sm">
              Route {selectedRoute.route_short_name}
            </span>
            <span className="flex items-center gap-1.5">
              <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
              <span className="text-emerald-400 text-xs">Live</span>
            </span>
          </div>
          <p className="text-gray-500 text-xs mb-3 leading-snug truncate">
            {selectedRoute.route_long_name}
          </p>
          <div className="grid grid-cols-2 gap-2">
            <StatCard label="Vehicles" value={String(feed.vehicle_count)} />
            <StatCard
              label="Updated"
              value={
                feed.fetched_at
                  ? new Date(feed.fetched_at).toLocaleTimeString('en-US', {
                      hour: '2-digit', minute: '2-digit', timeZone: 'America/Phoenix',
                    })
                  : '—'
              }
            />
          </div>
          {error && <p className="text-red-400 text-xs mt-2">{error}</p>}
        </div>
      )}

      {/* Speed legend */}
      {selectedRouteId && (
        <div className="absolute bottom-8 right-4 z-10 bg-gray-900/90 border border-gray-700 rounded-lg px-3 py-2 backdrop-blur-sm">
          <p className="text-gray-400 text-[10px] uppercase tracking-wider mb-1.5">Speed</p>
          <div className="space-y-1">
            {[
              { color: 'bg-emerald-500', label: '> 18 mph' },
              { color: 'bg-amber-400',   label: '8 – 18 mph' },
              { color: 'bg-orange-500',  label: '1 – 8 mph' },
              { color: 'bg-red-500',     label: 'Stopped' },
              { color: 'bg-gray-500',    label: 'Unknown' },
            ].map((i) => (
              <div key={i.label} className="flex items-center gap-2">
                <div className={`w-2.5 h-2.5 rounded-full ${i.color}`} />
                <span className="text-gray-300 text-xs">{i.label}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Map — always rendered, vehicles empty until route selected */}
      <LiveMap
        vehicles={feed?.vehicles ?? []}
        loading={loading && !!selectedRouteId}
      />
    </div>
  )
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-gray-800 rounded-lg px-3 py-2">
      <div className="text-gray-400 text-xs">{label}</div>
      <div className="text-white font-medium text-sm">{value}</div>
    </div>
  )
}

function LiveFilters({
  routes, selectedRouteId, onSelect, feed, onRefresh,
}: {
  routes: DimRoute[]
  selectedRouteId: string | null
  onSelect: (id: string | null) => void
  feed: FeedResponse | null
  onRefresh: () => void
}) {
  // Wrap as single-select: pass array in, extract first element out
  const selected = selectedRouteId ? [selectedRouteId] : []
  function handleChange(ids: string[]) {
    // Single select — take the most recently added id
    const next = ids.find((id) => id !== selectedRouteId) ?? null
    onSelect(next)
  }

  return (
    <>
      <FilterSection label="Route (required)">
        <RouteFilter
          routes={routes}
          selected={selected}
          onChange={handleChange}
        />
        {selectedRouteId && (
          <button
            onClick={() => onSelect(null)}
            className="w-full text-xs text-gray-500 hover:text-gray-300 transition-colors mt-1"
          >
            Clear route
          </button>
        )}
      </FilterSection>

      {feed && (
        <FilterSection label="Feed">
          <button
            onClick={onRefresh}
            className="w-full flex items-center justify-center gap-2 bg-gray-800 hover:bg-gray-700 border border-gray-700 rounded-lg px-3 py-2 text-sm text-white transition-colors"
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-4 h-4">
              <path strokeLinecap="round" d="M4 4v5h5M20 20v-5h-5M4 9a8 8 0 0114.7-2.7M20 15a8 8 0 01-14.7 2.7" />
            </svg>
            Refresh now
          </button>
          <p className="text-gray-500 text-xs text-center">Auto-refreshes every 30s</p>
        </FilterSection>
      )}
    </>
  )
}
