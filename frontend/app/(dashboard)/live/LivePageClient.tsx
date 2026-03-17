'use client'

import { useState, useEffect, useCallback, useRef } from 'react'
import { useFilterPanel } from '@/lib/filter-panel-context'
import { FilterSection, RouteFilter, DirectionFilter } from '@/components/ui/FilterControls'
import LiveMap from '@/components/map/LiveMap'
import type { LiveVehicle, LiveStats } from '@/lib/queries/live-ops'
import { LIVE_VEHICLES_SQL, LIVE_STATS_SQL } from '@/lib/queries/live-ops'
import type { DimRoute } from '@/types'

const REFRESH_INTERVAL = 5 * 60 * 1000 // 5 minutes

async function dbQuery<T>(sql: string): Promise<T[]> {
  const res = await fetch('/api/databricks/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql }),
  })
  const data = await res.json()
  if (data.error) throw new Error(data.error)
  return data.rows ?? []
}

export default function LivePageClient() {
  const { setContent } = useFilterPanel()
  const [vehicles, setVehicles] = useState<LiveVehicle[]>([])
  const [stats, setStats] = useState<LiveStats | null>(null)
  const [routes, setRoutes] = useState<DimRoute[]>([])
  const [selectedRoutes, setSelectedRoutes] = useState<string[]>([])
  const [directionFilter, setDirectionFilter] = useState<0 | 1 | 'both'>('both')
  const [loading, setLoading] = useState(true)
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null)
  const [error, setError] = useState<string | null>(null)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const fetchData = useCallback(async () => {
    try {
      const [vehicleRows, statsRows] = await Promise.all([
        dbQuery<LiveVehicle>(LIVE_VEHICLES_SQL),
        dbQuery<LiveStats>(LIVE_STATS_SQL),
      ])
      setVehicles(vehicleRows)
      setStats(statsRows[0] ?? null)
      setLastRefresh(new Date())
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load live data')
    } finally {
      setLoading(false)
    }
  }, [])

  // Load routes for filter
  useEffect(() => {
    dbQuery<DimRoute>(`
      SELECT DISTINCT r.route_id, r.route_short_name, r.route_long_name, r.route_type
      FROM silver_dim_route r
      INNER JOIN silver_fact_vehicle_positions v ON r.route_id = v.route_id
      WHERE v.service_date = TO_DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'America/Phoenix'))
      ORDER BY
        CASE WHEN r.route_short_name RLIKE '^[0-9]+$' THEN CAST(r.route_short_name AS INT) ELSE 9999 END,
        r.route_short_name
    `).then(setRoutes).catch(() => {})
  }, [])

  // Initial fetch + auto-refresh
  useEffect(() => {
    fetchData()
    timerRef.current = setInterval(fetchData, REFRESH_INTERVAL)
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [fetchData])

  // Filter panel
  useEffect(() => {
    setContent(
      <LiveFilters
        routes={routes}
        selectedRoutes={selectedRoutes}
        setSelectedRoutes={setSelectedRoutes}
        direction={directionFilter}
        setDirection={setDirectionFilter}
        onRefresh={fetchData}
        lastRefresh={lastRefresh}
      />
    )
  }, [routes, selectedRoutes, directionFilter, lastRefresh, fetchData, setContent])

  const filtered = vehicles.filter((v) => {
    if (selectedRoutes.length > 0 && !selectedRoutes.includes(v.route_id ?? '')) return false
    if (directionFilter !== 'both' && v.direction_id !== directionFilter) return false
    return true
  })

  return (
    <div className="flex flex-col h-full relative">
      {/* Stats overlay */}
      {stats && (
        <div className="absolute top-4 right-4 z-10 bg-gray-900/95 border border-gray-700 rounded-xl shadow-2xl p-4 w-64 backdrop-blur-sm">
          <div className="flex items-center justify-between mb-3">
            <span className="text-white font-semibold text-sm">Live Operations</span>
            <span className="flex items-center gap-1.5">
              <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
              <span className="text-emerald-400 text-xs">Live</span>
            </span>
          </div>

          <div className="text-gray-400 text-xs mb-3">
            {filtered.length} vehicles
            {selectedRoutes.length > 0 ? ` on ${selectedRoutes.length} route(s)` : ` across ${stats.routes_active} routes`}
          </div>

          <div className="grid grid-cols-2 gap-2">
            <StatCard label="Vehicles" value={String(filtered.length)} />
            <StatCard label="Routes" value={String(stats.routes_active)} />
            {stats.avg_speed_mps != null && (
              <StatCard
                label="Avg speed"
                value={`${(stats.avg_speed_mps * 2.237).toFixed(0)} mph`}
              />
            )}
            {lastRefresh && (
              <StatCard
                label="Updated"
                value={lastRefresh.toLocaleTimeString('en-US', {
                  hour: '2-digit', minute: '2-digit', timeZone: 'America/Phoenix',
                })}
              />
            )}
          </div>

          {error && (
            <div className="mt-2 text-red-400 text-xs">{error}</div>
          )}
        </div>
      )}

      {/* Map fills remaining space */}
      {loading ? (
        <div className="flex-1 bg-gray-900 flex items-center justify-center">
          <div className="text-gray-500 text-sm animate-pulse">Loading vehicle positions…</div>
        </div>
      ) : (
        <LiveMap vehicles={filtered} />
      )}
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
  routes, selectedRoutes, setSelectedRoutes,
  direction, setDirection,
  onRefresh, lastRefresh,
}: {
  routes: DimRoute[]
  selectedRoutes: string[]
  setSelectedRoutes: (v: string[]) => void
  direction: 0 | 1 | 'both'
  setDirection: (v: 0 | 1 | 'both') => void
  onRefresh: () => void
  lastRefresh: Date | null
}) {
  return (
    <>
      <FilterSection label="Route(s)">
        <RouteFilter routes={routes} selected={selectedRoutes} onChange={setSelectedRoutes} />
      </FilterSection>

      <FilterSection label="Direction">
        <DirectionFilter value={direction} onChange={setDirection} />
      </FilterSection>

      <FilterSection label="Data">
        <button
          onClick={onRefresh}
          className="w-full flex items-center justify-center gap-2 bg-gray-800 hover:bg-gray-700 border border-gray-700 rounded-lg px-3 py-2 text-sm text-white transition-colors"
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-4 h-4">
            <path strokeLinecap="round" d="M4 4v5h5M20 20v-5h-5M4 9a8 8 0 0114.7-2.7M20 15a8 8 0 01-14.7 2.7" />
          </svg>
          Refresh now
        </button>
        {lastRefresh && (
          <p className="text-gray-500 text-xs text-center">
            Auto-refreshes every 5 min
          </p>
        )}
      </FilterSection>
    </>
  )
}
