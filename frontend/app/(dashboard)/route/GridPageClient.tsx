'use client'

import { useState, useEffect, useTransition, useCallback, useRef } from 'react'
import { useFilterPanel } from '@/lib/filter-panel-context'
import {
  FilterSection,
  DateFilter,
  DirectionFilter,
  RouteFilter,
} from '@/components/ui/FilterControls'
import RouteGrid from '@/components/grid/RouteGrid'
import type { RouteMetrics15Min, DimRoute } from '@/types'

function defaultDate(): string {
  const d = new Date()
  d.setDate(d.getDate() - 1)
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

export default function GridPageClient() {
  const { setContent } = useFilterPanel()
  const setContentRef = useRef(setContent)
  setContentRef.current = setContent

  const [date, setDate] = useState(defaultDate)
  const [directionFilter, setDirectionFilter] = useState<0 | 1 | 'both'>('both')
  const [selectedRoutes, setSelectedRoutes] = useState<string[]>([])
  const [metrics, setMetrics] = useState<RouteMetrics15Min[]>([])
  const [routes, setRoutes] = useState<DimRoute[]>([])
  const [headsigns, setHeadsigns] = useState<Map<string, string>>(new Map())
  const [error, setError] = useState<string | null>(null)
  const [isPending, startTransition] = useTransition()

  // Load routes + headsigns once
  useEffect(() => {
    fetch('/api/databricks/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: `
          SELECT DISTINCT r.route_id, r.route_short_name, r.route_long_name, r.route_type, r.route_color
          FROM silver_dim_route r
          INNER JOIN gold_route_metrics_15min m ON r.route_id = m.route_id
          ORDER BY
            CASE WHEN r.route_short_name RLIKE '^[0-9]+$'
                 THEN CAST(r.route_short_name AS INT) ELSE 9999 END,
            r.route_short_name
        `,
      }),
    })
      .then((r) => r.json())
      .then((d) => setRoutes(d.rows ?? []))
      .catch(() => setError('Failed to load routes'))

    fetch('/api/databricks/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: `
          SELECT route_id, direction_id, trip_headsign AS headsign
          FROM (
            SELECT route_id, direction_id, trip_headsign, COUNT(*) AS cnt
            FROM silver_dim_trip
            WHERE trip_headsign IS NOT NULL
            GROUP BY route_id, direction_id, trip_headsign
          )
          QUALIFY ROW_NUMBER() OVER (PARTITION BY route_id, direction_id ORDER BY cnt DESC) = 1
        `,
      }),
    })
      .then((r) => r.json())
      .then((d) => {
        const map = new Map<string, string>()
        for (const row of d.rows ?? []) {
          map.set(`${row.route_id}|${row.direction_id}`, row.headsign)
        }
        setHeadsigns(map)
      })
  }, [])

  // Fetch metrics when filters change
  const fetchMetrics = useCallback(
    (d: string, rIds: string[]) => {
      setError(null)
      const routeFilter =
        rIds.length > 0
          ? `AND route_id IN (${rIds.map((r) => `'${r.replace(/'/g, "''")}'`).join(', ')})`
          : ''

      startTransition(() => {
        fetch('/api/databricks/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            sql: `
              SELECT
                service_date, route_id, direction_id, time_bucket_15min,
                trip_count,
                COALESCE(avg_delay_seconds, 0)  AS avg_delay_seconds,
                COALESCE(p90_delay_seconds, 0)  AS p90_delay_seconds,
                COALESCE(pct_on_time, 1.0)      AS pct_on_time,
                avg_dwell_delta_seconds,
                avg_late_start_seconds
              FROM gold_route_metrics_15min
              WHERE service_date = :serviceDate
              ${routeFilter}
              ORDER BY route_id, direction_id, time_bucket_15min
            `,
            params: { serviceDate: d },
          }),
        })
          .then((r) => r.json())
          .then((data) => {
            if (data.error) throw new Error(data.error)
            setMetrics(data.rows ?? [])
          })
          .catch((e) => setError(e.message))
      })
    },
    []
  )

  useEffect(() => {
    fetchMetrics(date, selectedRoutes)
  }, [date, selectedRoutes, fetchMetrics])

  // Inject filter panel content — ref avoids setContent in deps
  useEffect(() => {
    setContentRef.current(
      <GridFilters
        date={date}
        setDate={setDate}
        direction={directionFilter}
        setDirection={setDirectionFilter}
        selectedRoutes={selectedRoutes}
        setSelectedRoutes={setSelectedRoutes}
        routes={routes}
      />
    )
  }, [date, directionFilter, selectedRoutes, routes])

  return (
    <div className="flex flex-col h-full">
      {/* Page header */}
      <div className="px-6 py-4 border-b border-gray-800 flex items-center gap-3">
        <div>
          <h1 className="text-base font-semibold text-white">Route Grid</h1>
          <p className="text-gray-500 text-xs mt-0.5">
            {isPending
              ? 'Loading…'
              : metrics.length > 0
              ? `${new Set(metrics.map((m) => m.route_id)).size} routes · ${date}`
              : `No data for ${date}`}
          </p>
        </div>
      </div>

      {/* Grid */}
      <div className="flex-1 overflow-auto px-6 py-4">
        {error ? (
          <div className="text-red-400 text-sm bg-red-900/20 border border-red-800 rounded-lg px-4 py-3">
            {error}
          </div>
        ) : isPending ? (
          <GridSkeleton />
        ) : (
          <RouteGrid metrics={metrics} routes={routes} directionFilter={directionFilter} headsigns={headsigns} />
        )}
      </div>
    </div>
  )
}

// ── Filter panel content ────────────────────────────────────────────────────────
function GridFilters({
  date, setDate,
  direction, setDirection,
  selectedRoutes, setSelectedRoutes,
  routes,
}: {
  date: string
  setDate: (v: string) => void
  direction: 0 | 1 | 'both'
  setDirection: (v: 0 | 1 | 'both') => void
  selectedRoutes: string[]
  setSelectedRoutes: (v: string[]) => void
  routes: DimRoute[]
}) {
  return (
    <>
      <FilterSection label="Service Date">
        <DateFilter value={date} onChange={setDate} />
      </FilterSection>

      <FilterSection label="Direction">
        <DirectionFilter value={direction} onChange={setDirection} />
      </FilterSection>

      <FilterSection label="Route(s)">
        <RouteFilter routes={routes} selected={selectedRoutes} onChange={setSelectedRoutes} />
      </FilterSection>
    </>
  )
}

function GridSkeleton() {
  return (
    <div className="space-y-1 animate-pulse">
      {Array.from({ length: 14 }).map((_, i) => (
        <div key={i} className="flex items-center gap-px">
          <div className="w-28 h-7 bg-gray-800 rounded shrink-0 mr-2" />
          {Array.from({ length: 40 }).map((_, j) => (
            <div key={j} className="flex-1 min-w-[20px] h-7 bg-gray-800/50 rounded-sm" />
          ))}
        </div>
      ))}
    </div>
  )
}
