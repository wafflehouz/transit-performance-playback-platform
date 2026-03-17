'use client'

import { useState, useEffect, useTransition } from 'react'
import RouteGrid from '@/components/grid/RouteGrid'
import type { RouteMetrics15Min, DimRoute } from '@/types'

// Default to yesterday (most recent complete service date)
function defaultDate(): string {
  const d = new Date()
  d.setDate(d.getDate() - 1)
  return d.toISOString().split('T')[0]
}

export default function GridPageClient() {
  const [date, setDate] = useState(defaultDate)
  const [directionFilter, setDirectionFilter] = useState<0 | 1 | 'both'>('both')
  const [metrics, setMetrics] = useState<RouteMetrics15Min[]>([])
  const [routes, setRoutes] = useState<DimRoute[]>([])
  const [error, setError] = useState<string | null>(null)
  const [isPending, startTransition] = useTransition()

  // Load routes once
  useEffect(() => {
    fetch('/api/databricks/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: `
          SELECT DISTINCT r.route_id, r.route_short_name, r.route_long_name, r.route_type
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
  }, [])

  // Load metrics when date changes
  useEffect(() => {
    setError(null)
    startTransition(() => {
      fetch('/api/databricks/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: `
            SELECT
              service_date,
              route_id,
              direction_id,
              time_bucket_15min,
              trip_count,
              COALESCE(avg_delay_seconds, 0)  AS avg_delay_seconds,
              COALESCE(p90_delay_seconds, 0)  AS p90_delay_seconds,
              COALESCE(pct_on_time, 1.0)      AS pct_on_time,
              avg_dwell_delta_seconds,
              avg_late_start_seconds
            FROM gold_route_metrics_15min
            WHERE service_date = :serviceDate
            ORDER BY route_id, direction_id, time_bucket_15min
          `,
          params: { serviceDate: date },
        }),
      })
        .then((r) => r.json())
        .then((d) => {
          if (d.error) throw new Error(d.error)
          setMetrics(d.rows ?? [])
        })
        .catch((e) => setError(e.message))
    })
  }, [date])

  return (
    <div className="flex flex-col h-full">
      {/* Toolbar */}
      <div className="px-6 py-4 border-b border-gray-800 flex items-center gap-4 flex-wrap">
        <div>
          <h1 className="text-lg font-semibold text-white">Route Grid</h1>
          <p className="text-gray-400 text-xs mt-0.5">
            {routes.length > 0 ? `${routes.length} routes` : 'Loading routes…'}
            {isPending ? ' · Fetching…' : metrics.length > 0 ? ` · ${metrics.length} metric rows` : ''}
          </p>
        </div>

        <div className="flex items-center gap-3 ml-auto">
          {/* Date picker */}
          <div className="flex items-center gap-2">
            <label className="text-gray-400 text-xs">Date</label>
            <input
              type="date"
              value={date}
              onChange={(e) => setDate(e.target.value)}
              className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-1.5 text-white text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>

          {/* Direction filter */}
          <div className="flex rounded-lg overflow-hidden border border-gray-700">
            {(['both', 0, 1] as const).map((d) => (
              <button
                key={String(d)}
                onClick={() => setDirectionFilter(d)}
                className={`px-3 py-1.5 text-xs font-medium transition-colors ${
                  directionFilter === d
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-800 text-gray-400 hover:text-white'
                }`}
              >
                {d === 'both' ? 'Both dirs' : d === 0 ? 'Outbound' : 'Inbound'}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Grid */}
      <div className="flex-1 overflow-auto px-6 py-4">
        {error ? (
          <div className="text-red-400 text-sm bg-red-900/20 border border-red-800 rounded-lg px-4 py-3">
            {error}
          </div>
        ) : isPending ? (
          <div className="space-y-1 animate-pulse">
            {Array.from({ length: 12 }).map((_, i) => (
              <div key={i} className="flex items-center gap-px">
                <div className="w-28 h-7 bg-gray-800 rounded shrink-0 mr-2" />
                {Array.from({ length: 40 }).map((_, j) => (
                  <div key={j} className="flex-1 min-w-[20px] h-7 bg-gray-800/50 rounded-sm" />
                ))}
              </div>
            ))}
          </div>
        ) : (
          <RouteGrid metrics={metrics} routes={routes} directionFilter={directionFilter} />
        )}
      </div>
    </div>
  )
}
