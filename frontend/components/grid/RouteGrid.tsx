'use client'

import { useState, useMemo } from 'react'
import GridCell from './GridCell'
import AnomalyDrawer from './AnomalyDrawer'
import { buildGridData } from '@/lib/queries/route-grid'
import type { RouteMetrics15Min, DimRoute } from '@/types'

interface Props {
  metrics: RouteMetrics15Min[]
  routes: DimRoute[]
  directionFilter: 0 | 1 | 'both'
}

export default function RouteGrid({ metrics, routes, directionFilter }: Props) {
  const [selected, setSelected] = useState<RouteMetrics15Min | null>(null)

  const routeMap = useMemo(
    () => new Map(routes.map((r) => [r.route_id, r])),
    [routes]
  )

  const filtered = useMemo(
    () =>
      directionFilter === 'both'
        ? metrics
        : metrics.filter((m) => m.direction_id === directionFilter),
    [metrics, directionFilter]
  )

  const { grid, buckets, routeDirections } = useMemo(
    () => buildGridData(filtered),
    [filtered]
  )

  // Show only every-other bucket label to avoid crowding
  const labeledBuckets = useMemo(
    () => buckets.filter((_, i) => i % 4 === 0),
    [buckets]
  )

  if (routeDirections.length === 0) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-500 text-sm">
        No data for selected date.
      </div>
    )
  }

  const selectedRoute = selected ? routeMap.get(selected.route_id) : null

  return (
    <>
      <div className="overflow-auto">
        {/* Time axis header */}
        <div className="flex items-end gap-px mb-1 pl-28 pr-2">
          {buckets.map((b) => {
            const isLabeled = labeledBuckets.includes(b)
            if (!isLabeled) return <div key={b} className="flex-1 min-w-[20px]" />
            return (
              <div key={b} className="flex-1 min-w-[20px] text-gray-500 text-[10px] truncate">
                {new Date(b).toLocaleTimeString('en-US', {
                  hour: 'numeric',
                  minute: '2-digit',
                  timeZone: 'America/Phoenix',
                  hour12: true,
                })}
              </div>
            )
          })}
        </div>

        {/* Grid rows */}
        <div className="space-y-0.5">
          {routeDirections.map((rdKey) => {
            const [routeId, dirStr] = rdKey.split('|')
            const dir = parseInt(dirStr)
            const route = routeMap.get(routeId)
            const bucketMap = grid.get(rdKey)!

            return (
              <div key={rdKey} className="flex items-center gap-px group">
                {/* Route label */}
                <div className="w-28 shrink-0 flex items-center gap-1.5 pr-2">
                  <span className="text-white text-xs font-medium tabular-nums">
                    {route?.route_short_name ?? routeId}
                  </span>
                  <span className="text-gray-600 text-[10px]">
                    {dir === 0 ? '→' : '←'}
                  </span>
                </div>

                {/* Cells */}
                {buckets.map((b) => (
                  <div key={b} className="flex-1 min-w-[20px]">
                    <GridCell
                      metrics={bucketMap.get(b)}
                      onSelect={setSelected}
                    />
                  </div>
                ))}
              </div>
            )
          })}
        </div>

        {/* Legend */}
        <div className="flex items-center gap-4 mt-4 pl-28 text-xs text-gray-400">
          <LegendItem color="bg-emerald-600/70" label="< 1 min" />
          <LegendItem color="bg-yellow-500/70" label="1–3 min" />
          <LegendItem color="bg-orange-500/70" label="3–6 min" />
          <LegendItem color="bg-red-600/70" label="> 6 min" />
          <LegendItem color="bg-gray-800/40" label="No data" />
        </div>
      </div>

      {/* Anomaly detail drawer */}
      <AnomalyDrawer
        metrics={selected}
        routeShortName={selectedRoute?.route_short_name ?? selected?.route_id ?? ''}
        onClose={() => setSelected(null)}
      />
    </>
  )
}

function LegendItem({ color, label }: { color: string; label: string }) {
  return (
    <div className="flex items-center gap-1.5">
      <div className={`w-3 h-3 rounded-sm ${color}`} />
      <span>{label}</span>
    </div>
  )
}
