'use client'

import { useState } from 'react'
import { cn, formatDelay } from '@/lib/utils'
import type { RouteMetrics15Min } from '@/types'

interface Props {
  metrics: RouteMetrics15Min | undefined
  onSelect?: (metrics: RouteMetrics15Min) => void
}

function cellColor(m: RouteMetrics15Min | undefined): string {
  if (!m || m.trip_count === 0) return 'bg-gray-800/40'
  const delay = m.avg_delay_seconds
  if (delay < 60) return 'bg-emerald-600/70 hover:bg-emerald-500/80'
  if (delay < 180) return 'bg-yellow-500/70 hover:bg-yellow-400/80'
  if (delay < 360) return 'bg-orange-500/70 hover:bg-orange-400/80'
  return 'bg-red-600/70 hover:bg-red-500/80'
}

export default function GridCell({ metrics, onSelect }: Props) {
  const [hovered, setHovered] = useState(false)

  if (!metrics) {
    return <div className="h-7 w-full rounded-sm bg-gray-800/30" />
  }

  return (
    <div
      className={cn(
        'relative h-7 w-full rounded-sm cursor-pointer transition-colors',
        cellColor(metrics)
      )}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      onClick={() => onSelect?.(metrics)}
    >
      {/* Tooltip */}
      {hovered && (
        <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 z-50 pointer-events-none">
          <div className="bg-gray-900 border border-gray-700 rounded-lg px-3 py-2 text-xs whitespace-nowrap shadow-xl">
            <div className="font-medium text-white mb-1">
              {new Date(metrics.time_bucket_15min).toLocaleTimeString('en-US', {
                hour: '2-digit',
                minute: '2-digit',
                timeZone: 'America/Phoenix',
              })}
            </div>
            <div className="space-y-0.5 text-gray-300">
              <div>Avg delay: <span className="text-white">{formatDelay(Math.round(metrics.avg_delay_seconds))}</span></div>
              <div>P90 delay: <span className="text-white">{formatDelay(Math.round(metrics.p90_delay_seconds))}</span></div>
              <div>On-time: <span className="text-white">{(metrics.pct_on_time * 100).toFixed(0)}%</span></div>
              <div>Trips: <span className="text-white">{metrics.trip_count}</span></div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
