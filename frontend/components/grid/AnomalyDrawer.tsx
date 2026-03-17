'use client'

import { useEffect, useState } from 'react'
import { formatDelay } from '@/lib/utils'
import type { RouteMetrics15Min, AnomalyEvent } from '@/types'

interface Props {
  metrics: RouteMetrics15Min | null
  routeShortName: string
  onClose: () => void
}

export default function AnomalyDrawer({ metrics, routeShortName, onClose }: Props) {
  const [anomalies, setAnomalies] = useState<AnomalyEvent[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!metrics) return
    setLoading(true)

    fetch('/api/databricks/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: `
          SELECT event_id, detected_ts, service_date, route_id, direction_id,
                 time_bucket_15min, severity, trigger_flags,
                 delay_z_score, otp_drop_pp, delay_vs_baseline_seconds
          FROM gold_anomaly_events
          WHERE service_date = :serviceDate
            AND route_id = :routeId
            AND direction_id = :directionId
          ORDER BY severity DESC
          LIMIT 5
        `,
        params: {
          serviceDate: metrics.service_date,
          routeId: metrics.route_id,
          directionId: metrics.direction_id,
        },
      }),
    })
      .then((r) => r.json())
      .then((d) => setAnomalies(d.rows ?? []))
      .finally(() => setLoading(false))
  }, [metrics])

  if (!metrics) return null

  const phoenixTime = new Date(metrics.time_bucket_15min).toLocaleString('en-US', {
    timeZone: 'America/Phoenix',
    hour: '2-digit',
    minute: '2-digit',
    month: 'short',
    day: 'numeric',
  })

  return (
    <div className="fixed inset-y-0 right-0 w-80 bg-gray-900 border-l border-gray-800 shadow-2xl z-40 flex flex-col">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-4 border-b border-gray-800">
        <div>
          <div className="text-white font-semibold">Route {routeShortName}</div>
          <div className="text-gray-400 text-xs mt-0.5">{phoenixTime} · Dir {metrics.direction_id}</div>
        </div>
        <button onClick={onClose} className="text-gray-500 hover:text-white transition-colors">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-5 h-5">
            <path strokeLinecap="round" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      {/* Metrics summary */}
      <div className="px-4 py-4 border-b border-gray-800 grid grid-cols-2 gap-3">
        <Stat label="Avg delay" value={formatDelay(Math.round(metrics.avg_delay_seconds))} />
        <Stat label="P90 delay" value={formatDelay(Math.round(metrics.p90_delay_seconds))} />
        <Stat label="On-time %" value={`${(metrics.pct_on_time * 100).toFixed(1)}%`} />
        <Stat label="Trips" value={String(metrics.trip_count)} />
        {metrics.avg_dwell_delta_seconds != null && (
          <Stat label="Dwell delta" value={formatDelay(Math.round(metrics.avg_dwell_delta_seconds))} />
        )}
        {metrics.avg_late_start_seconds != null && (
          <Stat label="Late start" value={formatDelay(Math.round(metrics.avg_late_start_seconds))} />
        )}
      </div>

      {/* Anomaly events */}
      <div className="flex-1 overflow-y-auto px-4 py-4">
        <div className="text-xs font-medium text-gray-400 uppercase tracking-wider mb-3">Anomaly Events</div>
        {loading ? (
          <div className="text-gray-500 text-sm">Loading…</div>
        ) : anomalies.length === 0 ? (
          <div className="text-gray-500 text-sm">No anomalies detected for this bucket.</div>
        ) : (
          <div className="space-y-2">
            {anomalies.map((a) => (
              <div key={a.event_id} className="bg-gray-800 rounded-lg p-3 text-sm">
                <div className="flex items-center gap-2 mb-1.5">
                  <span className={`text-xs font-medium px-1.5 py-0.5 rounded ${
                    a.severity === 'critical' ? 'bg-red-900 text-red-300' : 'bg-amber-900 text-amber-300'
                  }`}>
                    {a.severity}
                  </span>
                  <span className="text-gray-400 text-xs truncate">{a.trigger_flags}</span>
                </div>
                {a.delay_vs_baseline_seconds != null && (
                  <div className="text-gray-300 text-xs">
                    {formatDelay(Math.round(a.delay_vs_baseline_seconds))} vs baseline
                  </div>
                )}
                {a.delay_z_score != null && (
                  <div className="text-gray-400 text-xs">z-score: {a.delay_z_score.toFixed(2)}</div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-gray-800 rounded-lg px-3 py-2">
      <div className="text-gray-400 text-xs mb-0.5">{label}</div>
      <div className="text-white font-medium text-sm">{value}</div>
    </div>
  )
}
