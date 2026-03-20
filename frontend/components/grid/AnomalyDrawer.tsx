'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { formatDelay } from '@/lib/utils'
import type { RouteMetrics15Min, AnomalyEvent } from '@/types'

function replayUrl(routeId: string, serviceDate: string, directionId: number, timeBucket: string): string {
  const p = new URLSearchParams({
    routeId,
    serviceDate,
    directionId: String(directionId),
    timeBucket,
  })
  return `/trip?${p.toString()}`
}

function delayColor(seconds: number): string {
  if (seconds < 60)  return 'text-emerald-400'
  if (seconds < 180) return 'text-yellow-400'
  if (seconds < 360) return 'text-orange-400'
  return 'text-red-400'
}

function otpColor(pct: number): string {
  if (pct >= 0.80) return 'text-emerald-400'
  if (pct >= 0.60) return 'text-yellow-400'
  if (pct >= 0.40) return 'text-orange-400'
  return 'text-red-400'
}

interface Props {
  metrics: RouteMetrics15Min | null
  routeShortName: string
  routeColor: string
  headsign: string | null
  onClose: () => void
}

export default function AnomalyDrawer({ metrics, routeShortName, routeColor, headsign, onClose }: Props) {
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

  const replayHref = replayUrl(metrics.route_id, metrics.service_date, metrics.direction_id, metrics.time_bucket_15min)

  return (
    <div className="fixed inset-y-0 right-0 w-80 bg-gray-900 border-l border-gray-800 shadow-2xl z-40 flex flex-col">
      {/* Color accent bar */}
      <div className="h-1 w-full shrink-0" style={{ backgroundColor: routeColor }} />

      {/* Header */}
      <div className="flex items-start justify-between px-4 py-3 border-b border-gray-800">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <div className="w-2.5 h-2.5 rounded-full shrink-0" style={{ backgroundColor: routeColor }} />
            <span className="text-white font-semibold">Route {routeShortName}</span>
          </div>
          {headsign && (
            <div className="text-gray-300 text-xs mt-0.5 truncate pl-4" title={headsign}>
              {headsign}
            </div>
          )}
          <div className="text-gray-500 text-xs mt-0.5 pl-4">{phoenixTime}</div>
        </div>
        <button onClick={onClose} className="text-gray-500 hover:text-white transition-colors ml-3 mt-0.5 shrink-0">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-5 h-5">
            <path strokeLinecap="round" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      {/* Metrics summary */}
      <div className="px-4 py-3 border-b border-gray-800 grid grid-cols-2 gap-2">
        <Stat
          label="Avg delay"
          value={formatDelay(Math.round(metrics.avg_delay_seconds))}
          valueClass={delayColor(metrics.avg_delay_seconds)}
        />
        <Stat
          label="P90 delay"
          value={formatDelay(Math.round(metrics.p90_delay_seconds))}
          valueClass={delayColor(metrics.p90_delay_seconds)}
        />
        <Stat
          label="On-time %"
          value={`${(metrics.pct_on_time * 100).toFixed(1)}%`}
          valueClass={otpColor(metrics.pct_on_time)}
        />
        <Stat label="Trips" value={String(metrics.trip_count)} />
        {metrics.avg_dwell_delta_seconds != null && (
          <Stat
            label="Dwell delta"
            value={formatDelay(Math.round(metrics.avg_dwell_delta_seconds))}
            valueClass={delayColor(Math.abs(metrics.avg_dwell_delta_seconds))}
          />
        )}
        {metrics.avg_late_start_seconds != null && (
          <Stat
            label="Late start"
            value={formatDelay(Math.round(metrics.avg_late_start_seconds))}
            valueClass={delayColor(Math.abs(metrics.avg_late_start_seconds))}
          />
        )}
      </div>

      {/* Anomaly events */}
      <div className="flex-1 overflow-y-auto px-4 py-3">
        <div className="text-xs font-medium text-gray-400 uppercase tracking-wider mb-3">Anomaly Events</div>
        {loading ? (
          <div className="text-gray-500 text-sm">Loading…</div>
        ) : anomalies.length === 0 ? (
          <div className="rounded-lg bg-gray-800/60 border border-gray-700 px-3 py-3 text-xs text-gray-400 space-y-1">
            <div className="text-gray-300 font-medium">No anomalies flagged</div>
            <div>Anomaly detection becomes more accurate after ~28 days of baseline data. Minor deviations may not trigger alerts yet.</div>
          </div>
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
                  <span className="text-gray-400 text-xs truncate flex-1">{a.trigger_flags}</span>
                  <Link
                    href={replayUrl(a.route_id, a.service_date, a.direction_id, a.time_bucket_15min)}
                    className="text-violet-400 hover:text-violet-300 text-xs font-medium shrink-0 flex items-center gap-1 transition-colors"
                    title="Replay this time window"
                  >
                    <PlayIcon />
                    Replay
                  </Link>
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

      {/* Sticky Replay CTA */}
      <div className="px-4 py-3 border-t border-gray-800 shrink-0">
        <Link
          href={replayHref}
          className="flex items-center justify-center gap-2 w-full px-4 py-2.5 rounded-lg text-white text-sm font-semibold transition-colors"
          style={{ backgroundColor: routeColor }}
          title="Open Trip Playback for this time window"
        >
          <PlayIcon className="w-4 h-4" />
          Open in Trip Playback
        </Link>
      </div>
    </div>
  )
}

function PlayIcon({ className = 'w-3 h-3' }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" className={className}>
      <path d="M8 5v14l11-7z" />
    </svg>
  )
}

function Stat({ label, value, valueClass }: { label: string; value: string; valueClass?: string }) {
  return (
    <div className="bg-gray-800 rounded-lg px-3 py-2">
      <div className="text-gray-400 text-xs mb-0.5">{label}</div>
      <div className={`font-medium text-sm ${valueClass ?? 'text-white'}`}>{value}</div>
    </div>
  )
}
