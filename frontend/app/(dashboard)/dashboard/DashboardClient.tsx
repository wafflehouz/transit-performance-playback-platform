'use client'

import { useState, useEffect } from 'react'
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts'
import Link from 'next/link'

const OTP_COLORS = { early: '#e57373', onTime: '#4db6ac', late: '#ffb74d' }

function toNum(v: any): number {
  const n = Number(v)
  return isNaN(n) ? 0 : n
}

async function fetchJson(sql: string, params: Record<string, string> = {}) {
  const res = await fetch('/api/databricks/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, params }),
  })
  const data = await res.json()
  if (data.error) throw new Error(data.error)
  return data.rows ?? []
}

function yesterday(): string {
  const d = new Date()
  d.setDate(d.getDate() - 1)
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

function sevenDaysAgo(): string {
  const d = new Date()
  d.setDate(d.getDate() - 7)
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

const EARLY   = `arrival_delay_seconds < -60 AND COALESCE(pickup_type, 0) = 0`
const ON_TIME = `(arrival_delay_seconds BETWEEN -60 AND 360 OR (arrival_delay_seconds < -60 AND COALESCE(pickup_type, 0) = 1))`
const LATE    = `arrival_delay_seconds > 360`

const SUMMARY_SQL = `
  SELECT
    COUNT(DISTINCT route_id)                                                AS route_count,
    COUNT(*)                                                                AS total_stops,
    ROUND(AVG(CASE WHEN ${ON_TIME} THEN 1.0 ELSE 0.0 END) * 100, 1)       AS on_time_pct,
    ROUND(AVG(CASE WHEN ${EARLY}   THEN 1.0 ELSE 0.0 END) * 100, 1)       AS early_pct,
    ROUND(AVG(CASE WHEN ${LATE}    THEN 1.0 ELSE 0.0 END) * 100, 1)       AS late_pct,
    ROUND(AVG(arrival_delay_seconds) / 60.0, 1)                            AS avg_delay_min
  FROM gold_stop_dwell_fact
  WHERE service_date = :serviceDate
    AND actual_arrival_ts IS NOT NULL
`

const TOP_DELAYED_SQL = `
  SELECT
    f.route_id,
    r.route_short_name,
    r.route_long_name,
    COUNT(*)                                                                AS total_stops,
    ROUND(AVG(CASE WHEN ${ON_TIME} THEN 1.0 ELSE 0.0 END) * 100, 1)       AS on_time_pct,
    ROUND(AVG(arrival_delay_seconds) / 60.0, 1)                            AS avg_delay_min
  FROM gold_stop_dwell_fact f
  JOIN silver_dim_route r ON f.route_id = r.route_id
  WHERE f.service_date = :serviceDate
    AND f.actual_arrival_ts IS NOT NULL
  GROUP BY f.route_id, r.route_short_name, r.route_long_name
  ORDER BY avg_delay_min DESC
  LIMIT 8
`

const TREND_SQL = `
  SELECT
    service_date,
    ROUND(AVG(CASE WHEN ${ON_TIME} THEN 1.0 ELSE 0.0 END) * 100, 1) AS on_time_pct,
    ROUND(AVG(CASE WHEN ${EARLY}   THEN 1.0 ELSE 0.0 END) * 100, 1) AS early_pct,
    ROUND(AVG(CASE WHEN ${LATE}    THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_pct,
    COUNT(*) AS total_stops
  FROM gold_stop_dwell_fact
  WHERE service_date >= :startDate
    AND service_date <= :endDate
    AND actual_arrival_ts IS NOT NULL
  GROUP BY service_date
  ORDER BY service_date
`

export default function DashboardClient() {
  const today = yesterday()
  const weekAgo = sevenDaysAgo()

  const [summary, setSummary] = useState<Record<string, any> | null>(null)
  const [topDelayed, setTopDelayed] = useState<any[]>([])
  const [trend, setTrend] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    async function load() {
      setLoading(true)
      setError(null)
      try {
        const [summaryRows, delayedRows, trendRows] = await Promise.all([
          fetchJson(SUMMARY_SQL, { serviceDate: today }),
          fetchJson(TOP_DELAYED_SQL, { serviceDate: today }),
          fetchJson(TREND_SQL, { startDate: weekAgo, endDate: today }),
        ])
        setSummary(summaryRows[0] ?? null)
        setTopDelayed(delayedRows)
        setTrend(trendRows)
      } catch (e: any) {
        setError(e.message)
      } finally {
        setLoading(false)
      }
    }
    load()
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const otpPct = summary ? toNum(summary.on_time_pct) : null

  const trendFormatted = trend.map((r) => ({
    on_time_pct: toNum(r.on_time_pct),
    early_pct:   toNum(r.early_pct),
    late_pct:    toNum(r.late_pct),
    label: new Date(r.service_date + 'T12:00:00Z').toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
  }))

  return (
    <div className="flex flex-col h-full overflow-auto">
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-800 flex items-center justify-between shrink-0">
        <div>
          <h1 className="text-base font-semibold text-white">Summary Dashboard</h1>
          <p className="text-gray-500 text-xs mt-0.5">
            {loading ? 'Loading…' : `Yesterday · ${today}`}
          </p>
        </div>
        {otpPct !== null && !loading && (
          <div className="flex items-center gap-1.5">
            <span className="text-2xl font-bold tabular-nums" style={{ color: OTP_COLORS.onTime }}>
              {otpPct.toFixed(1)}%
            </span>
            <span className="text-xs text-gray-500 leading-tight">on-<br />time</span>
          </div>
        )}
      </div>

      {/* Content */}
      <div className="flex-1 p-6 space-y-6">
        {error && (
          <div className="text-red-400 text-sm bg-red-900/20 border border-red-800 rounded-lg px-4 py-3">
            {error}
          </div>
        )}

        {loading ? (
          <LoadingSkeleton />
        ) : summary ? (
          <>
            {/* KPI row */}
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
              <KpiCard label="On-Time"      value={`${toNum(summary.on_time_pct).toFixed(1)}%`} color={OTP_COLORS.onTime} />
              <KpiCard label="Avg Delay"    value={`${toNum(summary.avg_delay_min) >= 0 ? '+' : ''}${toNum(summary.avg_delay_min).toFixed(1)}m`} />
              <KpiCard label="Active Routes" value={String(toNum(summary.route_count))} sub="routes observed" />
              <KpiCard label="Stop Obs."    value={toNum(summary.total_stops).toLocaleString()} sub="stop arrivals" />
            </div>

            {/* Charts row */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              {/* OTP breakdown */}
              <div className="bg-gray-900 rounded-xl border border-gray-800 p-5">
                <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-4">OTP Breakdown</h3>
                <div className="space-y-3">
                  {[
                    { label: 'On-Time', value: toNum(summary.on_time_pct), color: OTP_COLORS.onTime },
                    { label: 'Early',   value: toNum(summary.early_pct),   color: OTP_COLORS.early  },
                    { label: 'Late',    value: toNum(summary.late_pct),    color: OTP_COLORS.late   },
                  ].map((item) => (
                    <div key={item.label}>
                      <div className="flex justify-between text-xs mb-1">
                        <span className="text-gray-400">{item.label}</span>
                        <span className="font-semibold tabular-nums" style={{ color: item.color }}>{item.value.toFixed(1)}%</span>
                      </div>
                      <div className="h-2 bg-gray-800 rounded-full overflow-hidden">
                        <div className="h-full rounded-full" style={{ width: `${item.value}%`, background: item.color }} />
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* 7-day trend */}
              <div className="lg:col-span-2 bg-gray-900 rounded-xl border border-gray-800 p-5">
                <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-4">7-Day OTP Trend</h3>
                {trendFormatted.length === 0 ? (
                  <div className="flex items-center justify-center h-32 text-gray-700 text-xs">No trend data</div>
                ) : (
                  <ResponsiveContainer width="100%" height={140}>
                    <AreaChart data={trendFormatted} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                      <XAxis dataKey="label" tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} axisLine={false} />
                      <YAxis domain={[0, 100]} tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} axisLine={false} tickFormatter={(v) => `${v}%`} />
                      <Tooltip
                        contentStyle={{ background: '#111827', border: '1px solid #374151', borderRadius: 8, fontSize: 12 }}
                        labelStyle={{ color: '#9ca3af' }}
                        formatter={(v: any, name: any) => [`${(v as number).toFixed(1)}%`, name]}
                      />
                      <Area type="monotone" dataKey="on_time_pct" name="On-Time" stroke={OTP_COLORS.onTime} strokeWidth={2} fill="none" dot={false} />
                      <Area type="monotone" dataKey="early_pct"   name="Early"   stroke={OTP_COLORS.early}  strokeWidth={1.5} fill="none" dot={false} strokeDasharray="4 2" />
                      <Area type="monotone" dataKey="late_pct"    name="Late"    stroke={OTP_COLORS.late}   strokeWidth={1.5} fill="none" dot={false} strokeDasharray="4 2" />
                      <Legend wrapperStyle={{ fontSize: 11, color: '#9ca3af' }} />
                    </AreaChart>
                  </ResponsiveContainer>
                )}
              </div>
            </div>

            {/* Top delayed routes */}
            {topDelayed.length > 0 && (
              <div className="bg-gray-900 rounded-xl border border-gray-800 overflow-hidden">
                <div className="px-4 py-3 border-b border-gray-800 flex items-center justify-between">
                  <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider">Most Delayed Routes — Yesterday</h3>
                  <Link href="/otp" className="text-xs text-blue-400 hover:text-blue-300 transition-colors">
                    View OTP →
                  </Link>
                </div>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-800">
                      <th className="text-left px-4 py-2 text-xs font-medium text-gray-500">Route</th>
                      <th className="text-right px-4 py-2 text-xs font-medium text-gray-500">On-Time</th>
                      <th className="text-right px-4 py-2 text-xs font-medium text-gray-500">Avg Delay</th>
                      <th className="px-4 py-2 text-xs font-medium text-gray-500">OTP</th>
                      <th className="text-right px-4 py-2 text-xs font-medium text-gray-500">Stops</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topDelayed.map((row, i) => {
                      const otp = toNum(row.on_time_pct)
                      const delay = toNum(row.avg_delay_min)
                      const otpColor = otp >= 80 ? OTP_COLORS.onTime : otp >= 60 ? OTP_COLORS.late : OTP_COLORS.early
                      const delayColor = delay <= 1 ? '#9ca3af' : delay <= 3 ? OTP_COLORS.late : OTP_COLORS.early
                      return (
                        <tr key={i} className="border-b border-gray-800/50 hover:bg-gray-800/30 transition-colors">
                          <td className="px-4 py-2.5">
                            <div className="text-white font-medium text-sm">{row.route_short_name}</div>
                            <div className="text-gray-500 text-xs truncate max-w-[200px]">{row.route_long_name}</div>
                          </td>
                          <td className="px-4 py-2.5 text-right tabular-nums font-semibold text-sm" style={{ color: otpColor }}>
                            {otp.toFixed(1)}%
                          </td>
                          <td className="px-4 py-2.5 text-right tabular-nums text-sm" style={{ color: delayColor }}>
                            {delay >= 0 ? '+' : ''}{delay.toFixed(1)}m
                          </td>
                          <td className="px-4 py-2.5 w-28">
                            <div className="h-1.5 bg-gray-800 rounded-full overflow-hidden">
                              <div className="h-full rounded-full" style={{ width: `${otp}%`, background: otpColor }} />
                            </div>
                          </td>
                          <td className="px-4 py-2.5 text-right text-gray-500 tabular-nums text-xs">
                            {toNum(row.total_stops).toLocaleString()}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}

            {/* Quick links */}
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
              {[
                { href: '/live',   label: 'Live Operations', desc: 'Real-time vehicle positions',  icon: '📡' },
                { href: '/otp',    label: 'OTP Analysis',    desc: 'Stop-level on-time detail',    icon: '📊' },
                { href: '/dwell',  label: 'Dwell Analysis',  desc: 'Stop dwell time breakdown',    icon: '📈' },
                { href: '/trip',   label: 'Trip Playback',   desc: 'Replay individual trips',       icon: '▶️' },
                { href: '/route',  label: 'Route Metrics',   desc: '15-min route performance grid', icon: '🗂️' },
                { href: '/anomaly',label: 'Anomaly Monitor', desc: 'Deviation alerts',              icon: '⚠️' },
              ].map(({ href, label, desc, icon }) => (
                <Link
                  key={href}
                  href={href}
                  className="bg-gray-900 border border-gray-800 rounded-xl p-4 hover:border-gray-700 hover:bg-gray-800/50 transition-colors group"
                >
                  <div className="text-xl mb-2">{icon}</div>
                  <div className="text-sm font-medium text-white group-hover:text-blue-300 transition-colors">{label}</div>
                  <div className="text-xs text-gray-500 mt-0.5">{desc}</div>
                </Link>
              ))}
            </div>
          </>
        ) : (
          <div className="flex items-center justify-center h-64 text-gray-600 text-sm">No data for yesterday</div>
        )}
      </div>
    </div>
  )
}

function KpiCard({ label, value, sub, color }: { label: string; value: string; sub?: string; color?: string }) {
  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 px-4 py-4">
      <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1">{label}</div>
      <div className="text-2xl font-bold tabular-nums" style={color ? { color } : { color: '#f9fafb' }}>{value}</div>
      {sub && <div className="text-xs text-gray-500 mt-0.5">{sub}</div>}
    </div>
  )
}

function LoadingSkeleton() {
  return (
    <div className="space-y-4 animate-pulse">
      <div className="grid grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => <div key={i} className="h-20 bg-gray-800 rounded-xl" />)}
      </div>
      <div className="grid grid-cols-3 gap-4">
        <div className="h-44 bg-gray-800 rounded-xl" />
        <div className="lg:col-span-2 h-44 bg-gray-800 rounded-xl" />
      </div>
      <div className="h-64 bg-gray-800 rounded-xl" />
    </div>
  )
}
