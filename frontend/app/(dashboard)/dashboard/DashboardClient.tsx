'use client'

import { useState, useEffect, useRef } from 'react'
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts'
import Link from 'next/link'
import { useFilterPanel } from '@/lib/filter-panel-context'
import { useNav } from '@/lib/nav-context'
import { cn } from '@/lib/utils'
import { ROUTE_GROUPS_SQL, ROUTES_WITH_DATA_SQL } from '@/lib/queries/otp'

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

function toYMD(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

function getYesterday(): string {
  const d = new Date(); d.setDate(d.getDate() - 1); return toYMD(d)
}
function getSevenDaysAgo(): string {
  const d = new Date(); d.setDate(d.getDate() - 7); return toYMD(d)
}

// OTP conditions — aligned with otp.ts (use early_allowed for proper early logic)
const EARLY   = `f.arrival_delay_seconds < -60 AND COALESCE(f.early_allowed, 0) = 0`
const ON_TIME = `(f.arrival_delay_seconds BETWEEN -60 AND 360 OR (f.arrival_delay_seconds < -60 AND COALESCE(f.early_allowed, 0) = 1))`
const LATE    = `f.arrival_delay_seconds > 360`

// ── Filter state ──────────────────────────────────────────────────────────────

interface DashboardFilters {
  scope: 'all' | 'group' | 'single'
  groupName: string | null
  routeId: string | null
  timepointOnly: boolean
  excludeTerminals: boolean
}

interface RouteOption { route_id: string; route_short_name: string; route_long_name: string }

const LS_KEY = 'dashboard_defaults'
const DEFAULT_FILTERS: DashboardFilters = {
  scope: 'all', groupName: null, routeId: null, timepointOnly: false, excludeTerminals: false,
}

function loadDefaults(): DashboardFilters {
  try {
    const raw = localStorage.getItem(LS_KEY)
    if (raw) return { ...DEFAULT_FILTERS, ...JSON.parse(raw) }
  } catch {}
  return DEFAULT_FILTERS
}

// ── SQL builders ──────────────────────────────────────────────────────────────

function scopeJoin(f: DashboardFilters): string {
  if (f.scope === 'group' && f.groupName)
    return `INNER JOIN gold_route_groups grp ON f.route_id = grp.route_id AND grp.group_name = '${f.groupName.replace(/'/g, "''")}'`
  return ''
}

function scopeWhere(f: DashboardFilters): string {
  if (f.scope === 'single' && f.routeId)
    return `AND f.route_id = '${f.routeId.replace(/'/g, "''")}'`
  return ''
}

function tpWhere(f: DashboardFilters): string {
  return f.timepointOnly
    ? `AND f.stop_id IN (SELECT DISTINCT stop_id FROM silver_fact_stop_schedule WHERE timepoint = 1)`
    : ''
}

function termSnippet(f: DashboardFilters, dateClause: string) {
  if (!f.excludeTerminals) return { cte: '', join: '', where: '' }
  return {
    cte: `trip_bounds AS (
      SELECT trip_id, MIN(stop_sequence) AS first_seq, MAX(stop_sequence) AS last_seq
      FROM gold_stop_dwell_fact WHERE ${dateClause} GROUP BY trip_id
    )`,
    join: 'INNER JOIN trip_bounds tb ON f.trip_id = tb.trip_id',
    where: 'AND f.stop_sequence != tb.first_seq AND f.stop_sequence != tb.last_seq',
  }
}

function buildSummarySql(f: DashboardFilters): string {
  const t = termSnippet(f, 'service_date = :serviceDate')
  return `${t.cte ? `WITH ${t.cte}` : ''}
  SELECT
    COUNT(DISTINCT f.route_id)                                                AS route_count,
    COUNT(*)                                                                  AS total_stops,
    ROUND(AVG(CASE WHEN ${ON_TIME} THEN 1.0 ELSE 0.0 END) * 100, 1)         AS on_time_pct,
    ROUND(AVG(CASE WHEN ${EARLY}   THEN 1.0 ELSE 0.0 END) * 100, 1)         AS early_pct,
    ROUND(AVG(CASE WHEN ${LATE}    THEN 1.0 ELSE 0.0 END) * 100, 1)         AS late_pct,
    ROUND(AVG(f.arrival_delay_seconds) / 60.0, 1)                            AS avg_delay_min
  FROM gold_stop_dwell_fact f
  ${scopeJoin(f)} ${t.join}
  WHERE f.service_date = :serviceDate AND f.actual_arrival_ts IS NOT NULL
    ${scopeWhere(f)} ${tpWhere(f)} ${t.where}`
}

function buildTopDelayedSql(f: DashboardFilters): string {
  const t = termSnippet(f, 'service_date = :serviceDate')
  return `${t.cte ? `WITH ${t.cte}` : ''}
  SELECT
    f.route_id, r.route_short_name, r.route_long_name,
    COUNT(*)                                                                  AS total_stops,
    ROUND(AVG(CASE WHEN ${ON_TIME} THEN 1.0 ELSE 0.0 END) * 100, 1)         AS on_time_pct,
    ROUND(AVG(f.arrival_delay_seconds) / 60.0, 1)                            AS avg_delay_min
  FROM gold_stop_dwell_fact f
  JOIN silver_dim_route r ON f.route_id = r.route_id
  ${scopeJoin(f)} ${t.join}
  WHERE f.service_date = :serviceDate AND f.actual_arrival_ts IS NOT NULL
    ${scopeWhere(f)} ${tpWhere(f)} ${t.where}
  GROUP BY f.route_id, r.route_short_name, r.route_long_name
  ORDER BY avg_delay_min DESC LIMIT 8`
}

function buildTrendSql(f: DashboardFilters): string {
  const t = termSnippet(f, 'service_date >= :startDate AND service_date <= :endDate')
  return `${t.cte ? `WITH ${t.cte}` : ''}
  SELECT
    f.service_date,
    ROUND(AVG(CASE WHEN ${ON_TIME} THEN 1.0 ELSE 0.0 END) * 100, 1)         AS on_time_pct,
    ROUND(AVG(CASE WHEN ${EARLY}   THEN 1.0 ELSE 0.0 END) * 100, 1)         AS early_pct,
    ROUND(AVG(CASE WHEN ${LATE}    THEN 1.0 ELSE 0.0 END) * 100, 1)         AS late_pct,
    COUNT(*) AS total_stops
  FROM gold_stop_dwell_fact f
  ${scopeJoin(f)} ${t.join}
  WHERE f.service_date >= :startDate AND f.service_date <= :endDate
    AND f.actual_arrival_ts IS NOT NULL
    ${scopeWhere(f)} ${tpWhere(f)} ${t.where}
  GROUP BY f.service_date ORDER BY f.service_date`
}

// ── Main component ────────────────────────────────────────────────────────────

export default function DashboardClient() {
  const [filters, setFilters] = useState<DashboardFilters>(DEFAULT_FILTERS)
  const [groups, setGroups]   = useState<string[]>([])
  const [routes, setRoutes]   = useState<RouteOption[]>([])

  const [summary, setSummary]       = useState<Record<string, any> | null>(null)
  const [topDelayed, setTopDelayed] = useState<any[]>([])
  const [trend, setTrend]           = useState<any[]>([])
  const [loading, setLoading]       = useState(true)
  const [error, setError]           = useState<string | null>(null)

  const { setContent } = useFilterPanel()
  const setContentRef  = useRef(setContent)
  setContentRef.current = setContent
  const { setNavFilter } = useNav()

  // Read saved defaults on mount
  useEffect(() => { setFilters(loadDefaults()) }, [])

  // Fetch groups + routes once for the filter panel pickers
  useEffect(() => {
    fetchJson(ROUTE_GROUPS_SQL)
      .then((rows: any[]) => setGroups(rows.map((r) => r.group_name)))
      .catch(() => {})
    fetchJson(ROUTES_WITH_DATA_SQL)
      .then((rows: any[]) => setRoutes(rows.map((r) => ({
        route_id: r.route_id,
        route_short_name: r.route_short_name,
        route_long_name: r.route_long_name ?? '',
      })))
      ).catch(() => {})
  }, [])

  // Sync to navFilter so navigating to OTP/Dwell/Playback carries the selection
  useEffect(() => {
    setNavFilter({
      scope:           filters.scope === 'all' ? null : filters.scope,
      groupName:       filters.groupName,
      routeId:         filters.routeId,
      timepointOnly:   filters.timepointOnly,
      excludeTerminals: filters.excludeTerminals,
    })
  }, [filters, setNavFilter])

  // Inject filter panel — re-renders whenever filters, groups, or routes change
  useEffect(() => {
    setContentRef.current(
      <DashboardFilterPanel
        filters={filters}
        setFilters={setFilters}
        groups={groups}
        routes={routes}
        onSaveDefault={() => {
          try { localStorage.setItem(LS_KEY, JSON.stringify(filters)) } catch {}
        }}
      />
    )
  }, [filters, groups, routes])

  // Reload data when filters change
  useEffect(() => {
    const today   = getYesterday()
    const weekAgo = getSevenDaysAgo()
    const params  = { serviceDate: today, startDate: weekAgo, endDate: today }

    setLoading(true)
    setError(null)
    Promise.all([
      fetchJson(buildSummarySql(filters),    params),
      fetchJson(buildTopDelayedSql(filters), params),
      fetchJson(buildTrendSql(filters),      params),
    ])
      .then(([summaryRows, delayedRows, trendRows]) => {
        setSummary(summaryRows[0] ?? null)
        setTopDelayed(delayedRows)
        setTrend(trendRows)
      })
      .catch((e: any) => setError(e.message))
      .finally(() => setLoading(false))
  }, [filters]) // eslint-disable-line react-hooks/exhaustive-deps

  const today    = getYesterday()
  const otpPct   = summary ? toNum(summary.on_time_pct) : null
  const scopeLabel =
    filters.scope === 'group'  && filters.groupName ? filters.groupName
    : filters.scope === 'single' && filters.routeId
      ? `Route ${routes.find((r) => r.route_id === filters.routeId)?.route_short_name ?? filters.routeId}`
    : null

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
          <div className="flex items-center gap-2">
            <h1 className="text-base font-semibold text-white">Summary Dashboard</h1>
            {scopeLabel && (
              <span className="px-2 py-0.5 rounded-full bg-violet-600/20 text-violet-300 text-xs font-medium border border-violet-700/40">
                {scopeLabel}
              </span>
            )}
          </div>
          <p className="text-gray-500 text-xs mt-0.5">
            {loading ? 'Loading…' : `Yesterday · ${today}`}
          </p>
        </div>
        {otpPct !== null && !loading && (
          <div className="flex items-center gap-1.5">
            <span className="text-2xl font-bold tabular-nums" style={{ color: OTP_COLORS.onTime }}>
              {otpPct.toFixed(1)}%
            </span>
            <span className="text-xs text-gray-500">on-time</span>
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
              <KpiCard label="On-Time"       value={`${toNum(summary.on_time_pct).toFixed(1)}%`} color={OTP_COLORS.onTime} />
              <KpiCard label="Avg Delay"     value={`${toNum(summary.avg_delay_min) >= 0 ? '+' : ''}${toNum(summary.avg_delay_min).toFixed(1)}m`} />
              <KpiCard label="Active Routes" value={String(toNum(summary.route_count))} sub="routes observed" />
              <KpiCard label="Stop Obs."     value={toNum(summary.total_stops).toLocaleString()} sub="stop arrivals" />
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
                      <Area type="monotone" dataKey="on_time_pct" name="On-Time" stroke={OTP_COLORS.onTime} strokeWidth={2}   fill="none" dot={false} />
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
                  <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider">
                    Most Delayed Routes — Yesterday
                  </h3>
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
                      const otp   = toNum(row.on_time_pct)
                      const delay = toNum(row.avg_delay_min)
                      const otpColor   = otp   >= 80 ? OTP_COLORS.onTime : otp   >= 60 ? OTP_COLORS.late : OTP_COLORS.early
                      const delayColor = delay <= 1  ? '#9ca3af'         : delay <= 3  ? OTP_COLORS.late : OTP_COLORS.early
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

            <p className="text-xs text-gray-600 text-center pb-2">
              Use the sidebar on the left to navigate to Live Operations, OTP Analysis, Dwell, Trip Playback, and more.
            </p>
          </>
        ) : (
          <div className="flex items-center justify-center h-64 text-gray-600 text-sm">No data for yesterday</div>
        )}
      </div>
    </div>
  )
}

// ── Dashboard filter panel ────────────────────────────────────────────────────

function DashboardFilterPanel({
  filters, setFilters, groups, routes, onSaveDefault,
}: {
  filters: DashboardFilters
  setFilters: (f: DashboardFilters) => void
  groups: string[]
  routes: RouteOption[]
  onSaveDefault: () => void
}) {
  const [saved, setSaved] = useState(false)

  function update(patch: Partial<DashboardFilters>) {
    setFilters({ ...filters, ...patch })
  }

  function handleSave() {
    onSaveDefault()
    setSaved(true)
    setTimeout(() => setSaved(false), 2000)
  }

  return (
    <div className="space-y-5">
      {/* Scope */}
      <div>
        <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Scope</div>
        <div className="space-y-0.5">
          {(['all', 'group', 'single'] as const).map((s) => (
            <button
              key={s}
              onClick={() => update({ scope: s, groupName: null, routeId: null })}
              className={cn(
                'w-full text-left px-3 py-1.5 rounded-lg text-sm transition-colors',
                filters.scope === s
                  ? 'bg-violet-600 text-white'
                  : 'text-gray-400 hover:text-white hover:bg-gray-800'
              )}
            >
              {s === 'all' ? 'All Routes' : s === 'group' ? 'Route Group' : 'Single Route'}
            </button>
          ))}
        </div>
      </div>

      {/* Group picker */}
      {filters.scope === 'group' && (
        <div>
          <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Group</div>
          {groups.length === 0 ? (
            <p className="text-xs text-gray-600">Loading groups…</p>
          ) : (
            <select
              value={filters.groupName ?? ''}
              onChange={(e) => update({ groupName: e.target.value || null })}
              className="w-full bg-gray-800 text-white text-sm rounded-lg px-2 py-1.5 border border-gray-700 focus:outline-none focus:border-violet-500"
            >
              <option value="">Select group…</option>
              {groups.map((g) => <option key={g} value={g}>{g}</option>)}
            </select>
          )}
        </div>
      )}

      {/* Route picker */}
      {filters.scope === 'single' && (
        <div>
          <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Route</div>
          {routes.length === 0 ? (
            <p className="text-xs text-gray-600">Loading routes…</p>
          ) : (
            <select
              value={filters.routeId ?? ''}
              onChange={(e) => update({ routeId: e.target.value || null })}
              className="w-full bg-gray-800 text-white text-sm rounded-lg px-2 py-1.5 border border-gray-700 focus:outline-none focus:border-violet-500"
            >
              <option value="">Select route…</option>
              {routes.map((r) => (
                <option key={r.route_id} value={r.route_id}>
                  {r.route_short_name}{r.route_long_name ? ` — ${r.route_long_name}` : ''}
                </option>
              ))}
            </select>
          )}
        </div>
      )}

      {/* Stop type */}
      <div>
        <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Stop Type</div>
        <div className="flex rounded-lg overflow-hidden border border-gray-700">
          {([false, true] as const).map((tp) => (
            <button
              key={String(tp)}
              onClick={() => update({ timepointOnly: tp })}
              className={cn(
                'flex-1 py-1.5 text-xs transition-colors',
                filters.timepointOnly === tp ? 'bg-violet-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'
              )}
            >
              {tp ? 'Timepoints' : 'All Stops'}
            </button>
          ))}
        </div>
      </div>

      {/* Terminal stops */}
      <div>
        <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Terminal Stops</div>
        <div className="flex rounded-lg overflow-hidden border border-gray-700">
          {([false, true] as const).map((ex) => (
            <button
              key={String(ex)}
              onClick={() => update({ excludeTerminals: ex })}
              className={cn(
                'flex-1 py-1.5 text-xs transition-colors',
                filters.excludeTerminals === ex ? 'bg-violet-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'
              )}
            >
              {ex ? 'Exclude' : 'Include'}
            </button>
          ))}
        </div>
      </div>

      {/* Save as default */}
      <div className="pt-3 border-t border-gray-800">
        <button
          onClick={handleSave}
          className={cn(
            'w-full py-2 rounded-lg text-sm font-medium transition-colors',
            saved
              ? 'bg-green-700/40 text-green-300 border border-green-700/50'
              : 'bg-blue-600 hover:bg-blue-500 text-white'
          )}
        >
          {saved ? '✓ Saved as default' : 'Save as Default'}
        </button>
        <p className="text-xs text-gray-600 mt-1.5 text-center">Remembered in this browser</p>
      </div>
    </div>
  )
}

// ── Shared display components ─────────────────────────────────────────────────

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
