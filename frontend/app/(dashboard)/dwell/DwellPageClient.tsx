'use client'

import { useState, useEffect, useCallback, useRef, useMemo } from 'react'
import {
  BarChart, Bar, AreaChart, Area,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, Cell,
} from 'recharts'
import { useFilterPanel } from '@/lib/filter-panel-context'
import RouteFilterPanel, {
  type OtpFilterState,
  type DatePreset,
  resolveDates,
} from '@/components/filters/RouteFilterPanel'
import type { DimRoute } from '@/types'
import {
  DWELL_ROUTES_SQL, DWELL_GROUPS_SQL,
  dwellSummarySql, dwellBucketSql, topInflationStopsSql, dwellByRouteSql,
  stopProfileSql, tripMatrixSql, dwellTodSql, dwellTrendSql,
} from '@/lib/queries/dwell'
import { cn } from '@/lib/utils'

// ── Filter state ───────────────────────────────────────────────────────────────

export interface DwellFilterState extends OtpFilterState {
  excludeTerminals: boolean
}

// ── Dwell severity color scale ─────────────────────────────────────────────────

const BUCKET_COLORS: Record<string, string> = {
  'Fast (<15s)':       '#4b5563',
  'Normal (15-30s)':   '#4ade80',
  'Slow (30-60s)':     '#facc15',
  'High Pax (60-120s)':'#fb923c',
  'Outlier (120s+)':   '#ef4444',
}
const BUCKET_ORDER = ['Fast (<15s)', 'Normal (15-30s)', 'Slow (30-60s)', 'High Pax (60-120s)', 'Outlier (120s+)']

function cellColor(sec: number | null): string {
  if (sec === null) return '#111827'
  if (sec < 15)  return '#374151'  // gray-700: barely stopped
  if (sec < 30)  return '#15803d'  // green-700: normal dwell
  if (sec < 60)  return '#a16207'  // yellow-700: slow boarding
  if (sec < 120) return '#c2410c'  // orange-700: high pax demand
  return '#b91c1c'                 // red-700: outlier / wheelchair / incident
}

function p90Color(sec: number): string {
  if (sec <= 30)  return '#34d399'
  if (sec <= 60)  return '#fbbf24'
  if (sec <= 120) return '#fb923c'
  return '#f87171'
}

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

// ── Tabs ───────────────────────────────────────────────────────────────────────

type TabId = 'summary' | 'by-route' | 'stop-profile' | 'trip-matrix' | 'time-of-day' | 'trend'

const ALL_TABS: { id: TabId; label: string; modes: DwellFilterState['mode'][] }[] = [
  { id: 'summary',      label: 'Summary',      modes: ['all', 'group', 'single'] },
  { id: 'by-route',     label: 'By Route',     modes: ['all', 'group'] },
  { id: 'stop-profile', label: 'Stop Profile', modes: ['single'] },
  { id: 'trip-matrix',  label: 'Trip Matrix',  modes: ['single'] },
  { id: 'time-of-day',  label: 'Time of Day',  modes: ['all', 'group', 'single'] },
  { id: 'trend',        label: 'Trend',        modes: ['all', 'group', 'single'] },
]

// ── Main component ─────────────────────────────────────────────────────────────

export default function DwellPageClient() {
  const { setContent } = useFilterPanel()
  const setContentRef = useRef(setContent)
  setContentRef.current = setContent

  const [preset, setPreset] = useState<DatePreset>('7d')
  const [filters, setFilters] = useState<DwellFilterState>(() => ({
    mode: 'all',
    groupName: null,
    routeId: null,
    ...resolveDates('7d'),
    direction: 'both',
    timepointOnly: false,
    excludeTerminals: false,
  }))
  const [activeTab, setActiveTab] = useState<TabId>('summary')

  const [routes, setRoutes] = useState<DimRoute[]>([])
  const [groups, setGroups] = useState<string[]>([])
  const [summaryData, setSummaryData] = useState<Record<string, any> | null>(null)
  const [bucketData, setBucketData] = useState<any[]>([])
  const [topStopsData, setTopStopsData] = useState<any[]>([])
  const [byRouteData, setByRouteData] = useState<any[]>([])
  const [profileData, setProfileData] = useState<any[]>([])
  const [matrixData, setMatrixData] = useState<any[]>([])
  const [todData, setTodData] = useState<any[]>([])
  const [trendData, setTrendData] = useState<any[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const availableTabs = ALL_TABS.filter((t) => t.modes.includes(filters.mode))
  useEffect(() => {
    if (!availableTabs.find((t) => t.id === activeTab)) setActiveTab('summary')
  }, [filters.mode]) // eslint-disable-line react-hooks/exhaustive-deps

  // Static data once
  useEffect(() => {
    fetchJson(DWELL_ROUTES_SQL).then(setRoutes).catch(() => {})
    fetchJson(DWELL_GROUPS_SQL)
      .then((rows: any[]) => setGroups(rows.map((r) => r.group_name)))
      .catch(() => {})
  }, [])

  function handlePresetChange(p: DatePreset) {
    setPreset(p)
    setFilters((f) => ({ ...f, ...resolveDates(p) }))
  }

  const fetchData = useCallback(async (f: DwellFilterState) => {
    setLoading(true)
    setError(null)
    setSummaryData(null)
    setBucketData([])
    setTopStopsData([])
    setByRouteData([])
    setProfileData([])
    setMatrixData([])
    setTodData([])
    setTrendData([])

    const params = { startDate: f.startDate, endDate: f.endDate }
    const errors: string[] = []

    async function safe(p: Promise<any[]>): Promise<any[]> {
      try { return await p } catch (e: any) { errors.push(e.message); return [] }
    }

    const { mode, groupName, routeId, direction, timepointOnly, excludeTerminals } = f

    // Trend always uses 30-day window anchored at endDate
    const trendParams = { startDate: f.endDate, endDate: f.endDate }

    if (mode === 'single' && routeId) {
      const [summary, buckets, topStops, profile, matrix, tod, trend] = await Promise.all([
        safe(fetchJson(dwellSummarySql(null, routeId, direction, timepointOnly, excludeTerminals), params)),
        safe(fetchJson(dwellBucketSql(null, routeId, direction, timepointOnly, excludeTerminals), params)),
        safe(fetchJson(topInflationStopsSql(null, routeId, direction, timepointOnly, excludeTerminals, 15), params)),
        safe(fetchJson(stopProfileSql(routeId, direction, excludeTerminals), params)),
        safe(fetchJson(tripMatrixSql(routeId, direction, excludeTerminals), { startDate: f.endDate, endDate: f.endDate })),
        safe(fetchJson(dwellTodSql(null, routeId, direction, excludeTerminals), params)),
        safe(fetchJson(dwellTrendSql(null, routeId, excludeTerminals), trendParams)),
      ])
      setSummaryData(summary[0] ?? null)
      setBucketData(buckets)
      setTopStopsData(topStops)
      setProfileData(profile)
      setMatrixData(matrix)
      setTodData(tod)
      setTrendData(trend)
    } else {
      const [summary, buckets, topStops, byRoute, tod, trend] = await Promise.all([
        safe(fetchJson(dwellSummarySql(groupName, null, direction, timepointOnly, excludeTerminals), params)),
        safe(fetchJson(dwellBucketSql(groupName, null, direction, timepointOnly, excludeTerminals), params)),
        safe(fetchJson(topInflationStopsSql(groupName, null, direction, timepointOnly, excludeTerminals, 15), params)),
        safe(fetchJson(dwellByRouteSql(groupName, direction, timepointOnly, excludeTerminals), params)),
        safe(fetchJson(dwellTodSql(groupName, null, direction, excludeTerminals), params)),
        safe(fetchJson(dwellTrendSql(groupName, null, excludeTerminals), trendParams)),
      ])
      setSummaryData(summary[0] ?? null)
      setBucketData(buckets)
      setTopStopsData(topStops)
      setByRouteData(byRoute)
      setTodData(tod)
      setTrendData(trend)
    }

    if (errors.length > 0) setError(errors[0])
    setLoading(false)
  }, [])

  useEffect(() => {
    if (filters.mode === 'single' && !filters.routeId) return
    if (filters.mode === 'group' && !filters.groupName) return
    fetchData(filters)
  }, [filters, fetchData])

  // Inject filter panel
  const filterNode = useMemo(
    () => (
      <RouteFilterPanel
        filters={filters}
        onChange={(f) => setFilters((prev) => ({ ...prev, ...f }))}
        routes={routes}
        groups={groups}
        activePreset={preset}
        onPresetChange={handlePresetChange}
        excludeTerminals={filters.excludeTerminals}
        onExcludeTerminalsChange={(v) => setFilters((f) => ({ ...f, excludeTerminals: v }))}
      />
    ),
    [filters, routes, groups, preset] // eslint-disable-line react-hooks/exhaustive-deps
  )
  useEffect(() => { setContentRef.current(filterNode) }, [filterNode])

  const scopeLabel = useMemo(() => {
    if (filters.mode === 'group') return filters.groupName ?? 'Select a group'
    if (filters.mode === 'single') {
      const r = routes.find((r) => r.route_id === filters.routeId)
      return r ? `Route ${r.route_short_name}` : 'Select a route'
    }
    return 'All Routes'
  }, [filters, routes])

  const dateLabel = filters.startDate === filters.endDate
    ? filters.endDate
    : `${filters.startDate} → ${filters.endDate}`

  const needsSelection =
    (filters.mode === 'group' && !filters.groupName) ||
    (filters.mode === 'single' && !filters.routeId)

  const p90 = summaryData ? toNum(summaryData.p90_actual_sec) : null

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-800 flex items-center justify-between shrink-0">
        <div>
          <h1 className="text-base font-semibold text-white">Dwell Analysis</h1>
          <p className="text-gray-500 text-xs mt-0.5">
            {loading ? 'Loading…' : `${scopeLabel} · ${dateLabel}`}
          </p>
        </div>
        {p90 !== null && !loading && (
          <div className="flex items-center gap-1.5">
            <span className="text-2xl font-bold tabular-nums" style={{ color: p90Color(p90) }}>
              {p90}s
            </span>
            <span className="text-xs text-gray-500 leading-tight">p90<br />dwell</span>
          </div>
        )}
      </div>

      {/* Tab nav */}
      <div className="px-6 border-b border-gray-800 flex gap-0 shrink-0">
        {availableTabs.map((t) => (
          <button
            key={t.id}
            onClick={() => setActiveTab(t.id)}
            className={cn(
              'px-4 py-3 text-sm font-medium transition-colors border-b-2 -mb-px',
              activeTab === t.id
                ? 'text-white border-violet-500'
                : 'text-gray-500 border-transparent hover:text-gray-300'
            )}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto">
        {error && (
          <div className="mx-6 mt-4 text-red-400 text-sm bg-red-900/20 border border-red-800 rounded-lg px-4 py-3">
            {error}
          </div>
        )}

        {needsSelection ? (
          <div className="flex items-center justify-center h-64 text-gray-600 text-sm">
            {filters.mode === 'group' ? 'Select a route group to view data' : 'Select a route to view data'}
          </div>
        ) : loading ? (
          <LoadingSkeleton />
        ) : (
          <>
            {activeTab === 'summary' && (
              <SummaryTab summary={summaryData} buckets={bucketData} topStops={topStopsData} mode={filters.mode} />
            )}
            {activeTab === 'by-route' && (
              <ByRouteTab rows={byRouteData} />
            )}
            {activeTab === 'stop-profile' && (
              <StopProfileTab profileData={profileData} direction={filters.direction} />
            )}
            {activeTab === 'trip-matrix' && (
              <div className="p-6">
                <TripMatrix data={matrixData} endDate={filters.endDate} />
              </div>
            )}
            {activeTab === 'time-of-day' && (
              <TimeOfDayTab data={todData} />
            )}
            {activeTab === 'trend' && (
              <TrendTab data={trendData} endDate={filters.endDate} />
            )}
          </>
        )}
      </div>
    </div>
  )
}

// ── Summary tab ────────────────────────────────────────────────────────────────

function SummaryTab({
  summary, buckets, topStops, mode,
}: {
  summary: Record<string, any> | null
  buckets: any[]
  topStops: any[]
  mode: DwellFilterState['mode']
}) {
  if (!summary) return <EmptyState />

  const p50 = toNum(summary.p50_actual_sec)
  const p90 = toNum(summary.p90_actual_sec)
  const avgDelta = toNum(summary.avg_delta_sec)
  const over2min = toNum(summary.stops_over_2min)
  const fourthLabel = mode === 'single' ? 'Observations' : 'Routes'
  const fourthValue = mode === 'single'
    ? toNum(summary.observation_count).toLocaleString()
    : String(toNum(summary.route_count))

  const bucketsSorted = BUCKET_ORDER
    .map((name) => {
      const row = buckets.find((b) => b.dwell_bucket === name)
      return { name, events: row ? toNum(row.stop_events) : 0, pct: row ? toNum(row.pct_of_total) : 0 }
    })
    .filter((b) => b.events > 0)

  const topSorted = [...topStops].slice(0, 12)

  return (
    <div className="p-6 space-y-6">
      {/* KPI row */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <KpiCard label="p50 Dwell"     value={`${p50}s`}  color={p90Color(p50)} sub="median actual" />
        <KpiCard label="p90 Dwell"     value={`${p90}s`}  color={p90Color(p90)} sub="90th percentile" />
        <KpiCard label="Avg Delta"     value={`${avgDelta >= 0 ? '+' : ''}${avgDelta}s`} sub="vs scheduled" />
        <KpiCard label={fourthLabel}   value={fourthValue} />
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Bucket distribution */}
        <div className="bg-gray-900 rounded-xl border border-gray-800 p-5">
          <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-4">Dwell Distribution</h3>
          {bucketsSorted.length === 0 ? <EmptyChart /> : (
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={bucketsSorted} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="name" tick={{ fill: '#6b7280', fontSize: 9 }} tickLine={false} axisLine={false} interval={0} />
                <YAxis tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} axisLine={false} />
                <Tooltip
                  contentStyle={{ background: '#111827', border: '1px solid #374151', borderRadius: 8, fontSize: 12 }}
                  labelStyle={{ color: '#9ca3af' }}
                  formatter={(v: any, _: any, entry: any) => [`${toNum(entry.payload.pct).toFixed(1)}%  (${Number(v).toLocaleString()} events)`, '']}
                />
                <Bar dataKey="events" radius={[3, 3, 0, 0]} maxBarSize={40}>
                  {bucketsSorted.map((b, i) => (
                    <Cell key={i} fill={BUCKET_COLORS[b.name] ?? '#6b7280'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
          {/* Legend */}
          <div className="flex flex-wrap gap-x-3 gap-y-1 mt-3">
            {bucketsSorted.map((b) => (
              <span key={b.name} className="flex items-center gap-1 text-xs text-gray-400">
                <span className="w-2 h-2 rounded-sm shrink-0" style={{ background: BUCKET_COLORS[b.name] ?? '#6b7280' }} />
                {b.name}
              </span>
            ))}
          </div>
        </div>

        {/* Top inflation stops */}
        <div className="bg-gray-900 rounded-xl border border-gray-800 p-5">
          <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1">
            Top Stops by Dwell Inflation
          </h3>
          <p className="text-xs text-gray-600 mb-3">Avg actual dwell, descending</p>
          {topSorted.length === 0 ? <EmptyChart /> : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart
                data={topSorted}
                layout="vertical"
                margin={{ top: 0, right: 40, left: 0, bottom: 0 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" horizontal={false} />
                <XAxis type="number" tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} axisLine={false}
                  tickFormatter={(v) => `${v}s`} />
                <YAxis type="category" dataKey="stop_name" width={130}
                  tick={{ fill: '#9ca3af', fontSize: 9 }}
                  tickLine={false} axisLine={false}
                  tickFormatter={(v: string) => v.length > 18 ? v.slice(0, 18) + '…' : v}
                />
                <Tooltip
                  contentStyle={{ background: '#111827', border: '1px solid #374151', borderRadius: 8, fontSize: 12 }}
                  labelStyle={{ color: '#9ca3af' }}
                  formatter={(v: any, name: any) => [`${v}s`, name === 'avg_actual_sec' ? 'Avg Actual' : 'p90']}
                />
                <Bar dataKey="avg_actual_sec" name="Avg Actual" fill="#a78bfa" radius={[0, 3, 3, 0]} maxBarSize={14} />
                <Bar dataKey="p90_actual_sec" name="p90" fill="#f59e0b" radius={[0, 3, 3, 0]} maxBarSize={14} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* Stops over 2min callout */}
      {over2min > 0 && (
        <div className="flex items-center gap-3 bg-orange-900/20 border border-orange-800/50 rounded-xl px-5 py-3">
          <span className="text-orange-400 text-xl font-bold tabular-nums">{over2min.toLocaleString()}</span>
          <span className="text-orange-300 text-sm">
            stop events exceeded <strong>2 minutes</strong> of dwell — potential wheelchair boarding, crowding, or schedule gap
          </span>
        </div>
      )}
    </div>
  )
}

// ── By Route tab ───────────────────────────────────────────────────────────────

function ByRouteTab({ rows }: { rows: any[] }) {
  if (rows.length === 0) return <EmptyState />

  return (
    <div className="p-6">
      <div className="bg-gray-900 rounded-xl border border-gray-800 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800">
              <th className="text-left px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider">Route</th>
              <th className="text-right px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider">Obs.</th>
              <th className="text-right px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider">p50 Dwell</th>
              <th className="text-right px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider">p90 Dwell</th>
              <th className="text-right px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider">Avg Delta</th>
              <th className="text-right px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider">&gt;2 min</th>
              <th className="px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider w-24">p90 Bar</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => {
              const p90 = toNum(row.p90_actual_sec)
              const avg = toNum(row.avg_actual_sec)
              const delta = toNum(row.avg_delta_sec)
              const over2 = toNum(row.stops_over_2min)
              const total = toNum(row.total_stops)
              const pctOver2 = total > 0 ? (over2 / total) * 100 : 0
              // bar width: p90 as fraction of a 180s cap
              const barPct = Math.min((p90 / 180) * 100, 100)
              return (
                <tr key={i} className="border-b border-gray-800/50 hover:bg-gray-800/30 transition-colors">
                  <td className="px-4 py-3">
                    <div className="text-white font-medium">{row.route_short_name}</div>
                    <div className="text-gray-500 text-xs truncate max-w-[180px]">{row.route_long_name}</div>
                  </td>
                  <td className="px-4 py-3 text-right text-gray-500 tabular-nums text-xs">{total.toLocaleString()}</td>
                  <td className="px-4 py-3 text-right tabular-nums" style={{ color: p90Color(avg) }}>{avg}s</td>
                  <td className="px-4 py-3 text-right tabular-nums font-semibold" style={{ color: p90Color(p90) }}>{p90}s</td>
                  <td className="px-4 py-3 text-right tabular-nums text-gray-300 text-xs">
                    {delta >= 0 ? '+' : ''}{delta}s
                  </td>
                  <td className={cn(
                    'px-4 py-3 text-right tabular-nums text-xs',
                    pctOver2 > 10 ? 'text-orange-400' : 'text-gray-500'
                  )}>
                    {over2 > 0 ? over2.toLocaleString() : '—'}
                  </td>
                  <td className="px-4 py-3">
                    <div className="h-2 rounded-full bg-gray-800 overflow-hidden">
                      <div
                        className="h-full rounded-full transition-all"
                        style={{ width: `${barPct}%`, background: p90Color(p90) }}
                      />
                    </div>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Stop Profile tab ───────────────────────────────────────────────────────────

function StopProfileTab({
  profileData, direction,
}: {
  profileData: any[]
  direction: 0 | 1 | 'both'
}) {
  if (profileData.length === 0) return <EmptyState />

  // Group profile by direction
  const dirs = direction === 'both' ? [0, 1] : [direction as number]
  const grouped = dirs.map((d) => ({
    dirId: d,
    label: profileData.find((r) => toNum(r.direction_id) === d)?.direction_label ?? `Dir ${d}`,
    rows: profileData
      .filter((r) => toNum(r.direction_id) === d)
      .sort((a, b) => toNum(a.stop_sequence) - toNum(b.stop_sequence)),
  })).filter((g) => g.rows.length > 0)

  return (
    <div className="p-6 space-y-8">
      {/* Stop sequence bar charts per direction */}
      {grouped.map(({ dirId, label, rows }) => (
        <div key={dirId} className="bg-gray-900 rounded-xl border border-gray-800 p-5">
          <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1">
            {dirId === 0 ? `${label} →` : `← ${label}`} — Avg Dwell by Stop
          </h3>
          <p className="text-xs text-gray-600 mb-4">VP-inferred · ordered by stop sequence · terminal stops excluded when filter active</p>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={rows} layout="vertical" margin={{ top: 0, right: 48, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" horizontal={false} />
              <XAxis type="number" tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} axisLine={false}
                tickFormatter={(v) => `${v}s`} />
              <YAxis type="category" dataKey="stop_name" width={150}
                tick={{ fill: '#9ca3af', fontSize: 9 }}
                tickLine={false} axisLine={false}
                tickFormatter={(v: string) => v.length > 20 ? v.slice(0, 20) + '…' : v}
              />
              <Tooltip
                contentStyle={{ background: '#111827', border: '1px solid #374151', borderRadius: 8, fontSize: 12 }}
                labelStyle={{ color: '#9ca3af' }}
                formatter={(v: any, name: any) => [`${v}s`, name === 'avg_dwell_sec' ? 'Avg' : 'p90']}
              />
              <Bar dataKey="avg_dwell_sec" name="avg_dwell_sec" fill="#a78bfa" radius={[0, 3, 3, 0]} maxBarSize={12} />
              <Bar dataKey="p90_dwell_sec" name="p90_dwell_sec" fill="#f59e0b" radius={[0, 3, 3, 0]} maxBarSize={12} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      ))}
    </div>
  )
}

// ── Trip × Stop heatmap ────────────────────────────────────────────────────────

function TripMatrix({ data, endDate }: { data: any[]; endDate: string }) {
  // Use first available direction if both present (most useful single view)
  const dirs = [...new Set(data.map((r) => toNum(r.direction_id)))].sort()
  const [selectedDir, setSelectedDir] = useState<number>(dirs[0] ?? 0)

  const dirData = data.filter((r) => toNum(r.direction_id) === selectedDir)
  const dirLabel = dirData[0]?.direction_label ?? `Dir ${selectedDir}`

  // Build unique stops sorted by sequence (cap at 30)
  const stopSeqs: [number, string][] = []
  const seenSeqs = new Set<number>()
  for (const r of dirData) {
    const seq = toNum(r.stop_sequence)
    if (!seenSeqs.has(seq)) { seenSeqs.add(seq); stopSeqs.push([seq, r.stop_name ?? r.stop_id]) }
  }
  stopSeqs.sort((a, b) => a[0] - b[0])
  const stops = stopSeqs.slice(0, 30)

  // Unique trips (cap at 40)
  const trips = [...new Set(dirData.map((r) => r.trip_id as string))].slice(0, 40)

  // Lookup
  const lookup = new Map<string, number>()
  for (const r of dirData) lookup.set(`${r.trip_id}:${r.stop_sequence}`, toNum(r.dwell_seconds))

  const CELL_W = 32
  const CELL_H = 22
  const LABEL_W = 96

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 p-5">
      <div className="flex items-center justify-between mb-3">
        <div>
          <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider">
            Trip × Stop Matrix
          </h3>
          <p className="text-xs text-gray-600 mt-0.5">
            {endDate} · {dirLabel} · {trips.length} trips · {stops.length} stops · VP-inferred dwell
          </p>
        </div>
        {dirs.length > 1 && (
          <div className="flex gap-1">
            {dirs.map((d) => (
              <button
                key={d}
                onClick={() => setSelectedDir(d)}
                className={cn(
                  'px-2 py-1 text-xs rounded font-medium transition-colors',
                  d === selectedDir ? 'bg-violet-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'
                )}
              >
                {d === 0 ? 'Outbound' : 'Inbound'}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Color legend */}
      <div className="flex items-center gap-3 mb-3 flex-wrap">
        {[
          { label: '<15s', color: '#374151' },
          { label: '15-30s', color: '#15803d' },
          { label: '30-60s', color: '#a16207' },
          { label: '60-120s', color: '#c2410c' },
          { label: '120s+', color: '#b91c1c' },
        ].map(({ label, color }) => (
          <span key={label} className="flex items-center gap-1 text-xs text-gray-400">
            <span className="inline-block w-3 h-3 rounded-sm" style={{ background: color }} />
            {label}
          </span>
        ))}
        <span className="text-xs text-gray-600 ml-1">— no data</span>
      </div>

      {trips.length === 0 ? (
        <p className="text-gray-600 text-sm text-center py-6">No trip data for {endDate}</p>
      ) : (
        <div className="overflow-auto">
          <div style={{ minWidth: LABEL_W + stops.length * CELL_W }}>
            {/* Stop header row */}
            <div style={{ display: 'flex', alignItems: 'flex-end', height: 72, marginLeft: LABEL_W }}>
              {stops.map(([seq, name]) => (
                <div
                  key={seq}
                  style={{ width: CELL_W, flexShrink: 0, position: 'relative', height: 72 }}
                >
                  <div style={{
                    position: 'absolute',
                    bottom: 4,
                    left: '50%',
                    transform: 'translateX(-50%) rotate(-45deg)',
                    transformOrigin: '50% 100%',
                    whiteSpace: 'nowrap',
                    fontSize: 8,
                    color: '#6b7280',
                    lineHeight: 1,
                  }}>
                    {name.length > 14 ? name.slice(0, 14) + '…' : name}
                  </div>
                </div>
              ))}
            </div>

            {/* Data rows */}
            {trips.map((tripId) => (
              <div key={tripId} style={{ display: 'flex', alignItems: 'center', height: CELL_H }}>
                {/* Trip label */}
                <div style={{
                  width: LABEL_W, flexShrink: 0,
                  fontSize: 8, color: '#6b7280',
                  textAlign: 'right', paddingRight: 6,
                  overflow: 'hidden', whiteSpace: 'nowrap',
                }}>
                  {tripId.slice(-10)}
                </div>
                {/* Cells */}
                {stops.map(([seq, stopName]) => {
                  const val = lookup.has(`${tripId}:${seq}`) ? lookup.get(`${tripId}:${seq}`)! : null
                  return (
                    <div
                      key={seq}
                      title={val !== null ? `${stopName}: ${val}s` : `${stopName}: no data`}
                      style={{
                        width: CELL_W,
                        height: CELL_H,
                        flexShrink: 0,
                        backgroundColor: cellColor(val),
                        border: '1px solid #030712',
                      }}
                    />
                  )
                })}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ── Time of Day tab ────────────────────────────────────────────────────────────

function TimeOfDayTab({ data }: { data: any[] }) {
  if (data.length === 0) return <EmptyState />

  const formatted = data.map((r) => ({
    hour: `${String(toNum(r.phoenix_hour)).padStart(2, '0')}:00`,
    avg: toNum(r.avg_dwell_sec),
    p90: toNum(r.p90_dwell_sec),
    obs: toNum(r.observations),
  }))

  return (
    <div className="p-6 space-y-6">
      <div className="bg-gray-900 rounded-xl border border-gray-800 p-5">
        <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1">
          Avg Dwell by Hour of Day (Phoenix local, UTC-7)
        </h3>
        <p className="text-xs text-gray-600 mb-4">VP-inferred · hover for observation count</p>
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={formatted} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
            <XAxis dataKey="hour" tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} axisLine={false} interval={1} />
            <YAxis tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} axisLine={false}
              tickFormatter={(v) => `${v}s`} />
            <Tooltip
              contentStyle={{ background: '#111827', border: '1px solid #374151', borderRadius: 8, fontSize: 12 }}
              labelStyle={{ color: '#9ca3af' }}
              formatter={(v: any, name: any) => [
                `${v}s`,
                name === 'avg' ? 'Avg Dwell' : 'p90 Dwell'
              ]}
            />
            <Legend wrapperStyle={{ fontSize: 11, color: '#9ca3af' }} />
            <Bar dataKey="avg" name="Avg Dwell" fill="#a78bfa" maxBarSize={18} radius={[2, 2, 0, 0]}>
              {formatted.map((d, i) => (
                <Cell key={i} fill={cellColor(d.avg)} />
              ))}
            </Bar>
            <Bar dataKey="p90" name="p90 Dwell" fill="#f59e0b" maxBarSize={18} radius={[2, 2, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
        {/* Reference lines annotation */}
        <div className="flex gap-6 mt-3 text-xs text-gray-600">
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm" style={{ background: '#15803d' }} />≤30s normal</span>
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm" style={{ background: '#a16207' }} />30-60s slow</span>
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm" style={{ background: '#c2410c' }} />60-120s high pax</span>
          <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-sm" style={{ background: '#b91c1c' }} />120s+ outlier</span>
        </div>
      </div>
    </div>
  )
}

// ── Trend tab ──────────────────────────────────────────────────────────────────

function TrendTab({ data, endDate }: { data: any[]; endDate: string }) {
  if (data.length === 0) return <EmptyState />

  const formatted = data.map((r) => ({
    ...r,
    avg: toNum(r.avg_dwell_sec),
    p50: toNum(r.p50_dwell_sec),
    p90: toNum(r.p90_dwell_sec),
    label: new Date(r.service_date + 'T12:00:00Z').toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
  }))

  return (
    <div className="p-6 space-y-6">
      <div className="bg-gray-900 rounded-xl border border-gray-800 p-5">
        <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1">
          30-Day Dwell Trend
        </h3>
        <p className="text-xs text-gray-600 mb-4">
          VP-inferred · 30 days ending {endDate} · avg / p50 / p90 dwell seconds
        </p>
        <ResponsiveContainer width="100%" height={220}>
          <AreaChart data={formatted} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
            <XAxis dataKey="label" tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} axisLine={false} interval={4} />
            <YAxis tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} axisLine={false}
              tickFormatter={(v) => `${v}s`} />
            <Tooltip
              contentStyle={{ background: '#111827', border: '1px solid #374151', borderRadius: 8, fontSize: 12 }}
              labelStyle={{ color: '#9ca3af' }}
              formatter={(v: any, name: any) => [
                `${v}s`,
                name === 'avg' ? 'Avg' : name === 'p50' ? 'p50 (Median)' : 'p90'
              ]}
            />
            <Legend wrapperStyle={{ fontSize: 11, color: '#9ca3af' }} />
            <Area type="monotone" dataKey="p90" name="p90" stroke="#f59e0b" strokeWidth={1.5} fill="none" dot={false} strokeDasharray="4 2" />
            <Area type="monotone" dataKey="avg" name="avg" stroke="#a78bfa" strokeWidth={2} fill="none" dot={false} />
            <Area type="monotone" dataKey="p50" name="p50" stroke="#34d399" strokeWidth={1.5} fill="none" dot={false} strokeDasharray="4 2" />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

// ── Shared ─────────────────────────────────────────────────────────────────────

function KpiCard({ label, value, sub, color }: { label: string; value: string; sub?: string; color?: string }) {
  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 px-4 py-4">
      <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1">{label}</div>
      <div className="text-2xl font-bold tabular-nums" style={color ? { color } : { color: '#f9fafb' }}>{value}</div>
      {sub && <div className="text-xs text-gray-500 mt-0.5">{sub}</div>}
    </div>
  )
}

function EmptyState() {
  return <div className="flex items-center justify-center h-64 text-gray-600 text-sm">No data for selected filters</div>
}

function EmptyChart() {
  return <div className="flex items-center justify-center h-32 text-gray-700 text-xs">No data</div>
}

function LoadingSkeleton() {
  return (
    <div className="p-6 space-y-4 animate-pulse">
      <div className="grid grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => <div key={i} className="h-20 bg-gray-800 rounded-xl" />)}
      </div>
      <div className="grid grid-cols-2 gap-4">
        <div className="h-56 bg-gray-800 rounded-xl" />
        <div className="h-56 bg-gray-800 rounded-xl" />
      </div>
    </div>
  )
}
