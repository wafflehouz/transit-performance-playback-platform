'use client'

import { useState, useEffect, useCallback, useRef, useMemo } from 'react'
import {
  AreaChart, Area, BarChart, Bar,
  PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import { useFilterPanel } from '@/lib/filter-panel-context'
import RouteFilterPanel, {
  type OtpFilterState,
  type DatePreset,
  resolveDates,
} from '@/components/filters/RouteFilterPanel'
import type { DimRoute } from '@/types'
import {
  ROUTES_WITH_DATA_SQL, ROUTE_GROUPS_SQL,
  summaryAllSql, otpTrendSql, routesTableSql,
  singleRouteSummarySql, timeOfDaySql, stopOtpSql,
  delayHistogramSql, singleRouteTrendSql, routeHeadsignSql,
  schedulePivotSql,
} from '@/lib/queries/otp'
import { cn } from '@/lib/utils'

// ── OTP colors (Swiftly / Databricks palette) ─────────────────────────────────
const OTP_COLORS = { early: '#e57373', onTime: '#4db6ac', late: '#ffb74d' }

// ── VM route colors ───────────────────────────────────────────────────────────
const VM_NAME_COLORS: Record<string, string> = {
  'A': '#38BDF8', 'B': '#F59E0B', 'S': '#84CC16', 'SKYT': '#94A3B8',
}
function routeColor(route: DimRoute | undefined): string {
  if (!route) return '#A855F7'
  const key = route.route_short_name.toUpperCase().trim()
  return VM_NAME_COLORS[key] ?? '#A855F7'
}

// Databricks returns numbers as strings
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

// ── Tab definitions ────────────────────────────────────────────────────────────
type TabId = 'summary' | 'routes' | 'time-of-day' | 'histogram' | 'stops' | 'schedule'

const ALL_TABS: { id: TabId; label: string; modes: OtpFilterState['mode'][] }[] = [
  { id: 'summary',     label: 'Summary',     modes: ['all', 'group', 'single'] },
  { id: 'routes',      label: 'Routes',      modes: ['all', 'group'] },
  { id: 'time-of-day', label: 'Time of Day', modes: ['single'] },
  { id: 'histogram',   label: 'Histogram',   modes: ['single'] },
  { id: 'stops',       label: 'Stops',       modes: ['single'] },
  { id: 'schedule',    label: 'Schedule',    modes: ['single'] },
]

// ── Main page ──────────────────────────────────────────────────────────────────

export default function OtpPageClient() {
  const { setContent } = useFilterPanel()
  const setContentRef = useRef(setContent)
  setContentRef.current = setContent

  const [preset, setPreset] = useState<DatePreset>('7d')
  const [filters, setFilters] = useState<OtpFilterState>(() => ({
    mode: 'all',
    groupName: null,
    routeId: null,
    ...resolveDates('7d'),
    direction: 'both',
    timepointOnly: false,
  }))
  const [activeTab, setActiveTab] = useState<TabId>('summary')

  const [routes, setRoutes] = useState<DimRoute[]>([])
  const [groups, setGroups] = useState<string[]>([])
  const [summaryData, setSummaryData] = useState<Record<string, any> | null>(null)
  const [trendData, setTrendData] = useState<any[]>([])
  const [routesData, setRoutesData] = useState<any[]>([])
  const [todData, setTodData] = useState<any[]>([])
  const [stopsData, setStopsData] = useState<any[]>([])
  const [histData, setHistData] = useState<any[]>([])
  const [headsigns, setHeadsigns] = useState<Record<number, string>>({})
  const [excludeTerminals, setExcludeTerminals] = useState(false)
  const [scheduleDate, setScheduleDate] = useState(() => {
    const d = new Date()
    d.setDate(d.getDate() - 1)
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
  })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Available tabs based on mode
  const availableTabs = ALL_TABS.filter((t) => t.modes.includes(filters.mode))

  useEffect(() => {
    if (!availableTabs.find((t) => t.id === activeTab)) setActiveTab('summary')
  }, [filters.mode]) // eslint-disable-line react-hooks/exhaustive-deps

  // Load static data once
  useEffect(() => {
    fetchJson(ROUTES_WITH_DATA_SQL).then(setRoutes).catch(() => {})
    fetchJson(ROUTE_GROUPS_SQL)
      .then((rows: any[]) => setGroups(rows.map((r) => r.group_name)))
      .catch(() => {})
  }, [])

  // When preset changes, update the date range in filters
  function handlePresetChange(p: DatePreset) {
    setPreset(p)
    setFilters((f) => ({ ...f, ...resolveDates(p) }))
  }

  // Fetch — each query independent so one failure doesn't block others
  const fetchData = useCallback(async (f: OtpFilterState) => {
    setLoading(true)
    setError(null)
    setSummaryData(null)
    setTrendData([])
    setRoutesData([])
    setTodData([])
    setHistData([])
    setStopsData([])
    setHeadsigns({})

    const params = { startDate: f.startDate, endDate: f.endDate }
    const errors: string[] = []

    async function safe(p: Promise<any[]>): Promise<any[]> {
      try { return await p } catch (e: any) { errors.push(e.message); return [] }
    }

    const tp = f.timepointOnly

    if (f.mode === 'single' && f.routeId) {
      const [summary, trend, tod, hist, stops, hsigns] = await Promise.all([
        safe(fetchJson(singleRouteSummarySql(f.routeId, f.direction, tp), params)),
        safe(fetchJson(singleRouteTrendSql(f.routeId, f.direction, tp), params)),
        safe(fetchJson(timeOfDaySql(f.routeId, f.direction, tp), params)),
        safe(fetchJson(delayHistogramSql(f.routeId, f.direction, tp), params)),
        safe(fetchJson(stopOtpSql(f.routeId, f.direction, tp), params)),
        safe(fetchJson(routeHeadsignSql(f.routeId))),
      ])
      setSummaryData(summary[0] ?? null)
      setTrendData(trend)
      setTodData(tod)
      setHistData(hist)
      setStopsData(stops)
      // Build direction_id → first headsign map
      const hsMap: Record<number, string> = {}
      for (const row of hsigns) {
        const dir = Number(row.direction_id)
        if (!(dir in hsMap) && row.trip_headsign) hsMap[dir] = row.trip_headsign
      }
      setHeadsigns(hsMap)
    } else {
      const [summary, trend, routeRows] = await Promise.all([
        safe(fetchJson(summaryAllSql(f.groupName, tp), params)),
        safe(fetchJson(otpTrendSql(f.groupName, tp), params)),
        safe(fetchJson(routesTableSql(f.groupName, f.direction, tp), params)),
      ])
      setSummaryData(summary[0] ?? null)
      setTrendData(trend)
      setRoutesData(routeRows)
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
        onChange={setFilters}
        routes={routes}
        groups={groups}
        activePreset={preset}
        onPresetChange={handlePresetChange}
        excludeTerminals={excludeTerminals}
        onExcludeTerminalsChange={setExcludeTerminals}
        {...(activeTab === 'schedule' ? {
          scheduleDate,
          onScheduleDateChange: setScheduleDate,
        } : {})}
      />
    ),
    [filters, routes, groups, preset, excludeTerminals, activeTab, scheduleDate] // eslint-disable-line react-hooks/exhaustive-deps
  )
  useEffect(() => { setContentRef.current(filterNode) }, [filterNode])

  // Header scope label
  const scopeLabel = useMemo(() => {
    if (filters.mode === 'group') return filters.groupName ?? 'Select a group'
    if (filters.mode === 'single') {
      const r = routes.find((r) => r.route_id === filters.routeId)
      return r ? `Route ${r.route_short_name}` : 'Select a route'
    }
    return 'All Routes'
  }, [filters, routes])

  const dateLabel = activeTab === 'schedule'
    ? scheduleDate
    : filters.startDate === filters.endDate
      ? filters.endDate
      : `${filters.startDate} → ${filters.endDate}`

  const needsSelection =
    (filters.mode === 'group' && !filters.groupName) ||
    (filters.mode === 'single' && !filters.routeId)

  const selectedRoute = routes.find((r) => r.route_id === filters.routeId)

  const otpPct = summaryData ? toNum(summaryData.on_time_pct) : null

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-800 flex items-center justify-between shrink-0">
        <div>
          <h1 className="text-base font-semibold text-white">On-Time Performance</h1>
          <p className="text-gray-500 text-xs mt-0.5">
            {loading ? 'Loading…' : `${scopeLabel} · ${dateLabel}`}
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
        ) : loading && activeTab !== 'schedule' ? (
          <LoadingSkeleton />
        ) : (
          <>
            {activeTab === 'summary' && (
              <SummaryTab summary={summaryData} trend={trendData} mode={filters.mode} />
            )}
            {activeTab === 'routes' && (
              <RoutesTab rows={routesData} routes={routes} />
            )}
            {activeTab === 'time-of-day' && (
              <TimeOfDayTab data={todData} />
            )}
            {activeTab === 'histogram' && (
              <HistogramTab data={histData} />
            )}
            {activeTab === 'stops' && (
              <StopsTab data={stopsData} direction={filters.direction} headsigns={headsigns} />
            )}
            {activeTab === 'schedule' && filters.routeId && (
              <ScheduleTab
                routeId={filters.routeId}
                direction={filters.direction}
                timepointOnly={filters.timepointOnly}
                excludeTerminals={excludeTerminals}
                serviceDate={scheduleDate}
                headsigns={headsigns}
              />
            )}
          </>
        )}
      </div>
    </div>
  )
}

// ── Summary tab ────────────────────────────────────────────────────────────────

function SummaryTab({
  summary,
  trend,
  mode,
}: {
  summary: Record<string, any> | null
  trend: any[]
  mode: OtpFilterState['mode']
}) {
  if (!summary) return <EmptyState />

  const onTime = toNum(summary.on_time_pct)
  const early  = toNum(summary.early_pct)
  const late   = toNum(summary.late_pct)

  const pieData = [
    { name: 'Early',   value: early,  color: OTP_COLORS.early },
    { name: 'On-Time', value: onTime, color: OTP_COLORS.onTime },
    { name: 'Late',    value: late,   color: OTP_COLORS.late },
  ]

  const trendFormatted = trend.map((r) => ({
    ...r,
    on_time_pct:  toNum(r.on_time_pct),
    early_pct:    toNum(r.early_pct),
    late_pct:     toNum(r.late_pct),
    label: new Date(r.service_date + 'T12:00:00Z').toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
  }))

  return (
    <div className="p-6 space-y-6">
      {/* KPI row */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <KpiCard label="On-Time"    value={`${onTime.toFixed(1)}%`} color={OTP_COLORS.onTime} />
        <KpiCard label="Early"      value={`${early.toFixed(1)}%`}  color={OTP_COLORS.early} />
        <KpiCard label="Late"       value={`${late.toFixed(1)}%`}   color={OTP_COLORS.late} />
        <KpiCard
          label={mode === 'single' ? 'Stop Obs.' : 'Routes'}
          value={mode === 'single'
            ? toNum(summary.total_stops).toLocaleString()
            : String(toNum(summary.route_count))}
          sub={mode === 'single' ? 'observations' : 'active'}
        />
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* OTP donut */}
        <div className="bg-gray-900 rounded-xl border border-gray-800 p-5">
          <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-4">OTP Breakdown</h3>
          <div className="flex items-center gap-4">
            <ResponsiveContainer width={120} height={120}>
              <PieChart>
                <Pie data={pieData} cx="50%" cy="50%" innerRadius={36} outerRadius={56}
                  startAngle={90} endAngle={-270} dataKey="value" strokeWidth={0}>
                  {pieData.map((d, i) => <Cell key={i} fill={d.color} />)}
                </Pie>
              </PieChart>
            </ResponsiveContainer>
            <div className="space-y-2 flex-1">
              {pieData.map((d) => (
                <div key={d.name} className="flex items-center gap-2">
                  <div className="w-2.5 h-2.5 rounded-full shrink-0" style={{ background: d.color }} />
                  <span className="text-xs text-gray-400">{d.name}</span>
                  <span className="text-xs text-white font-semibold ml-auto">{d.value.toFixed(1)}%</span>
                </div>
              ))}
              {summary.avg_delay_min != null && (
                <div className="pt-1 border-t border-gray-800 text-xs text-gray-500">
                  avg {toNum(summary.avg_delay_min) >= 0 ? '+' : ''}{toNum(summary.avg_delay_min).toFixed(1)} min
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Trend */}
        <div className="lg:col-span-2 bg-gray-900 rounded-xl border border-gray-800 p-5">
          <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-4">OTP Trend</h3>
          {trendFormatted.length === 0 ? <EmptyChart /> : (
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
    </div>
  )
}

// ── Routes tab ─────────────────────────────────────────────────────────────────

function RoutesTab({ rows, routes }: { rows: any[]; routes: DimRoute[] }) {
  if (rows.length === 0) return <EmptyState />

  return (
    <div className="p-6">
      <div className="bg-gray-900 rounded-xl border border-gray-800 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800">
              <th className="text-left px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider">Route</th>
              <th className="text-right px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider">Obs.</th>
              <th className="text-right px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider" style={{ color: OTP_COLORS.onTime }}>On-Time</th>
              <th className="text-right px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider" style={{ color: OTP_COLORS.early }}>Early</th>
              <th className="text-right px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider" style={{ color: OTP_COLORS.late }}>Late</th>
              <th className="px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider">OTP Bar</th>
              <th className="text-right px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider">Avg Delay</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => {
              const route = routes.find((r) => r.route_id === row.route_id)
              const color = routeColor(route)
              const onTime = toNum(row.on_time_pct)
              const early  = toNum(row.early_pct)
              const late   = toNum(row.late_pct)
              const avgMin = toNum(row.avg_delay_min)
              return (
                <tr key={i} className="border-b border-gray-800/50 hover:bg-gray-800/30 transition-colors">
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-2">
                      <div className="w-2 h-6 rounded-full shrink-0" style={{ background: color }} />
                      <div>
                        <div className="text-white font-medium">{row.route_short_name}</div>
                        <div className="text-gray-500 text-xs truncate max-w-[160px]">{row.route_long_name}</div>
                      </div>
                    </div>
                  </td>
                  <td className="px-4 py-3 text-right text-gray-500 tabular-nums text-xs">{toNum(row.total_stops).toLocaleString()}</td>
                  <td className="px-4 py-3 text-right tabular-nums font-semibold" style={{ color: OTP_COLORS.onTime }}>{onTime.toFixed(1)}%</td>
                  <td className="px-4 py-3 text-right tabular-nums" style={{ color: OTP_COLORS.early }}>{early.toFixed(1)}%</td>
                  <td className="px-4 py-3 text-right tabular-nums" style={{ color: OTP_COLORS.late }}>{late.toFixed(1)}%</td>
                  <td className="px-4 py-3 w-28">
                    <div className="flex h-2 rounded-full overflow-hidden">
                      <div style={{ width: `${early}%`, background: OTP_COLORS.early }} />
                      <div style={{ width: `${onTime}%`, background: OTP_COLORS.onTime }} />
                      <div style={{ width: `${late}%`, background: OTP_COLORS.late }} />
                    </div>
                  </td>
                  <td className="px-4 py-3 text-right text-gray-300 tabular-nums text-xs">
                    {avgMin >= 0 ? '+' : ''}{avgMin.toFixed(1)}m
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

// ── Time of Day tab ────────────────────────────────────────────────────────────

function TimeOfDayTab({ data }: { data: any[] }) {
  if (data.length === 0) return <EmptyState />

  const formatted = data.map((r) => ({
    label: r.time_bucket,
    early:   toNum(r.early_count),
    onTime:  toNum(r.on_time_count),
    late:    toNum(r.late_count),
    total:   toNum(r.total_count),
  }))

  return (
    <div className="p-6 space-y-6">
      <div className="bg-gray-900 rounded-xl border border-gray-800 p-5">
        <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1">Stop Departures by Time of Day (Phoenix local)</h3>
        <p className="text-xs text-gray-600 mb-4">Stacked: Early · On-Time · Late</p>
        <ResponsiveContainer width="100%" height={240}>
          <BarChart data={formatted} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
            <XAxis dataKey="label" tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} axisLine={false} interval={3} />
            <YAxis tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} axisLine={false} />
            <Tooltip
              contentStyle={{ background: '#111827', border: '1px solid #374151', borderRadius: 8, fontSize: 12 }}
              labelStyle={{ color: '#9ca3af' }}
              formatter={(v: any, name: any) => [v, name]}
            />
            <Legend wrapperStyle={{ fontSize: 11, color: '#9ca3af' }} />
            <Bar dataKey="early"  name="Early"   stackId="a" fill={OTP_COLORS.early}  maxBarSize={14} />
            <Bar dataKey="onTime" name="On-Time"  stackId="a" fill={OTP_COLORS.onTime} maxBarSize={14} />
            <Bar dataKey="late"   name="Late"     stackId="a" fill={OTP_COLORS.late}   maxBarSize={14} radius={[2, 2, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

// ── Histogram tab ──────────────────────────────────────────────────────────────

function HistogramTab({ data }: { data: any[] }) {
  if (data.length === 0) return <EmptyState />

  // Build per-minute bins with all 3 categories
  const binMap = new Map<number, { early: number; onTime: number; late: number }>()
  for (const r of data) {
    const min = toNum(r.delay_minute)
    const entry = binMap.get(min) ?? { early: 0, onTime: 0, late: 0 }
    const count = toNum(r.stop_count)
    if (r.otp_category === 'Early')   entry.early  += count
    else if (r.otp_category === 'On-Time') entry.onTime += count
    else entry.late += count
    binMap.set(min, entry)
  }

  const bins = Array.from(binMap.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([min, v]) => ({ label: `${min >= 0 ? '+' : ''}${min}m`, min, ...v }))

  return (
    <div className="p-6 space-y-6">
      <div className="bg-gray-900 rounded-xl border border-gray-800 p-5">
        <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1">Delay Distribution (1-min bins)</h3>
        <p className="text-xs text-gray-600 mb-4">Negative = early, positive = late. Window: -20 to +60 min.</p>
        <ResponsiveContainer width="100%" height={240}>
          <BarChart data={bins} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
            <XAxis dataKey="label" tick={{ fill: '#6b7280', fontSize: 9 }} tickLine={false} axisLine={false} interval={4} />
            <YAxis tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} axisLine={false} />
            <Tooltip
              contentStyle={{ background: '#111827', border: '1px solid #374151', borderRadius: 8, fontSize: 12 }}
              labelStyle={{ color: '#9ca3af' }}
              formatter={(v: any, name: any) => [v, name]}
            />
            <Legend wrapperStyle={{ fontSize: 11, color: '#9ca3af' }} />
            <Bar dataKey="early"  name="Early"   stackId="a" fill={OTP_COLORS.early}  maxBarSize={20} />
            <Bar dataKey="onTime" name="On-Time"  stackId="a" fill={OTP_COLORS.onTime} maxBarSize={20} />
            <Bar dataKey="late"   name="Late"     stackId="a" fill={OTP_COLORS.late}   maxBarSize={20} radius={[2, 2, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

// ── Stops tab — Swiftly-style horizontal stacked bars ─────────────────────────

function StopsTab({
  data,
  direction,
  headsigns,
}: {
  data: any[]
  direction: 0 | 1 | 'both'
  headsigns: Record<number, string>
}) {
  if (data.length === 0) return <EmptyState />

  // Group by direction, sorted by stop_order
  const dirs = direction === 'both' ? [0, 1] : [direction]
  const grouped = dirs.map((d) => ({
    dirId: d,
    rows: data
      .filter((r) => toNum(r.direction_id) === d)
      .sort((a, b) => toNum(a.stop_order) - toNum(b.stop_order)),
  })).filter((g) => g.rows.length > 0)

  function dirLabel(dirId: number) {
    if (headsigns[dirId]) return headsigns[dirId]
    return dirId === 0 ? 'Outbound' : 'Inbound'
  }

  return (
    <div className="overflow-auto">
      {grouped.map(({ dirId, rows }) => (
        <div key={dirId} className="mb-6">
          {direction === 'both' && (
            <div className="px-4 py-2 text-xs font-semibold text-gray-500 uppercase tracking-wider border-b border-gray-800">
              {dirId === 0 ? `${dirLabel(dirId)} →` : `← ${dirLabel(dirId)}`}
            </div>
          )}
          {/* Legend row */}
          <div className="flex items-center gap-4 px-4 py-2 border-b border-gray-800/50">
            <div className="w-44 shrink-0" />
            <div className="flex items-center gap-3 text-xs text-gray-500">
              <span className="flex items-center gap-1">
                <span className="inline-block w-2.5 h-2.5 rounded-sm" style={{ background: OTP_COLORS.early }} />
                Early (&lt;1 min)
              </span>
              <span className="flex items-center gap-1">
                <span className="inline-block w-2.5 h-2.5 rounded-sm" style={{ background: OTP_COLORS.onTime }} />
                On-Time
              </span>
              <span className="flex items-center gap-1">
                <span className="inline-block w-2.5 h-2.5 rounded-sm" style={{ background: OTP_COLORS.late }} />
                Late (&gt;6 min)
              </span>
              <span className="ml-2 text-gray-600">* = timepoint</span>
            </div>
          </div>
          {/* Stop rows */}
          <div className="py-1">
            {rows.map((row, i) => {
              const early  = toNum(row.early_pct)
              const onTime = toNum(row.on_time_pct)
              const late   = toNum(row.late_pct)
              const isTimepoint = toNum(row.timepoint) === 1

              return (
                <div
                  key={i}
                  className="flex items-center gap-2 px-4 py-px hover:bg-gray-800/30 group"
                  title={`${row.stop_name} · ${toNum(row.observations)} obs · avg ${toNum(row.avg_delay_min) >= 0 ? '+' : ''}${toNum(row.avg_delay_min).toFixed(1)}m`}
                >
                  {/* Stop name — right-aligned, fixed width */}
                  <div className="w-44 shrink-0 text-right">
                    <span className="text-xs text-gray-400 truncate block leading-6">
                      {isTimepoint && <span className="text-gray-300">* </span>}
                      {row.stop_name}
                    </span>
                  </div>

                  {/* Stacked bar */}
                  <div className="flex-1 flex h-6 overflow-hidden rounded-sm">
                    {/* Early — left side */}
                    {early > 0 && (
                      <div
                        style={{ width: `${early}%`, background: OTP_COLORS.early, flexShrink: 0 }}
                        className="flex items-center justify-center overflow-hidden"
                      >
                        {early >= 4 && (
                          <span className="text-white text-xs font-medium whitespace-nowrap px-1">
                            {early.toFixed(1)}%
                          </span>
                        )}
                      </div>
                    )}
                    {/* On-Time — center, label at right edge */}
                    <div
                      style={{ width: `${onTime}%`, background: OTP_COLORS.onTime, flexShrink: 0 }}
                      className="flex items-center justify-end overflow-hidden"
                    >
                      {onTime >= 8 && (
                        <span className="text-white text-xs font-medium whitespace-nowrap px-1.5">
                          {onTime.toFixed(1)}%
                        </span>
                      )}
                    </div>
                    {/* Late — right side */}
                    {late > 0 && (
                      <div
                        style={{ width: `${late}%`, background: OTP_COLORS.late, flexShrink: 0 }}
                        className="flex items-center justify-end overflow-hidden"
                      >
                        {late >= 4 && (
                          <span className="text-white text-xs font-medium whitespace-nowrap px-1">
                            {late.toFixed(1)}%
                          </span>
                        )}
                      </div>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      ))}
    </div>
  )
}

// ── Shared utilities ───────────────────────────────────────────────────────────

function KpiCard({ label, value, sub, color }: { label: string; value: string; sub?: string; color?: string }) {
  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 px-4 py-4">
      <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1">{label}</div>
      <div className="text-2xl font-bold tabular-nums" style={color ? { color } : { color: '#f9fafb' }}>{value}</div>
      {sub && <div className="text-xs text-gray-500 mt-0.5">{sub}</div>}
    </div>
  )
}

// ── Schedule tab — trip × stop pivot heatmap ───────────────────────────────────

function ScheduleTab({
  routeId,
  direction,
  timepointOnly,
  excludeTerminals,
  serviceDate,
  headsigns,
}: {
  routeId: string
  direction: 0 | 1 | 'both'
  timepointOnly: boolean
  excludeTerminals: boolean
  serviceDate: string
  headsigns: Record<number, string>
}) {
  const [rows, setRows] = useState<any[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!routeId) return
    setLoading(true)
    setError(null)
    fetchJson(schedulePivotSql(routeId, direction, timepointOnly), { serviceDate })
      .then(setRows)
      .catch((e: any) => setError(e.message))
      .finally(() => setLoading(false))
  }, [routeId, direction, timepointOnly, serviceDate])

  const dirIds = direction === 'both' ? [0, 1] : [direction as number]

  const CELL_W = 48
  const CELL_H = 24
  const HEADER_H = 150
  const ROW_LABEL_W = 58

  return (
    <div className="p-4 space-y-4">
      {loading && (
        <p className="text-xs text-gray-500 animate-pulse">Loading {serviceDate}…</p>
      )}

      {error && (
        <div className="text-red-400 text-sm bg-red-900/20 border border-red-800 rounded-lg px-4 py-3">
          {error}
        </div>
      )}

      {/* Legend */}
      {!loading && rows.length > 0 && (
        <div className="flex items-center gap-3 text-xs text-gray-400 flex-wrap">
          <span className="font-medium text-gray-300">Arrival Delay:</span>
          {[
            { label: '4+ min early', bg: '#be185d' },
            { label: '1–4 min early', bg: '#f472b6' },
            { label: 'On-time', bg: '#1f2937', border: true },
            { label: '6–10 min late', bg: '#d97706' },
            { label: '10+ min late', bg: '#dc2626' },
          ].map((s) => (
            <span key={s.label} className="flex items-center gap-1">
              <span
                className="inline-block w-3 h-3 rounded-sm"
                style={{ background: s.bg, border: s.border ? '1px solid #374151' : 'none' }}
              />
              {s.label}
            </span>
          ))}
          <span className="text-gray-600 ml-2">Row = trip start · Column = stop · Cell = +M:SS (early) / −M:SS (late)</span>
        </div>
      )}

      {!loading && rows.length === 0 && !error && <EmptyState />}

      {!loading && dirIds.map((dirId) => {
        const dirRows = rows.filter((r) => toNum(r.direction_id) === dirId)
        if (dirRows.length === 0) return null

        // Unique stops sorted by canonical_seq
        const stopMap = new Map<string, { stop_name: string; canonical_seq: number }>()
        for (const r of dirRows) {
          if (!stopMap.has(r.stop_id)) {
            stopMap.set(r.stop_id, { stop_name: r.stop_name, canonical_seq: toNum(r.canonical_seq) })
          }
        }
        const allStops = Array.from(stopMap.entries())
          .sort((a, b) => a[1].canonical_seq - b[1].canonical_seq)
          .map(([stop_id, info]) => ({ stop_id, ...info }))
        const stops = excludeTerminals && allStops.length > 2
          ? allStops.slice(1, allStops.length - 1)
          : allStops

        // Unique trips sorted by first scheduled_arrival_ts
        const tripFirstTs = new Map<string, string>()
        for (const r of dirRows) {
          const ts = r.scheduled_arrival_ts ?? ''
          if (!tripFirstTs.has(r.trip_id) || ts < tripFirstTs.get(r.trip_id)!) {
            tripFirstTs.set(r.trip_id, ts)
          }
        }
        const trips = Array.from(tripFirstTs.entries())
          .sort((a, b) => a[1].localeCompare(b[1]))
          .map(([trip_id, firstTs]) => ({ trip_id, firstTs }))

        // Lookup: trip_id → stop_id → delay seconds
        const lookup = new Map<string, Map<string, number | null>>()
        for (const r of dirRows) {
          if (!lookup.has(r.trip_id)) lookup.set(r.trip_id, new Map())
          const delay = r.arrival_delay_seconds != null ? toNum(r.arrival_delay_seconds) : null
          lookup.get(r.trip_id)!.set(r.stop_id, delay)
        }

        const headsign = headsigns[dirId]
        const dirHeading = dirId === 0
          ? `${headsign ?? 'Outbound'} →`
          : `← ${headsign ?? 'Inbound'}`

        return (
          <div key={dirId} className="bg-gray-900 rounded-xl border border-gray-800 overflow-hidden">
            <div className="px-4 py-2 text-xs font-semibold text-gray-400 uppercase tracking-wider border-b border-gray-800">
              {dirHeading}
            </div>
            <p className="px-4 pt-2 pb-1 text-xs text-gray-600">
              {trips.length} trips · {stops.length} stops
            </p>
            <div className="overflow-x-auto">
              <div style={{ minWidth: ROW_LABEL_W + stops.length * CELL_W + 16 }}>
                {/* Column headers — rotated stop names */}
                <div style={{ display: 'flex', paddingLeft: ROW_LABEL_W, height: HEADER_H }}>
                  {stops.map((stop) => (
                    <div
                      key={stop.stop_id}
                      style={{ width: CELL_W, flexShrink: 0, position: 'relative', height: HEADER_H }}
                    >
                      <div
                        style={{
                          position: 'absolute',
                          bottom: 8,
                          left: 0,
                          width: CELL_W,
                          textAlign: 'center',
                          transform: 'rotate(-45deg)',
                          transformOrigin: 'center bottom',
                          fontSize: 9,
                          color: '#9ca3af',
                          whiteSpace: 'nowrap',
                          overflow: 'visible',
                          pointerEvents: 'none',
                        }}
                      >
                        {stop.stop_name.length > 22 ? stop.stop_name.slice(0, 22) + '…' : stop.stop_name}
                      </div>
                    </div>
                  ))}
                </div>

                {/* Separator */}
                <div style={{ height: 10, marginLeft: ROW_LABEL_W, borderBottom: '2px solid #374151' }} />

                {/* Trip rows */}
                {trips.map(({ trip_id, firstTs }) => {
                  const stopLookup = lookup.get(trip_id) ?? new Map()
                  return (
                    <div key={trip_id} style={{ display: 'flex', alignItems: 'center', height: CELL_H }}>
                      {/* Row label: trip's first scheduled arrival in Phoenix time */}
                      <div
                        style={{
                          width: ROW_LABEL_W,
                          flexShrink: 0,
                          textAlign: 'right',
                          paddingRight: 8,
                          fontSize: 10,
                          color: '#6b7280',
                          fontVariantNumeric: 'tabular-nums',
                        }}
                      >
                        {formatScheduleTime(firstTs)}
                      </div>

                      {/* Delay cells */}
                      {stops.map((stop) => {
                        const delay = stopLookup.has(stop.stop_id) ? stopLookup.get(stop.stop_id)! : null
                        const { bg, text } = scheduleDelayColor(delay)
                        return (
                          <div
                            key={stop.stop_id}
                            style={{
                              width: CELL_W,
                              height: CELL_H,
                              flexShrink: 0,
                              background: bg,
                              display: 'flex',
                              alignItems: 'center',
                              justifyContent: 'center',
                              fontSize: 9,
                              color: text,
                              fontVariantNumeric: 'tabular-nums',
                              borderLeft: '1px solid #111827',
                              borderBottom: '1px solid #111827',
                            }}
                            title={
                              delay !== null
                                ? `${stop.stop_name}: ${formatDelay(delay)}`
                                : `${stop.stop_name}: no data`
                            }
                          >
                            {delay !== null ? formatDelay(delay) : ''}
                          </div>
                        )
                      })}
                    </div>
                  )
                })}
              </div>
            </div>
          </div>
        )
      })}
    </div>
  )
}

function scheduleDelayColor(sec: number | null): { bg: string; text: string } {
  if (sec === null) return { bg: '#111827', text: '#374151' }
  if (sec < -240)  return { bg: '#be185d', text: '#fce7f3' }  // 4+ min early
  if (sec < -60)   return { bg: '#f472b6', text: '#831843' }  // 1-4 min early
  if (sec <= 360)  return { bg: '#1f2937', text: '#6b7280' }  // on-time
  if (sec <= 600)  return { bg: '#d97706', text: '#fff7ed' }  // 6-10 min late
  return             { bg: '#dc2626', text: '#fef2f2' }       // 10+ min late
}

function formatDelay(sec: number | null): string {
  if (sec === null) return ''
  const abs = Math.abs(Math.round(sec))
  const m = Math.floor(abs / 60)
  const s = abs % 60
  const sign = sec >= 0 ? '+' : '-'
  return `${sign}${m}:${s.toString().padStart(2, '0')}`
}

function formatScheduleTime(ts: string): string {
  // ts is a UTC timestamp; Phoenix = UTC-7 (Arizona has no DST)
  try {
    const d = new Date(ts)
    const phoenixMs = d.getTime() - 7 * 3600 * 1000
    const pd = new Date(phoenixMs)
    const h = pd.getUTCHours().toString().padStart(2, '0')
    const m = pd.getUTCMinutes().toString().padStart(2, '0')
    return `${h}:${m}`
  } catch {
    return ts?.slice(11, 16) ?? ''
  }
}

function EmptyState() {
  return <div className="flex items-center justify-center h-64 text-gray-600 text-sm">No data for selected filters</div>
}
function EmptyChart() {
  return <div className="flex items-center justify-center h-32 text-gray-700 text-xs">No trend data</div>
}
function LoadingSkeleton() {
  return (
    <div className="p-6 space-y-4 animate-pulse">
      <div className="grid grid-cols-4 gap-4">{Array.from({ length: 4 }).map((_, i) => <div key={i} className="h-20 bg-gray-800 rounded-xl" />)}</div>
      <div className="grid grid-cols-3 gap-4">
        <div className="h-44 bg-gray-800 rounded-xl" />
        <div className="lg:col-span-2 h-44 bg-gray-800 rounded-xl" />
      </div>
    </div>
  )
}
