'use client'

import { useState, useEffect, useCallback, useRef, useMemo } from 'react'
import { useFilterPanel } from '@/lib/filter-panel-context'
import { useNav } from '@/lib/nav-context'
import { FilterSection } from '@/components/ui/FilterControls'
import LiveMap, { type LiveVehicle, type OtpStatus } from '@/components/map/LiveMap'
import type { DimRoute, RouteStop } from '@/types'
import { cn } from '@/lib/utils'

const RENDER_API = process.env.NEXT_PUBLIC_RENDER_API_URL ?? 'http://localhost:3001'
const REFRESH_MS = 30_000

type LiveScope = 'group' | 'single'

// Valley Metro display colors
const VM_NAME_COLORS: Record<string, string> = {
  'A':    '#38BDF8',
  'B':    '#F59E0B',
  'S':    '#84CC16',
  'SKYT': '#94A3B8',
}
const VM_TYPE_COLORS: Record<number, string> = {
  0: '#38BDF8',
  1: '#A855F7',
  2: '#A855F7',
  3: '#A855F7',
}
const FALLBACK_ROUTE_COLOR = '#A855F7'

function getCuratedColor(routeType: number, routeShortName?: string): string {
  if (routeShortName) {
    const key = routeShortName.toUpperCase().trim()
    if (VM_NAME_COLORS[key]) return VM_NAME_COLORS[key]
  }
  return VM_TYPE_COLORS[routeType] ?? FALLBACK_ROUTE_COLOR
}

interface FeedResponse {
  vehicle_count: number
  fetched_at: string | null
  fetched_at_ms: number | null
  vehicles: LiveVehicle[]
}

const OTP_COLOR: Record<OtpStatus, string> = {
  early:     '#ec4899',
  on_time:   '#22c55e',
  late:      '#f59e0b',
  very_late: '#ef4444',
  unknown:   '#6b7280',
}

function formatDelay(s: number | null): string {
  if (s === null) return 'No schedule data'
  if (s < -60) return `${Math.abs(Math.round(s / 60))}m early`
  if (s <= 60)  return 'On time'
  const m = Math.floor(s / 60), sec = s % 60
  return sec > 0 ? `+${m}m ${sec}s late` : `+${m}m late`
}

function fmtGtfsSecs(secs: number): string {
  const h = Math.floor(secs / 3600) % 24
  const m = Math.floor((secs % 3600) / 60)
  const h12 = h % 12 || 12
  const ampm = h < 12 ? 'AM' : 'PM'
  return `${h12}:${String(m).padStart(2, '0')} ${ampm}`
}

export default function LivePageClient() {
  const { setContent } = useFilterPanel()
  const setContentRef = useRef(setContent)
  setContentRef.current = setContent

  const { setNavFilter } = useNav()

  // ── Scope ─────────────────────────────────────────────────────────────────
  const [scope, setScope] = useState<LiveScope>('group')

  // ── Routes metadata (from Render + Databricks) ────────────────────────────
  const [routeIds, setRouteIds]             = useState<string[]>([])
  const [routeNames, setRouteNames]         = useState<Map<string, string>>(new Map())
  const [routeTypes, setRouteTypes]         = useState<Map<string, number>>(new Map())
  const [routesLoading, setRoutesLoading]   = useState(true)
  const [routeNamesLoading, setRouteNamesLoading] = useState(false)

  // ── Group selection ───────────────────────────────────────────────────────
  const [groups, setGroups]                     = useState<string[]>([])
  const [groupsLoading, setGroupsLoading]       = useState(true)
  const [selectedGroupName, setSelectedGroupName] = useState<string | null>(null)
  const [groupRouteIds, setGroupRouteIds]       = useState<string[]>([])
  const [groupRoutesLoading, setGroupRoutesLoading] = useState(false)

  // ── Single-route selection ────────────────────────────────────────────────
  const [selectedRouteId, setSelectedRouteId] = useState<string | null>(null)

  // ── Selected vehicle ──────────────────────────────────────────────────────
  const [selectedVehicleId, setSelectedVehicleId] = useState<string | null>(null)
  const [tripStartTs, setTripStartTs]             = useState<string | null>(null)

  // ── Feed ──────────────────────────────────────────────────────────────────
  const [feed, setFeed]   = useState<FeedResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // ── Route stops: shapes (group/single) + stop dots (single only) ──────────
  const [shapePoints, setShapePoints] = useState<Array<RouteStop & { route_id?: string }>>([])
  const [stopPoints, setStopPoints]   = useState<RouteStop[]>([])

  const routeStops = useMemo<Array<RouteStop & { point_type?: string }>>(() => [
    ...shapePoints.map((r) => ({ ...r, point_type: 'shape' as const })),
    ...stopPoints.map((r)  => ({ ...r, point_type: 'stop'  as const })),
  ], [shapePoints, stopPoints])

  // ── Route IDs driving shapes: group routes OR single selected route ────────
  const shapeRouteIds = useMemo(() => {
    if (scope === 'group') return groupRouteIds
    if (scope === 'single' && selectedRouteId) return [selectedRouteId]
    return []
  }, [scope, groupRouteIds, selectedRouteId])

  // ── Route IDs to fetch vehicles for ───────────────────────────────────────
  const activeRouteIds = useMemo(() => {
    if (scope === 'group') return groupRouteIds
    if (scope === 'single') return selectedRouteId ? [selectedRouteId] : []
    return []
  }, [scope, groupRouteIds, selectedRouteId])

  // ── Derived DimRoute list (for display names) ─────────────────────────────
  const routes = useMemo<DimRoute[]>(
    () => routeIds.map((id) => {
      const name = routeNames.get(id)
      return {
        route_id:         id,
        route_short_name: id,
        route_long_name:  name ? name.split(' – ')[1] ?? '' : '',
        route_type:       routeTypes.get(id) ?? 3,
        route_color:      null,
      }
    }),
    [routeIds, routeNames, routeTypes]
  )

  // ── Route colors map ──────────────────────────────────────────────────────
  const routeColors = useMemo(() => {
    const map = new Map<string, string>()
    for (const id of shapeRouteIds) {
      map.set(id, getCuratedColor(routeTypes.get(id) ?? 3, id))
    }
    return map
  }, [shapeRouteIds, routeTypes])

  // ── Step 1: fetch active route IDs + group names on mount ─────────────────
  useEffect(() => {
    fetch(`${RENDER_API}/vehicles/routes`)
      .then((r) => r.json())
      .then((d: { route_ids: string[] }) => {
        setRouteIds(d.route_ids ?? [])
        setRouteNamesLoading(true)
      })
      .catch(() => {})
      .finally(() => setRoutesLoading(false))

    fetch('/api/databricks/query', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ sql: 'SELECT DISTINCT group_name FROM gold_route_groups ORDER BY group_name' }),
    })
      .then((r) => r.json())
      .then((d: { rows?: Array<{ group_name: string }> }) =>
        setGroups(d.rows?.map((r) => r.group_name) ?? [])
      )
      .catch(() => {})
      .finally(() => setGroupsLoading(false))
  }, [])

  // ── Step 2: enrich route names from Databricks ────────────────────────────
  useEffect(() => {
    if (routeIds.length === 0) return
    fetch('/api/databricks/query', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: `
          SELECT route_id, route_short_name, route_long_name, route_type
          FROM silver_dim_route
          WHERE route_id IN (${routeIds.map((id) => `'${id}'`).join(',')})
        `,
      }),
    })
      .then((r) => r.json())
      .then((d: { rows: Array<{ route_id: string; route_short_name: string; route_long_name: string; route_type: number }> }) => {
        const names = new Map<string, string>()
        const types = new Map<string, number>()
        for (const row of d.rows ?? []) {
          names.set(row.route_id, `${row.route_short_name} – ${row.route_long_name}`)
          if (row.route_type != null) types.set(row.route_id, row.route_type)
        }
        setRouteNames(names)
        setRouteTypes(types)
      })
      .catch(() => {})
      .finally(() => setRouteNamesLoading(false))
  }, [routeIds])

  // ── Fetch route IDs for selected group ────────────────────────────────────
  useEffect(() => {
    if (!selectedGroupName) { setGroupRouteIds([]); return }
    setGroupRoutesLoading(true)
    const safe = selectedGroupName.replace(/'/g, "''")
    fetch('/api/databricks/query', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ sql: `SELECT route_id FROM gold_route_groups WHERE group_name = '${safe}'` }),
    })
      .then((r) => r.json())
      .then((d: { rows?: Array<{ route_id: string }> }) =>
        setGroupRouteIds(d.rows?.map((r) => r.route_id) ?? [])
      )
      .catch(() => setGroupRouteIds([]))
      .finally(() => setGroupRoutesLoading(false))
  }, [selectedGroupName])

  // ── Warmup: ping Databricks on mount ─────────────────────────────────────
  useEffect(() => {
    fetch('/api/databricks/query', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ sql: 'SELECT 1' }),
    }).catch(() => {})
  }, [])

  // ── Shapes: load for the active shape route IDs ───────────────────────────
  useEffect(() => {
    if (!shapeRouteIds.length) { setShapePoints([]); return }
    const ids = shapeRouteIds.map((id) => `'${id.replace(/'/g, "''")}'`).join(',')
    fetch('/api/databricks/query', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: `
          WITH best_trip AS (
            SELECT t.route_id, t.shape_id, t.direction_id,
              ROW_NUMBER() OVER (PARTITION BY t.route_id, t.direction_id ORDER BY COUNT(*) DESC) AS rn
            FROM silver_dim_trip t
            JOIN silver_fact_stop_schedule ss ON t.trip_id = ss.trip_id
            WHERE t.route_id IN (${ids}) AND t.shape_id IS NOT NULL
            GROUP BY t.route_id, t.shape_id, t.direction_id
          )
          SELECT sp.shape_pt_sequence AS stop_sequence,
            sp.shape_id               AS stop_id,
            bt.route_id,
            CAST(NULL AS STRING)      AS stop_name,
            sp.shape_pt_lat           AS lat,
            sp.shape_pt_lon           AS lon,
            bt.direction_id
          FROM best_trip bt
          JOIN silver_fact_shape_points sp ON bt.shape_id = sp.shape_id
          WHERE bt.rn = 1
          ORDER BY bt.route_id, bt.direction_id, sp.shape_pt_sequence
        `,
      }),
    })
      .then((r) => r.json())
      .then((d: { rows?: Array<RouteStop & { route_id: string }> }) =>
        setShapePoints(d.rows ?? [])
      )
      .catch(() => setShapePoints([]))
  }, [shapeRouteIds])

  // ── Stop dots: only in single-route scope ─────────────────────────────────
  useEffect(() => {
    if (scope !== 'single' || !selectedRouteId) { setStopPoints([]); return }
    const sid = selectedRouteId.replace(/'/g, "''")
    fetch('/api/databricks/query', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: `
          WITH trip_stop_counts AS (
            SELECT t.trip_id, t.direction_id, COUNT(*) AS n
            FROM silver_dim_trip t
            JOIN silver_fact_stop_schedule ss ON t.trip_id = ss.trip_id
            WHERE t.route_id = '${sid}'
            GROUP BY t.trip_id, t.direction_id
          ),
          best_trip AS (
            SELECT trip_id, direction_id,
              ROW_NUMBER() OVER (PARTITION BY direction_id ORDER BY n DESC) AS rn
            FROM trip_stop_counts
          ),
          rep_trips AS (SELECT trip_id, direction_id FROM best_trip WHERE rn = 1)
          SELECT ss.stop_sequence, ss.stop_id, s.stop_name, s.lat, s.lon, rt.direction_id
          FROM rep_trips rt
          JOIN silver_fact_stop_schedule ss ON rt.trip_id = ss.trip_id
          JOIN silver_dim_stop s ON ss.stop_id = s.stop_id
          ORDER BY rt.direction_id, ss.stop_sequence
        `,
      }),
    })
      .then((r) => r.json())
      .then((d: { rows?: RouteStop[] }) => setStopPoints(d.rows ?? []))
      .catch(() => setStopPoints([]))
  }, [scope, selectedRouteId])

  // ── Fetch vehicles ────────────────────────────────────────────────────────
  const fetchVehicles = useCallback(async (_scope: LiveScope, ids: string[]) => {
    try {
      if (!ids.length) return
      const url = `${RENDER_API}/vehicles/multi?route_ids=${ids.join(',')}`
      const res = await fetch(url)
      if (!res.ok) throw new Error(`Feed error: ${res.status}`)
      const data: FeedResponse = await res.json()
      setFeed(data)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Feed unavailable')
    }
  }, [])

  // Start / restart polling when scope or active IDs change
  useEffect(() => {
    if (timerRef.current) clearInterval(timerRef.current)
    setFeed(null)
    setSelectedVehicleId(null)
    if (!activeRouteIds.length) return
    fetchVehicles(scope, activeRouteIds)
    timerRef.current = setInterval(() => fetchVehicles(scope, activeRouteIds), REFRESH_MS)
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [scope, activeRouteIds, fetchVehicles])

  // ── Trip start time for selected vehicle ──────────────────────────────────
  useEffect(() => {
    if (!selectedVehicleId) { setTripStartTs(null); return }
    const tripId = feed?.vehicles.find((v) => v.vehicle_id === selectedVehicleId)?.trip_id
    if (!tripId) { setTripStartTs(null); return }
    const safeId = tripId.replace(/'/g, "''")
    fetch('/api/databricks/query', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: `
          SELECT scheduled_arrival_secs
          FROM silver_fact_stop_schedule
          WHERE trip_id = '${safeId}'
          ORDER BY stop_sequence ASC
          LIMIT 1
        `,
      }),
    })
      .then((r) => r.json())
      .then((d: { rows?: Array<{ scheduled_arrival_secs: number }> }) => {
        const secs = d.rows?.[0]?.scheduled_arrival_secs
        setTripStartTs(secs != null ? fmtGtfsSecs(secs) : null)
      })
      .catch(() => setTripStartTs(null))
  }, [selectedVehicleId, feed?.vehicles])

  // ── Sync selection → nav context so sidebar links carry state ────────────
  useEffect(() => {
    setNavFilter({
      scope,
      groupName: scope === 'group' ? selectedGroupName : null,
      routeId:   scope === 'single' ? selectedRouteId : null,
    })
  }, [scope, selectedGroupName, selectedRouteId, setNavFilter])

  // ── Scope change handler ──────────────────────────────────────────────────
  function handleScopeChange(s: LiveScope) {
    setScope(s)
    if (s !== 'group') setSelectedGroupName(null)
    if (s !== 'single') setSelectedRouteId(null)
    setSelectedVehicleId(null)
  }

  // ── Derived counts ────────────────────────────────────────────────────────
  const otpCounts = feed?.vehicles.reduce(
    (acc, v) => { acc[v.otp_status] = (acc[v.otp_status] ?? 0) + 1; return acc },
    {} as Partial<Record<OtpStatus, number>>
  ) ?? {}

  const selectedVehicle = feed?.vehicles.find((v) => v.vehicle_id === selectedVehicleId) ?? null

  // ── Stats overlay label ───────────────────────────────────────────────────
  const statsLabel = useMemo(() => {
    if (scope === 'group') return selectedGroupName ?? null
    if (scope === 'single' && selectedRouteId) {
      return routeNames.get(selectedRouteId) ?? `Route ${selectedRouteId}`
    }
    return null
  }, [scope, selectedGroupName, selectedRouteId, routeNames])

  // ── Show empty state when a selection is required but not made ────────────
  const needsSelection =
    (scope === 'group' && !selectedGroupName) ||
    (scope === 'single' && !selectedRouteId)

  // ── Inject filter panel ───────────────────────────────────────────────────
  useEffect(() => {
    if (selectedVehicle) {
      setContentRef.current(
        <VehicleDetailPanel
          vehicle={selectedVehicle}
          tripStartTs={tripStartTs}
          onBack={() => setSelectedVehicleId(null)}
        />
      )
    } else {
      setContentRef.current(
        <LiveFilterPanel
          scope={scope}
          onScopeChange={handleScopeChange}
          groups={groups}
          groupsLoading={groupsLoading}
          selectedGroupName={selectedGroupName}
          onSelectGroup={(name) => { setSelectedGroupName(name) }}
          groupRoutesLoading={groupRoutesLoading}
          routeIds={routeIds}
          routeNames={routeNames}
          routeNamesLoading={routeNamesLoading}
          routesLoading={routesLoading}
          selectedRouteId={selectedRouteId}
          onSelectRoute={setSelectedRouteId}
        />
      )
    }
  }, [
    scope, groups, groupsLoading, selectedGroupName, groupRoutesLoading,
    routeIds, routeNames, routeNamesLoading, routesLoading,
    selectedRouteId, selectedVehicle, tripStartTs,
  ])

  return (
    <div className="flex flex-col h-full relative">

      {/* Empty state — only when a selection is required */}
      {needsSelection && (
        <div className="absolute inset-0 z-10 flex items-center justify-center pointer-events-none">
          <div className="bg-gray-900/90 border border-gray-700 rounded-2xl px-8 py-6 text-center backdrop-blur-sm shadow-2xl">
            <div className="w-12 h-12 rounded-full bg-blue-500/10 flex items-center justify-center mx-auto mb-3">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-6 h-6 text-blue-400">
                <circle cx="12" cy="12" r="3" fill="currentColor" stroke="none" />
                <path strokeLinecap="round" d="M6.3 6.3a8 8 0 000 11.4M17.7 6.3a8 8 0 010 11.4" />
              </svg>
            </div>
            <p className="text-white font-semibold mb-1">
              {scope === 'group' ? 'Select a route group to begin' : 'Select a route to begin'}
            </p>
            <p className="text-gray-400 text-sm">Use the Filters panel on the left</p>
          </div>
        </div>
      )}

      {/* Stats overlay */}
      {feed && statsLabel && (
        <div className="absolute top-4 right-4 z-10 bg-gray-900/95 border border-gray-700 rounded-xl shadow-2xl p-4 w-64 backdrop-blur-sm">
          <div className="flex items-center justify-between mb-0.5">
            <span className="text-white font-semibold text-sm truncate pr-2">{statsLabel}</span>
            <span className="flex items-center gap-1.5 shrink-0">
              <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
              <span className="text-emerald-400 text-xs">Live</span>
            </span>
          </div>
          <p className="text-gray-600 text-xs mb-2">
            {feed.vehicle_count} vehicle{feed.vehicle_count !== 1 ? 's' : ''}
          </p>

          <div className="space-y-1.5">
            {([
              ['on_time',   'On Time'],
              ['late',      'Late'],
              ['very_late', 'Very Late'],
              ['early',     'Early'],
              ['unknown',   'No Data'],
            ] as [OtpStatus, string][]).map(([status, label]) => {
              const count = otpCounts[status] ?? 0
              if (count === 0) return null
              return (
                <div key={status} className="flex items-center justify-between text-xs">
                  <div className="flex items-center gap-2">
                    <div className="w-2.5 h-2.5 rounded-full" style={{ background: OTP_COLOR[status] }} />
                    <span className="text-gray-300 whitespace-nowrap">{label}</span>
                  </div>
                  <span className="text-white font-medium">
                    {count} ({Math.round((count / feed.vehicle_count) * 100)}%)
                  </span>
                </div>
              )
            })}
          </div>

          {error && <p className="text-red-400 text-xs mt-2">{error}</p>}
        </div>
      )}

      <LiveMap
        vehicles={feed?.vehicles ?? []}
        fetchedAtMs={feed?.fetched_at_ms ?? null}
        routeStops={routeStops}
        routeColors={routeColors}
        selectedVehicleId={selectedVehicleId}
        onVehicleSelect={(v) => setSelectedVehicleId(v ? v.vehicle_id : null)}
      />
    </div>
  )
}

// ── Vehicle detail panel ───────────────────────────────────────────────────────

function VehicleDetailPanel({ vehicle, tripStartTs, onBack }: {
  vehicle: LiveVehicle
  tripStartTs: string | null
  onBack: () => void
}) {
  const color = OTP_COLOR[vehicle.otp_status]
  const speedMph = vehicle.speed_mps != null ? (vehicle.speed_mps * 2.237).toFixed(0) : null

  const rows: [string, string][] = [
    ['Route', vehicle.route_id  ?? '—'],
    ['To',    vehicle.headsign  ?? '—'],
    ['Start', tripStartTs ?? '…'],
    ['Speed', speedMph ? `${speedMph} mph` : '—'],
  ]

  return (
    <div className="flex flex-col gap-4">
      <button
        onClick={onBack}
        className="flex items-center gap-1.5 text-xs text-gray-400 hover:text-white transition-colors w-fit"
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-3.5 h-3.5">
          <path strokeLinecap="round" d="M19 12H5M12 5l-7 7 7 7" />
        </svg>
        Back to filters
      </button>

      <div>
        <span
          className="inline-block text-xs font-semibold px-2 py-0.5 rounded-full border capitalize mb-2"
          style={{ color, borderColor: `${color}55`, background: `${color}20` }}
        >
          {vehicle.otp_status.replace('_', ' ')}
        </span>
        <h2 className="text-white font-bold text-base leading-tight">
          Vehicle {vehicle.vehicle_id}
        </h2>
      </div>

      <div
        className="rounded-xl px-4 py-3 text-center border"
        style={{ background: `${color}10`, borderColor: `${color}30` }}
      >
        <p className="text-gray-400 text-xs mb-1 uppercase tracking-wide">Current Delay</p>
        <p className="font-bold text-2xl" style={{ color }}>
          {formatDelay(vehicle.delay_seconds)}
        </p>
      </div>

      <div className="space-y-2.5">
        {rows.map(([label, value]) => (
          <div key={label} className="flex items-start justify-between gap-3 text-xs">
            <span className="text-gray-500 shrink-0">{label}</span>
            <span className="text-gray-200 font-medium text-right leading-relaxed">{value}</span>
          </div>
        ))}
      </div>

      <div className="flex items-center gap-1.5 pt-1">
        <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
        <span className="text-gray-600 text-xs">Live — refreshes every 30s</span>
      </div>
    </div>
  )
}

// ── Filter panel ──────────────────────────────────────────────────────────────

function LiveFilterPanel({
  scope, onScopeChange,
  groups, groupsLoading, selectedGroupName, onSelectGroup,
  groupRoutesLoading,
  routeIds, routeNames, routeNamesLoading, routesLoading,
  selectedRouteId, onSelectRoute,
}: {
  scope: LiveScope
  onScopeChange: (s: LiveScope) => void
  groups: string[]
  groupsLoading: boolean
  selectedGroupName: string | null
  onSelectGroup: (name: string | null) => void
  groupRoutesLoading: boolean
  routeIds: string[]
  routeNames: Map<string, string>
  routeNamesLoading: boolean
  routesLoading: boolean
  selectedRouteId: string | null
  onSelectRoute: (id: string | null) => void
}) {
  const modeLabels: Record<LiveScope, string> = {
    group:  'Route Group',
    single: 'Single Route',
  }

  return (
    <>
      <FilterSection label="Scope">
        <div className="flex flex-col gap-0.5">
          {(['group', 'single'] as LiveScope[]).map((s) => (
            <button
              key={s}
              onClick={() => onScopeChange(s)}
              className={cn(
                'w-full text-left px-3 py-2 text-sm rounded-lg transition-colors font-medium',
                scope === s
                  ? 'bg-violet-600 text-white'
                  : 'text-gray-400 hover:text-white hover:bg-gray-800'
              )}
            >
              {modeLabels[s]}
            </button>
          ))}
        </div>
      </FilterSection>

      {scope === 'group' && (
        <FilterSection label={groupsLoading ? 'Group' : `Group (${groups.length})`}>
          <GroupPicker
            groups={groups}
            selectedGroupName={selectedGroupName}
            onSelect={onSelectGroup}
            loading={groupsLoading || groupRoutesLoading}
          />
        </FilterSection>
      )}

      {scope === 'single' && (
        <FilterSection label="Route">
          <RoutePicker
            routeIds={routeIds}
            routeNames={routeNames}
            routeNamesLoading={routeNamesLoading}
            routesLoading={routesLoading}
            selectedId={selectedRouteId}
            onSelect={onSelectRoute}
          />
        </FilterSection>
      )}
    </>
  )
}

// ── Group picker ──────────────────────────────────────────────────────────────

function GroupPicker({
  groups, selectedGroupName, onSelect, loading,
}: {
  groups: string[]
  selectedGroupName: string | null
  onSelect: (name: string | null) => void
  loading: boolean
}) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    function handler(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const filtered = groups.filter((g) => g.toLowerCase().includes(search.toLowerCase()))
  const label = selectedGroupName ?? (loading ? 'Loading groups…' : 'Select a group…')

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-left transition-colors hover:border-gray-600"
      >
        <span className={selectedGroupName ? 'text-white truncate pr-2' : 'text-gray-500'}>{label}</span>
        {loading ? (
          <svg className="w-4 h-4 text-gray-500 shrink-0 animate-spin" viewBox="0 0 24 24" fill="none">
            <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth={3} strokeOpacity={0.25} />
            <path d="M12 2a10 10 0 0 1 10 10" stroke="currentColor" strokeWidth={3} strokeLinecap="round" />
          </svg>
        ) : (
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-4 h-4 text-gray-500 shrink-0">
            <path strokeLinecap="round" d="M19 9l-7 7-7-7" />
          </svg>
        )}
      </button>

      {open && (
        <div className="absolute top-full left-0 right-0 mt-1 bg-gray-800 border border-gray-700 rounded-lg shadow-xl z-50 max-h-72 flex flex-col">
          <div className="p-2 border-b border-gray-700">
            <input
              autoFocus
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search groups…"
              className="w-full bg-gray-900 rounded px-2 py-1.5 text-sm text-white placeholder-gray-500 focus:outline-none"
            />
          </div>
          <div className="overflow-y-auto flex-1">
            {selectedGroupName && (
              <button
                onClick={() => { onSelect(null); setOpen(false) }}
                className="w-full text-left px-3 py-2 text-xs text-blue-400 hover:bg-gray-700 border-b border-gray-700"
              >
                Clear selection
              </button>
            )}
            {filtered.map((g) => (
              <button
                key={g}
                onClick={() => { onSelect(g); setOpen(false); setSearch('') }}
                className={`w-full text-left px-3 py-2 text-sm hover:bg-gray-700 transition-colors ${
                  g === selectedGroupName ? 'text-blue-300 bg-blue-600/10' : 'text-gray-300'
                }`}
              >
                {g}
              </button>
            ))}
            {filtered.length === 0 && (
              <p className="text-gray-600 text-xs text-center py-4">No groups found</p>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

// ── Route picker ──────────────────────────────────────────────────────────────

function RoutePicker({
  routeIds, routeNames, routeNamesLoading, routesLoading, selectedId, onSelect,
}: {
  routeIds: string[]
  routeNames: Map<string, string>
  routeNamesLoading: boolean
  routesLoading: boolean
  selectedId: string | null
  onSelect: (id: string | null) => void
}) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    function handler(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const routeList = routeIds.map((id) => {
    const name = routeNames.get(id)
    return { id, shortName: id, longName: name ? name.split(' – ')[1] ?? '' : '' }
  })
  const filtered = routeList.filter((r) => {
    const q = search.toLowerCase()
    return r.shortName.toLowerCase().includes(q) || r.longName.toLowerCase().includes(q)
  })

  const selected = routeList.find((r) => r.id === selectedId)
  const loading = routesLoading || (routeIds.length === 0 && routeNamesLoading)
  const label = selected
    ? `Route ${selected.shortName}${selected.longName ? ` – ${selected.longName}` : ''}`
    : loading ? 'Loading routes…' : 'Select a route…'

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-left transition-colors hover:border-gray-600"
      >
        <span className={selectedId ? 'text-white truncate pr-2' : 'text-gray-500'}>{label}</span>
        {loading ? (
          <svg className="w-4 h-4 text-gray-500 shrink-0 animate-spin" viewBox="0 0 24 24" fill="none">
            <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth={3} strokeOpacity={0.25} />
            <path d="M12 2a10 10 0 0 1 10 10" stroke="currentColor" strokeWidth={3} strokeLinecap="round" />
          </svg>
        ) : (
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-4 h-4 text-gray-500 shrink-0">
            <path strokeLinecap="round" d="M19 9l-7 7-7-7" />
          </svg>
        )}
      </button>

      {open && (
        <div className="absolute top-full left-0 right-0 mt-1 bg-gray-800 border border-gray-700 rounded-lg shadow-xl z-50 max-h-60 flex flex-col">
          <div className="p-2 border-b border-gray-700">
            <input
              autoFocus
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search routes…"
              className="w-full bg-gray-900 rounded px-2 py-1.5 text-sm text-white placeholder-gray-500 focus:outline-none"
            />
          </div>
          <div className="overflow-y-auto flex-1">
            {selectedId && (
              <button
                onClick={() => { onSelect(null); setOpen(false) }}
                className="w-full text-left px-3 py-2 text-xs text-blue-400 hover:bg-gray-700 border-b border-gray-700"
              >
                Clear selection
              </button>
            )}
            {filtered.map((r) => (
              <button
                key={r.id}
                onClick={() => { onSelect(r.id); setOpen(false); setSearch('') }}
                className={`w-full text-left px-3 py-2 text-sm flex items-center gap-2 hover:bg-gray-700 transition-colors ${
                  r.id === selectedId ? 'text-blue-300 bg-blue-600/10' : 'text-gray-300'
                }`}
              >
                <span className="font-semibold w-8 shrink-0">{r.shortName}</span>
                {r.longName && <span className="text-gray-500 text-xs truncate">{r.longName}</span>}
              </button>
            ))}
            {filtered.length === 0 && !loading && (
              <p className="text-gray-600 text-xs text-center py-4">No routes found</p>
            )}
            {routeNamesLoading && (
              <div className="flex items-center gap-1.5 px-3 py-2 border-t border-gray-700/60">
                <svg className="w-3 h-3 text-gray-600 animate-spin shrink-0" viewBox="0 0 24 24" fill="none">
                  <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth={3} strokeOpacity={0.25} />
                  <path d="M12 2a10 10 0 0 1 10 10" stroke="currentColor" strokeWidth={3} strokeLinecap="round" />
                </svg>
                <span className="text-gray-600 text-xs">Loading route names…</span>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
