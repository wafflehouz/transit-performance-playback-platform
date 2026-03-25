'use client'

import { useState, useEffect, useRef, useMemo } from 'react'
import { useSearchParams } from 'next/navigation'
import { useFilterPanel } from '@/lib/filter-panel-context'
import RouteFilterPanel, {
  type OtpFilterState,
  type DatePreset,
} from '@/components/filters/RouteFilterPanel'
import PlaybackMap, { type PlaybackPoint, type PlaybackStop } from '@/components/map/PlaybackMap'
import { playbackTripListSql, playbackPathSql, playbackStopsSql, playbackVehiclesSql, trafficCongestionSql } from '@/lib/queries/playback'
import { ROUTES_WITH_DATA_SQL } from '@/lib/queries/otp'
import type { DimRoute, CongestionHex } from '@/types'
import { cn } from '@/lib/utils'

// ── Helpers ────────────────────────────────────────────────────────────────────

function todayMinus1(): string {
  const d = new Date()
  d.setDate(d.getDate() - 1)
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

// UTC timestamp (ms) → Phoenix local HH:MM:SS (UTC-7, no DST)
function fmtPhoenix(ms: number, withSeconds = true): string {
  const d = new Date(ms - 7 * 3600 * 1000)
  const h = d.getUTCHours()
  const m = String(d.getUTCMinutes()).padStart(2, '0')
  const ampm = h >= 12 ? 'PM' : 'AM'
  const h12 = String(h % 12 || 12)
  if (!withSeconds) return `${h12}:${m} ${ampm}`
  const s = String(d.getUTCSeconds()).padStart(2, '0')
  return `${h12}:${m}:${s} ${ampm}`
}

function fmtTimestamp(ts: string | null): string {
  if (!ts) return '—'
  return fmtPhoenix(new Date(ts).getTime(), false)
}

function fmtDelay(s: number | null, _pickupType: number): string {
  if (s === null) return '—'
  const abs = Math.abs(s)
  const m = Math.floor(abs / 60)
  const sec = abs % 60
  const mmss = `${m}:${String(sec).padStart(2, '0')}`
  return s < 0 ? `-${mmss}` : `+${mmss}`
}

function delayColor(s: number | null, pickupType: number): string {
  if (s === null) return 'text-gray-500'
  if (s < -60 && pickupType === 0) return 'text-pink-400'
  if (s <= 360) return 'text-emerald-400'
  if (s <= 600) return 'text-amber-400'
  return 'text-red-400'
}

// VM route color (matches OTP page)
const VM_NAME_COLORS: Record<string, string> = {
  'A': '#38BDF8', 'B': '#F59E0B', 'S': '#84CC16', 'SKYT': '#94A3B8',
}
function routeColor(route: DimRoute | undefined): string | null {
  if (!route) return null
  return VM_NAME_COLORS[route.route_short_name.toUpperCase().trim()] ?? '#A855F7'
}

async function fetchJson(sql: string, params: Record<string, string> = {}): Promise<any[]> {
  const res = await fetch('/api/databricks/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, params }),
  })
  const data = await res.json()
  if (data.error) throw new Error(data.error)
  return data.rows ?? []
}

// ── Types ──────────────────────────────────────────────────────────────────────

interface TripListRow {
  trip_id: string
  direction_id: number | null
  trip_headsign: string | null
  first_ts: string
  last_ts: string
  point_count: number
  first_stop_scheduled_ts: string | null
}

interface StopRow extends PlaybackStop {
  stop_sequence: number
  scheduled_arrival_ts: string | null
  actual_arrival_ts: string | null
  actual_arrival_ts_ms: number | null  // pre-parsed for scrubber sync
  dwell_seconds: number | null
}

const SPEEDS = [8, 16, 32, 64] as const
type Speed = typeof SPEEDS[number]

// ── Main component ─────────────────────────────────────────────────────────────

export default function PlaybackPageClient() {
  const { setContent } = useFilterPanel()
  const setContentRef = useRef(setContent)
  setContentRef.current = setContent

  // Deep-link params from anomaly drawer (routeId, serviceDate, directionId, timeBucket)
  const searchParams = useSearchParams()
  const initRouteId    = searchParams.get('routeId')
  const initDate       = searchParams.get('serviceDate')
  const initDirectionId = searchParams.get('directionId')
  const timeBucketParam = useRef(searchParams.get('timeBucket'))  // read once; drives auto-selection

  const [serviceDate, setServiceDate] = useState(() => initDate ?? todayMinus1())
  const [filters, setFilters] = useState<OtpFilterState>(() => ({
    mode: 'single',
    routeId: initRouteId,
    groupName: null,
    startDate: initDate ?? todayMinus1(),
    endDate:   initDate ?? todayMinus1(),
    direction: initDirectionId != null ? (Number(initDirectionId) as 0 | 1) : 'both',
    timepointOnly: false,
  }))
  const [preset, setPreset] = useState<DatePreset>('1d')

  const [routes, setRoutes] = useState<DimRoute[]>([])
  const [tripList, setTripList] = useState<TripListRow[]>([])
  const [selectedTripId, setSelectedTripId] = useState<string | null>(null)
  const [pathPoints, setPathPoints] = useState<PlaybackPoint[]>([])
  const [stopRows, setStopRows] = useState<StopRow[]>([])

  const [vehicles,           setVehicles]           = useState<{ vehicle_id: string; ping_count: number }[]>([])
  const [loadingTrips,       setLoadingTrips]       = useState(false)
  const [loadingTrip,        setLoadingTrip]        = useState(false)
  const [error,              setError]              = useState<string | null>(null)
  const [congestionByBucket, setCongestionByBucket] = useState<Map<number, CongestionHex[]>>(new Map())
  const [showTraffic,        setShowTraffic]        = useState(true)

  // Playback state
  const [isPlaying,   setIsPlaying]   = useState(false)
  const [playbackMs,  setPlaybackMs]  = useState(0)
  const [speed,       setSpeed]       = useState<Speed>(8)

  // Refs for RAF loop (avoids stale closures)
  const isPlayingRef  = useRef(false)
  const playbackMsRef = useRef(0)
  const speedRef      = useRef<Speed>(8)
  const endMsRef      = useRef(0)
  const rafRef        = useRef<number | null>(null)

  useEffect(() => { isPlayingRef.current  = isPlaying  }, [isPlaying])
  useEffect(() => { playbackMsRef.current = playbackMs }, [playbackMs])
  useEffect(() => { speedRef.current      = speed      }, [speed])

  const activeRouteId = filters.mode === 'single' ? filters.routeId : null
  const selectedRoute = routes.find((r) => r.route_id === activeRouteId)

  // ── Load routes once ───────────────────────────────────────────────────────
  useEffect(() => {
    fetchJson(ROUTES_WITH_DATA_SQL).then(setRoutes).catch(() => {})
  }, [])

  // ── Load trip list when route/date changes ─────────────────────────────────
  useEffect(() => {
    if (!activeRouteId) { setTripList([]); setSelectedTripId(null); return }
    setLoadingTrips(true)
    setTripList([])
    setSelectedTripId(null)
    fetchJson(playbackTripListSql(activeRouteId), { serviceDate })
      .then((rows) => setTripList(rows))
      .catch((e) => setError(e.message))
      .finally(() => setLoadingTrips(false))
  }, [activeRouteId, serviceDate])

  // ── Auto-select trip from deep-link timeBucket param ──────────────────────
  // Runs once when tripList first populates after a deep-link navigation.
  useEffect(() => {
    const tb = timeBucketParam.current
    if (tripList.length === 0 || !tb || selectedTripId) return
    const targetMs = new Date(tb).getTime()
    let best = tripList[0]
    let bestDiff = Infinity
    for (const t of tripList) {
      const firstMs = new Date(t.first_ts).getTime()
      const lastMs  = new Date(t.last_ts).getTime()
      // Exact: trip was active at this time
      if (firstMs <= targetMs && targetMs <= lastMs) { best = t; break }
      const diff = Math.min(Math.abs(firstMs - targetMs), Math.abs(lastMs - targetMs))
      if (diff < bestDiff) { bestDiff = diff; best = t }
    }
    setSelectedTripId(best.trip_id)
  }, [tripList]) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Load path + stops when trip changes ───────────────────────────────────
  useEffect(() => {
    if (!selectedTripId) {
      setPathPoints([])
      setStopRows([])
      setIsPlaying(false)
      return
    }

    setLoadingTrip(true)
    setPathPoints([])
    setStopRows([])
    setVehicles([])
    setIsPlaying(false)
    setError(null)

    const params = { serviceDate }
    Promise.all([
      fetchJson(playbackPathSql(selectedTripId), params),
      fetchJson(playbackStopsSql(selectedTripId), params),
      fetchJson(playbackVehiclesSql(selectedTripId), params),
    ])
      .then(([pathRows, sRows, vRows]) => {
        setVehicles(vRows.map((r: any) => ({ vehicle_id: String(r.vehicle_id), ping_count: Number(r.ping_count) })))
        const pts: PlaybackPoint[] = pathRows.map((r: any) => ({
          tsMs: new Date(r.point_ts).getTime(),
          lat: Number(r.lat),
          lon: Number(r.lon),
          bearing: r.bearing != null ? Number(r.bearing) : null,
          speed: r.speed_mps != null ? Number(r.speed_mps) : null,
        }))

        const stops: StopRow[] = sRows.map((r: any) => ({
          stop_id: r.stop_id,
          stop_name: r.stop_name ?? null,
          stop_sequence: Number(r.stop_sequence),
          lat: r.lat != null ? Number(r.lat) : null,
          lon: r.lon != null ? Number(r.lon) : null,
          scheduled_arrival_ts: r.scheduled_arrival_ts ?? null,
          actual_arrival_ts: r.actual_arrival_ts ?? null,
          actual_arrival_ts_ms: r.actual_arrival_ts ? new Date(r.actual_arrival_ts).getTime() : null,
          arrival_delay_seconds: r.arrival_delay_seconds != null ? Number(r.arrival_delay_seconds) : null,
          pickup_type: Number(r.pickup_type ?? 0),
          dwell_seconds: r.dwell_seconds != null ? Number(r.dwell_seconds) : null,
        }))

        setPathPoints(pts)
        setStopRows(stops)

        if (pts.length > 0) {
          const startMs = pts[0].tsMs
          endMsRef.current = pts[pts.length - 1].tsMs
          playbackMsRef.current = startMs
          setPlaybackMs(startMs)
        }
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoadingTrip(false))
  }, [selectedTripId, serviceDate])

  // ── Load congestion after path points are ready ───────────────────────────
  // Derives H3 indices from the VP path (client-side via h3-js), then queries
  // gold_route_segment_congestion for those hexes + service date.
  useEffect(() => {
    if (pathPoints.length === 0) { setCongestionByBucket(new Map()); return }
    import('h3-js').then(({ latLngToCell }) => {
      const h3Set = new Set(pathPoints.map((p) => latLngToCell(p.lat, p.lon, 9)))
      const h3Indices = [...h3Set]
      const sql = trafficCongestionSql(h3Indices)
      if (!sql) return
      fetchJson(sql, { serviceDate })
        .then((rows) => {
          if (rows.length === 0) return
          const byBucket = new Map<number, CongestionHex[]>()
          for (const r of rows) {
            const bucketMs = Number(r.bucket_ms)
            if (!byBucket.has(bucketMs)) byBucket.set(bucketMs, [])
            byBucket.get(bucketMs)!.push({
              h3_index:         r.h3_index,
              congestion_level: r.congestion_level,
              avg_speed_mps:    r.avg_speed_mps != null ? Number(r.avg_speed_mps) : null,
              p10_speed_mps:    r.p10_speed_mps != null ? Number(r.p10_speed_mps) : null,
            })
          }
          setCongestionByBucket(byBucket)
        })
        .catch(() => {})
    })
  }, [pathPoints, serviceDate])

  // ── Playback RAF loop ──────────────────────────────────────────────────────
  useEffect(() => {
    if (!isPlaying || pathPoints.length === 0) {
      if (rafRef.current !== null) { cancelAnimationFrame(rafRef.current); rafRef.current = null }
      return
    }

    let lastT: DOMHighResTimeStamp | null = null

    function frame(t: DOMHighResTimeStamp) {
      if (!isPlayingRef.current) return
      if (lastT !== null) {
        const dt  = t - lastT
        const next = playbackMsRef.current + dt * speedRef.current
        const end  = endMsRef.current
        if (next >= end) {
          playbackMsRef.current = end
          setPlaybackMs(end)
          setIsPlaying(false)
          isPlayingRef.current = false
          return
        }
        playbackMsRef.current = next
        setPlaybackMs(next)
      }
      lastT = t
      rafRef.current = requestAnimationFrame(frame)
    }

    rafRef.current = requestAnimationFrame(frame)
    return () => {
      if (rafRef.current !== null) { cancelAnimationFrame(rafRef.current); rafRef.current = null }
    }
  }, [isPlaying, pathPoints])

  // ── Current point index (binary search) ───────────────────────────────────
  const currentIdx = useMemo(() => {
    if (pathPoints.length === 0) return -1
    if (playbackMs < pathPoints[0].tsMs) return -1
    let lo = 0, hi = pathPoints.length - 1
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1
      if (pathPoints[mid].tsMs <= playbackMs) lo = mid
      else hi = mid - 1
    }
    return lo
  }, [pathPoints, playbackMs])

  // Current stop (last stop with actual arrival before or at playbackMs)
  const currentStopIdx = useMemo(() => {
    let last = -1
    for (let i = 0; i < stopRows.length; i++) {
      const ms = stopRows[i].actual_arrival_ts_ms
      if (ms !== null && ms <= playbackMs) last = i
    }
    return last
  }, [stopRows, playbackMs])

  // Current 15-min congestion bucket (O(1) Map lookup, runs at 60fps during playback)
  const currentCongestionHexes = useMemo<CongestionHex[]>(() => {
    if (congestionByBucket.size === 0) return []
    const bucketMs = Math.floor(playbackMs / 900_000) * 900_000
    return congestionByBucket.get(bucketMs) ?? []
  }, [congestionByBucket, playbackMs])

  // Scroll current stop into view
  const stopRefs = useRef<(HTMLDivElement | null)[]>([])
  useEffect(() => {
    if (currentStopIdx >= 0) {
      stopRefs.current[currentStopIdx]?.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
    }
  }, [currentStopIdx])

  // ── Filter panel sidebar content ───────────────────────────────────────────
  // Deps: every value rendered into the panel JSX — avoids re-running on 60fps RAF renders
  useEffect(() => {
    setContentRef.current(
      <RouteFilterPanel
        filters={filters}
        onChange={(f) => setFilters({ ...f, mode: 'single', groupName: null })}
        routes={routes}
        groups={[]}
        showDirection={false}
        activePreset={preset}
        onPresetChange={setPreset}
        scheduleDate={serviceDate}
        onScheduleDateChange={(d) => setServiceDate(d)}
      />
    )
  }, [filters, routes, preset, serviceDate]) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Trip selector helpers ──────────────────────────────────────────────────
  function dirLabel(id: number | null): string {
    if (id === 0) return 'Outbound'
    if (id === 1) return 'Inbound'
    return ''
  }

  function tripLabel(t: TripListRow): string {
    const ts   = t.first_stop_scheduled_ts ?? t.first_ts
    const time = fmtPhoenix(new Date(ts).getTime(), false)
    const head = t.trip_headsign ? `To ${t.trip_headsign}` : ''
    return [time, head].filter(Boolean).join(' · ')
  }

  // ── Play/pause ─────────────────────────────────────────────────────────────
  function togglePlay() {
    if (pathPoints.length === 0) return
    // If at end, restart from beginning
    if (!isPlaying && playbackMs >= endMsRef.current) {
      const startMs = pathPoints[0].tsMs
      playbackMsRef.current = startMs
      setPlaybackMs(startMs)
    }
    setIsPlaying((p) => !p)
  }

  function handleScrub(e: React.ChangeEvent<HTMLInputElement>) {
    const val = Number(e.target.value)
    playbackMsRef.current = val
    setPlaybackMs(val)
  }

  // ── Computed display values ────────────────────────────────────────────────
  const startMs   = pathPoints.length > 0 ? pathPoints[0].tsMs : 0
  const endMs     = pathPoints.length > 0 ? pathPoints[pathPoints.length - 1].tsMs : 0
  const currentTs = pathPoints.length > 0 ? fmtPhoenix(playbackMs) : '—'
  const elapsedMs = playbackMs - startMs
  const totalMs   = endMs - startMs
  function fmtDuration(ms: number): string {
    const s = Math.floor(ms / 1000)
    const m = Math.floor(s / 60)
    const h = Math.floor(m / 60)
    return h > 0
      ? `${h}:${String(m % 60).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`
      : `${m}:${String(s % 60).padStart(2, '0')}`
  }

  const selectedTrip = tripList.find((t) => t.trip_id === selectedTripId)

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div className="flex flex-col h-full">

      {/* ── Header ───────────────────────────────────────────────────────── */}
      <div className="px-6 pt-5 pb-3 border-b border-gray-800 flex flex-col gap-3">
        <div>
          <h1 className="text-xl font-semibold text-white">Trip Playback</h1>
          <p className="text-sm text-gray-400 mt-0.5">
            {selectedRoute
              ? `Route ${selectedRoute.route_short_name} · ${serviceDate}`
              : 'Select a route and date to begin'}
          </p>
        </div>

        {/* Trip selector */}
        {activeRouteId && (
          <div className="flex items-center gap-3">
            <span className="text-xs text-gray-500 shrink-0">Trip</span>
            {loadingTrips ? (
              <div className="h-8 w-64 bg-gray-800 rounded animate-pulse" />
            ) : tripList.length === 0 ? (
              <span className="text-sm text-gray-500">No trips found for this route / date</span>
            ) : (
              <select
                value={selectedTripId ?? ''}
                onChange={(e) => setSelectedTripId(e.target.value || null)}
                className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-1.5 text-sm text-white focus:outline-none focus:ring-1 focus:ring-violet-500 min-w-72"
              >
                <option value="">Select a trip…</option>
                {tripList.map((t) => (
                  <option key={t.trip_id} value={t.trip_id}>{tripLabel(t)}</option>
                ))}
              </select>
            )}
            {selectedTrip && (
              <span className="text-xs text-gray-500">
                {selectedTrip.point_count} GPS points · {stopRows.length} stops
              </span>
            )}
          </div>
        )}

        {error && (
          <p className="text-xs text-red-400">{error}</p>
        )}
      </div>

      {/* ── Body: map + stop sidebar ──────────────────────────────────────── */}
      <div className="flex flex-1 overflow-hidden min-h-0">

        {/* Map */}
        <div className="flex-1 relative">
          {loadingTrip && (
            <div className="absolute inset-0 z-10 flex items-center justify-center bg-gray-950/60">
              <div className="w-8 h-8 border-2 border-violet-500 border-t-transparent rounded-full animate-spin" />
            </div>
          )}
          {!selectedTripId && !loadingTrip && (
            <div className="absolute inset-0 z-10 flex items-center justify-center">
              <p className="text-gray-600 text-sm">
                {!activeRouteId ? 'Select a route in the filter panel' : 'Select a trip above'}
              </p>
            </div>
          )}
          <PlaybackMap
            points={pathPoints}
            currentIdx={currentIdx}
            stops={stopRows}
            routeColor={routeColor(selectedRoute)}
            congestionHexes={currentCongestionHexes}
            showTraffic={showTraffic}
          />
          {/* Traffic toggle — floating over map corner, always visible when trip loaded */}
          {pathPoints.length > 0 && (
            <button
              onClick={() => congestionByBucket.size > 0 && setShowTraffic((v) => !v)}
              title={congestionByBucket.size === 0 ? 'No traffic data — run notebook 47 for this date' : undefined}
              className={cn(
                'absolute top-3 right-3 z-10 px-3 py-1.5 rounded-lg text-xs font-semibold border transition-colors',
                congestionByBucket.size === 0
                  ? 'bg-gray-900/80 border-gray-700 text-gray-600 cursor-default'
                  : showTraffic
                  ? 'bg-emerald-900/80 border-emerald-600 text-emerald-300 cursor-pointer'
                  : 'bg-gray-900/80 border-gray-700 text-gray-400 hover:text-white cursor-pointer',
              )}
            >
              <span className="flex items-center gap-1.5">
                <span className={cn(
                  'w-2 h-2 rounded-sm inline-block',
                  congestionByBucket.size === 0 ? 'bg-gray-700'
                    : showTraffic ? 'bg-emerald-400' : 'bg-gray-600',
                )} />
                Traffic{congestionByBucket.size === 0 ? ' (no data)' : ''}
              </span>
            </button>
          )}
        </div>

        {/* Stop list sidebar */}
        {stopRows.length > 0 && (
          <div className="w-72 border-l border-gray-800 flex flex-col overflow-hidden bg-gray-950">
            <div className="px-3 py-2 border-b border-gray-800 flex-shrink-0">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide">
                {selectedTrip
                  ? `${dirLabel(selectedTrip.direction_id) || 'Trip'} — ${selectedTrip.trip_headsign ?? ''}`
                  : 'Stops'}
              </p>
              {vehicles.length > 0 && (
                <p
                  className="text-xs text-gray-600 mt-0.5"
                  title={vehicles.length > 1 ? `Vehicle handoff detected — ${vehicles.map((v) => `#${v.vehicle_id} (${v.ping_count} pings)`).join(', ')}` : undefined}
                >
                  {vehicles.length > 1 ? (
                    <span className="text-amber-600">
                      Vehicles {vehicles.map((v) => `#${v.vehicle_id}`).join(' → ')}
                    </span>
                  ) : (
                    `Vehicle #${vehicles[0].vehicle_id}`
                  )}
                </p>
              )}
            </div>
            <div className="overflow-y-auto flex-1 min-h-0">
              {stopRows.map((s, i) => {
                const isCurrent = i === currentStopIdx
                const isPast    = s.actual_arrival_ts_ms !== null && s.actual_arrival_ts_ms <= playbackMs
                return (
                  <div
                    key={`${s.stop_id}-${i}`}
                    ref={(el) => { stopRefs.current[i] = el }}
                    className={cn(
                      'px-3 py-2 border-b border-gray-800/60 flex items-start gap-2 transition-colors',
                      isCurrent && 'bg-violet-950/40',
                      !isPast && !isCurrent && 'opacity-50',
                    )}
                  >
                    {/* Sequence number */}
                    <span className="text-xs text-gray-600 w-5 shrink-0 pt-0.5 text-right">{i + 1}</span>

                    {/* OTP dot */}
                    <span
                      className="w-2 h-2 rounded-full shrink-0 mt-1"
                      style={{ background: s.arrival_delay_seconds != null
                        ? (s.arrival_delay_seconds < -60 && s.pickup_type === 0 ? '#ec4899'
                          : s.arrival_delay_seconds <= 360 ? '#22c55e'
                          : s.arrival_delay_seconds <= 600 ? '#f59e0b'
                          : '#ef4444')
                        : '#6b7280' }}
                    />

                    <div className="flex-1 min-w-0">
                      <p className={cn('text-xs font-medium truncate', isCurrent ? 'text-white' : 'text-gray-300')}>
                        {s.stop_name ?? s.stop_id}
                      </p>
                      <div className="flex items-center gap-2 mt-0.5">
                        <span className="text-xs text-gray-600">
                          {fmtTimestamp(s.scheduled_arrival_ts)}
                        </span>
                        {s.actual_arrival_ts && (
                          <>
                            <span className="text-gray-700 text-xs">→</span>
                            <span className={cn('text-xs font-medium', delayColor(s.arrival_delay_seconds, s.pickup_type))}>
                              {fmtDelay(s.arrival_delay_seconds, s.pickup_type)}
                            </span>
                          </>
                        )}
                        {s.dwell_seconds != null && s.dwell_seconds > 0 && (
                          <span className="text-xs text-gray-600" title="Inferred dwell time">
                            {s.dwell_seconds >= 60
                              ? `${Math.floor(s.dwell_seconds / 60)}m${s.dwell_seconds % 60 > 0 ? `${s.dwell_seconds % 60}s` : ''} dwell`
                              : `${s.dwell_seconds}s dwell`}
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        )}
      </div>

      {/* ── Playback controls ─────────────────────────────────────────────── */}
      <div className="h-16 border-t border-gray-800 bg-gray-900 flex items-center px-4 gap-4 flex-shrink-0">

        {/* Play / Pause */}
        <button
          onClick={togglePlay}
          disabled={pathPoints.length === 0}
          className="w-9 h-9 rounded-full bg-violet-600 hover:bg-violet-500 disabled:bg-gray-700 disabled:cursor-not-allowed flex items-center justify-center transition-colors flex-shrink-0"
          aria-label={isPlaying ? 'Pause' : 'Play'}
        >
          {isPlaying ? <PauseIcon /> : <PlayIcon />}
        </button>

        {/* Time display */}
        <span className="text-xs text-gray-400 font-mono shrink-0 w-28">
          {pathPoints.length > 0 ? currentTs : '—'}
        </span>

        {/* Scrubber */}
        <div className="flex-1 flex items-center gap-2">
          <span className="text-xs text-gray-600 font-mono shrink-0">
            {pathPoints.length > 0 ? fmtDuration(elapsedMs) : '0:00'}
          </span>
          <input
            type="range"
            min={startMs}
            max={endMs || startMs + 1}
            value={playbackMs}
            step={1000}
            onChange={handleScrub}
            disabled={pathPoints.length === 0}
            className="flex-1 accent-violet-500 disabled:opacity-30 disabled:cursor-not-allowed"
          />
          <span className="text-xs text-gray-600 font-mono shrink-0">
            {pathPoints.length > 0 ? fmtDuration(totalMs) : '0:00'}
          </span>
        </div>

        {/* Speed selector */}
        <div className="flex rounded-lg overflow-hidden border border-gray-700 text-xs flex-shrink-0">
          {SPEEDS.map((s) => (
            <button
              key={s}
              onClick={() => setSpeed(s)}
              className={cn(
                'px-2.5 py-1.5 font-medium transition-colors',
                speed === s ? 'bg-violet-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white',
              )}
            >
              {s}×
            </button>
          ))}
        </div>
      </div>
    </div>
  )
}

// ── Icons ──────────────────────────────────────────────────────────────────────

function PlayIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" className="w-4 h-4 text-white ml-0.5">
      <path d="M8 5v14l11-7z" />
    </svg>
  )
}

function PauseIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" className="w-4 h-4 text-white">
      <path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z" />
    </svg>
  )
}
