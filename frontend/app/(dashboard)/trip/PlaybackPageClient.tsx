'use client'

import { useState, useEffect, useRef, useMemo } from 'react'
import { useSearchParams } from 'next/navigation'
import { useFilterPanel } from '@/lib/filter-panel-context'
import { useNav } from '@/lib/nav-context'
import PlaybackMap, { type PlaybackPoint, type PlaybackStop } from '@/components/map/PlaybackMap'
import PlaybackFilterPanel from '@/components/filters/PlaybackFilterPanel'
import type { TripListRow } from '@/components/filters/PlaybackFilterPanel'
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

function dbToMs(ts: string): number {
  return new Date(ts.replace(' ', 'T') + 'Z').getTime()
}

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
  return fmtPhoenix(dbToMs(ts), false)
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

function fmtDuration(ms: number): string {
  const s = Math.floor(ms / 1000)
  const m = Math.floor(s / 60)
  const h = Math.floor(m / 60)
  return h > 0
    ? `${h}:${String(m % 60).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`
    : `${m}:${String(s % 60).padStart(2, '0')}`
}

function binarySearch(points: PlaybackPoint[], ms: number): number {
  if (points.length === 0) return -1
  if (ms < points[0].tsMs) return -1
  let lo = 0, hi = points.length - 1
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1
    if (points[mid].tsMs <= ms) lo = mid
    else hi = mid - 1
  }
  return lo
}

// ── Types ──────────────────────────────────────────────────────────────────────

interface StopRow extends PlaybackStop {
  stop_sequence: number
  scheduled_arrival_ts: string | null
  actual_arrival_ts: string | null
  actual_arrival_ts_ms: number | null
  dwell_seconds: number | null
}

// User selections per track slot
interface TrackConfig {
  routeId: string | null
  selectedTripId: string | null
}

// Loaded/derived data per track slot
interface TrackDataState {
  tripList: TripListRow[]
  pathPoints: PlaybackPoint[]
  stopRows: StopRow[]
  vehicles: { vehicle_id: string; ping_count: number }[]
  loadingTrips: boolean
  loadingTrip: boolean
}

const MAX_TRACKS = 3
const SLOT_COLORS = ['#38BDF8', '#F59E0B', '#84CC16']

const SPEEDS = [8, 16, 32, 64] as const
type Speed = typeof SPEEDS[number]

function emptyData(): TrackDataState {
  return { tripList: [], pathPoints: [], stopRows: [], vehicles: [], loadingTrips: false, loadingTrip: false }
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

// ── Main component ─────────────────────────────────────────────────────────────

export default function PlaybackPageClient() {
  const { setContent } = useFilterPanel()
  const setContentRef = useRef(setContent)
  setContentRef.current = setContent

  const { setNavFilter } = useNav()

  const searchParams    = useSearchParams()
  const initRouteId     = searchParams.get('routeId')
  const initDate        = searchParams.get('serviceDate')
  const timeBucketParam = useRef(searchParams.get('timeBucket'))

  const [serviceDate, setServiceDate] = useState(() => initDate ?? todayMinus1())

  // Track configurations (user selections) — separate from loaded data
  const [configs, setConfigs] = useState<TrackConfig[]>([
    { routeId: initRouteId, selectedTripId: null },
  ])
  const [trackStates, setTrackStates] = useState<TrackDataState[]>([emptyData()])

  const [routes,        setRoutes]        = useState<DimRoute[]>([])
  const [routesLoading, setRoutesLoading] = useState(true)
  const [error,         setError]         = useState<string | null>(null)
  const [congestionByBucket, setCongestionByBucket] = useState<Map<number, CongestionHex[]>>(new Map())
  const [showTraffic, setShowTraffic] = useState(true)

  // Playback state
  const [isPlaying,  setIsPlaying]  = useState(false)
  const [playbackMs, setPlaybackMs] = useState(0)
  const [speed,      setSpeed]      = useState<Speed>(8)

  const isPlayingRef  = useRef(false)
  const playbackMsRef = useRef(0)
  const speedRef      = useRef<Speed>(8)
  const endMsRef      = useRef(0)
  const rafRef        = useRef<number | null>(null)

  useEffect(() => { isPlayingRef.current  = isPlaying  }, [isPlaying])
  useEffect(() => { playbackMsRef.current = playbackMs }, [playbackMs])
  useEffect(() => { speedRef.current      = speed      }, [speed])

  // Derived flags
  const hasAnyData = trackStates.some((s) => s.pathPoints.length > 0)
  const multiRoute  = trackStates.filter((s) => s.pathPoints.length > 0).length > 1

  const startMs = useMemo(() => {
    const firsts = trackStates.filter((s) => s.pathPoints.length > 0).map((s) => s.pathPoints[0].tsMs)
    return firsts.length > 0 ? Math.min(...firsts) : 0
  }, [trackStates])

  const endMs = useMemo(() => {
    const lasts = trackStates.filter((s) => s.pathPoints.length > 0).map((s) => s.pathPoints.at(-1)!.tsMs)
    return lasts.length > 0 ? Math.max(...lasts) : 0
  }, [trackStates])

  useEffect(() => { endMsRef.current = endMs }, [endMs])

  // Per-track current point index (binary search on shared clock)
  const currentIdxByTrack = useMemo(
    () => trackStates.map((s) => binarySearch(s.pathPoints, playbackMs)),
    [trackStates, playbackMs],
  )

  // Current stop index for single-route stop sidebar
  const primaryStops = trackStates[0]?.stopRows ?? []
  const currentStopIdx = useMemo(() => {
    let last = -1
    for (let i = 0; i < primaryStops.length; i++) {
      const ms = primaryStops[i].actual_arrival_ts_ms
      if (ms !== null && ms <= playbackMs) last = i
    }
    return last
  }, [primaryStops, playbackMs])

  // Current 15-min congestion bucket (keyed to primary route corridor)
  const currentCongestionHexes = useMemo<CongestionHex[]>(() => {
    if (congestionByBucket.size === 0) return []
    const bucketMs = Math.floor(playbackMs / 900_000) * 900_000
    return congestionByBucket.get(bucketMs) ?? []
  }, [congestionByBucket, playbackMs])

  // Nav context — reflect primary route
  useEffect(() => {
    setNavFilter({
      scope:         configs[0]?.routeId ? 'single' : null,
      groupName:     null,
      routeId:       configs[0]?.routeId ?? null,
      timepointOnly: false,
    })
  }, [configs, setNavFilter])

  // ── Load routes once ───────────────────────────────────────────────────────
  useEffect(() => {
    fetchJson(ROUTES_WITH_DATA_SQL)
      .then(setRoutes)
      .catch(() => {})
      .finally(() => setRoutesLoading(false))
  }, [])

  // ── Load trip lists when route selections or date change ──────────────────
  const routeIdsKey = configs.map((c) => c.routeId ?? '').join('|')
  useEffect(() => {
    let cancelled = false
    setTrackStates(configs.map((c) => ({ ...emptyData(), loadingTrips: !!c.routeId })))
    setIsPlaying(false)
    setError(null)

    configs.forEach((c, i) => {
      if (!c.routeId) return
      fetchJson(playbackTripListSql(c.routeId), { serviceDate })
        .then((rows) => {
          if (cancelled) return
          setTrackStates((prev) =>
            prev.map((s, j) => j === i ? { ...s, tripList: rows, loadingTrips: false } : s),
          )
        })
        .catch((e) => {
          if (cancelled) return
          setError(e.message)
          setTrackStates((prev) =>
            prev.map((s, j) => j === i ? { ...s, loadingTrips: false } : s),
          )
        })
    })

    return () => { cancelled = true }
  }, [routeIdsKey, serviceDate]) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Auto-select trip for primary track from deep-link timeBucket ──────────
  useEffect(() => {
    const tb       = timeBucketParam.current
    const tripList = trackStates[0]?.tripList ?? []
    if (tripList.length === 0 || !tb || configs[0]?.selectedTripId) return
    const targetMs = dbToMs(tb)
    let best = tripList[0], bestDiff = Infinity
    for (const t of tripList) {
      const diff = Math.abs(dbToMs(t.first_ts) - targetMs)
      if (diff < bestDiff) { bestDiff = diff; best = t }
    }
    setConfigs((prev) => prev.map((c, i) => i === 0 ? { ...c, selectedTripId: best.trip_id } : c))
  }, [trackStates]) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Load path + stops when trip selections change ─────────────────────────
  const tripIdsKey = configs.map((c) => c.selectedTripId ?? '').join('|')
  useEffect(() => {
    let cancelled = false
    setTrackStates((prev) =>
      prev.map((s, i) => ({
        ...s,
        pathPoints: [],
        stopRows:   [],
        vehicles:   [],
        loadingTrip: !!configs[i]?.selectedTripId,
      })),
    )
    setIsPlaying(false)
    setError(null)

    configs.forEach((c, i) => {
      if (!c.selectedTripId) return
      const params = { serviceDate }
      Promise.all([
        fetchJson(playbackPathSql(c.selectedTripId), params),
        fetchJson(playbackStopsSql(c.selectedTripId), params),
        fetchJson(playbackVehiclesSql(c.selectedTripId), params),
      ])
        .then(([pathRows, sRows, vRows]) => {
          if (cancelled) return
          const pts: PlaybackPoint[] = pathRows.map((r: any) => ({
            tsMs:    dbToMs(r.point_ts),
            lat:     Number(r.lat),
            lon:     Number(r.lon),
            bearing: r.bearing   != null ? Number(r.bearing)   : null,
            speed:   r.speed_mps != null ? Number(r.speed_mps) : null,
          }))
          const stops: StopRow[] = sRows.map((r: any) => ({
            stop_id:               r.stop_id,
            stop_name:             r.stop_name ?? null,
            stop_sequence:         Number(r.stop_sequence),
            lat:                   r.lat != null ? Number(r.lat) : null,
            lon:                   r.lon != null ? Number(r.lon) : null,
            scheduled_arrival_ts:  r.scheduled_arrival_ts ?? null,
            actual_arrival_ts:     r.actual_arrival_ts    ?? null,
            actual_arrival_ts_ms:  r.actual_arrival_ts ? dbToMs(r.actual_arrival_ts) : null,
            arrival_delay_seconds: r.arrival_delay_seconds != null ? Number(r.arrival_delay_seconds) : null,
            pickup_type:           Number(r.pickup_type ?? 0),
            dwell_seconds:         r.dwell_seconds != null ? Number(r.dwell_seconds) : null,
          }))
          const vehicles = vRows.map((r: any) => ({
            vehicle_id: String(r.vehicle_id),
            ping_count: Number(r.ping_count),
          }))

          setTrackStates((prev) =>
            prev.map((s, j) => j === i
              ? { ...s, pathPoints: pts, stopRows: stops, vehicles, loadingTrip: false }
              : s),
          )

          // Position scrubber at start of first loaded trip
          if (pts.length > 0 && i === 0) {
            const ms = pts[0].tsMs
            playbackMsRef.current = ms
            setPlaybackMs(ms)
          }
        })
        .catch((e) => {
          if (cancelled) return
          setError(e.message)
          setTrackStates((prev) =>
            prev.map((s, j) => j === i ? { ...s, loadingTrip: false } : s),
          )
        })
    })

    return () => { cancelled = true }
  }, [tripIdsKey, serviceDate]) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Load congestion for primary route corridor ────────────────────────────
  const primaryPathPoints = trackStates[0]?.pathPoints ?? []
  useEffect(() => {
    if (primaryPathPoints.length === 0) { setCongestionByBucket(new Map()); return }
    import('h3-js').then(({ latLngToCell }) => {
      const h3Set = new Set(primaryPathPoints.map((p) => latLngToCell(p.lat, p.lon, 9)))
      const sql   = trafficCongestionSql([...h3Set])
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
  }, [primaryPathPoints, serviceDate]) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Playback RAF loop ──────────────────────────────────────────────────────
  useEffect(() => {
    if (!isPlaying || !hasAnyData) {
      if (rafRef.current !== null) { cancelAnimationFrame(rafRef.current); rafRef.current = null }
      return
    }

    let lastT: DOMHighResTimeStamp | null = null

    function frame(t: DOMHighResTimeStamp) {
      if (!isPlayingRef.current) return
      if (lastT !== null) {
        const dt   = t - lastT
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
  }, [isPlaying, hasAnyData])

  // Scroll current stop into view
  const stopRefs = useRef<(HTMLDivElement | null)[]>([])
  useEffect(() => {
    if (currentStopIdx >= 0) {
      stopRefs.current[currentStopIdx]?.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
    }
  }, [currentStopIdx])

  // ── Track management handlers ──────────────────────────────────────────────
  function handleAddTrack() {
    if (configs.length >= MAX_TRACKS) return
    setConfigs((prev) => [...prev, { routeId: null, selectedTripId: null }])
    setTrackStates((prev) => [...prev, emptyData()])
  }

  function handleRemoveTrack(idx: number) {
    setConfigs((prev) => prev.filter((_, i) => i !== idx))
    setTrackStates((prev) => prev.filter((_, i) => i !== idx))
  }

  function handleRouteChange(idx: number, routeId: string | null) {
    setConfigs((prev) => prev.map((c, i) => i === idx ? { routeId, selectedTripId: null } : c))
  }

  function handleTripChange(idx: number, tripId: string | null) {
    setConfigs((prev) => prev.map((c, i) => i === idx ? { ...c, selectedTripId: tripId } : c))
  }

  // ── Play/pause ─────────────────────────────────────────────────────────────
  function togglePlay() {
    if (!hasAnyData) return
    if (!isPlaying && playbackMs >= endMsRef.current) {
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

  // ── Filter panel injection ─────────────────────────────────────────────────
  useEffect(() => {
    setContentRef.current(
      <PlaybackFilterPanel
        tracks={configs.map((c, i) => ({
          routeId:        c.routeId,
          selectedTripId: c.selectedTripId,
          tripList:       trackStates[i]?.tripList    ?? [],
          loadingTrips:   trackStates[i]?.loadingTrips ?? false,
          color:          SLOT_COLORS[i],
        }))}
        routes={routes}
        routesLoading={routesLoading}
        serviceDate={serviceDate}
        onServiceDateChange={setServiceDate}
        onRouteChange={handleRouteChange}
        onTripChange={handleTripChange}
        onAddTrack={handleAddTrack}
        onRemoveTrack={handleRemoveTrack}
      />,
    )
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [configs, trackStates, routes, routesLoading, serviceDate])

  // ── Computed display values ────────────────────────────────────────────────
  const headerTitle = configs
    .filter((c) => c.routeId)
    .map((c) => routes.find((r) => r.route_id === c.routeId)?.route_short_name)
    .filter(Boolean)
    .join(' + ')

  const isLoading       = trackStates.some((s) => s.loadingTrip)
  const noRoutes        = configs.every((c) => !c.routeId)
  const noTrips         = configs.every((c) => !c.selectedTripId)
  const currentTs       = hasAnyData ? fmtPhoenix(playbackMs) : '—'
  const elapsedMs       = playbackMs - startMs
  const totalMs         = endMs - startMs
  const primaryVehicles = trackStates[0]?.vehicles ?? []

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div className="flex flex-col h-full">

      {/* ── Header ───────────────────────────────────────────────────────── */}
      <div className="px-6 pt-5 pb-3 border-b border-gray-800 flex flex-col gap-3">
        <div>
          <h1 className="text-xl font-semibold text-white">Trip Playback</h1>
          <p className="text-sm text-gray-400 mt-0.5">
            {headerTitle
              ? `Route${headerTitle.includes('+') ? 's' : ''} ${headerTitle} · ${serviceDate}`
              : 'Select a route in the filter panel to begin'}
          </p>
        </div>
        {error && <p className="text-xs text-red-400">{error}</p>}
      </div>

      {/* ── Body: map + stop sidebar ──────────────────────────────────────── */}
      <div className="flex flex-1 overflow-hidden min-h-0">

        {/* Map */}
        <div className="flex-1 relative">
          {isLoading && (
            <div className="absolute inset-0 z-10 flex items-center justify-center bg-gray-950/60">
              <div className="w-8 h-8 border-2 border-violet-500 border-t-transparent rounded-full animate-spin" />
            </div>
          )}
          {noRoutes && !isLoading && (
            <div className="absolute inset-0 z-10 flex items-center justify-center">
              <p className="text-gray-600 text-sm">Select a route in the filter panel</p>
            </div>
          )}
          {!noRoutes && noTrips && !isLoading && (
            <div className="absolute inset-0 z-10 flex items-center justify-center">
              <p className="text-gray-600 text-sm">Select trips in the filter panel</p>
            </div>
          )}

          <PlaybackMap
            tracks={trackStates.map((s, i) => ({
              points:     s.pathPoints,
              currentIdx: currentIdxByTrack[i] ?? -1,
              stops:      s.stopRows,
              color:      SLOT_COLORS[i],
            }))}
            congestionHexes={currentCongestionHexes}
            showTraffic={showTraffic}
          />

          {/* Traffic toggle */}
          {hasAnyData && (
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

        {/* Stop list sidebar — single-route mode only */}
        {!multiRoute && primaryStops.length > 0 && (
          <div className="w-72 border-l border-gray-800 flex flex-col overflow-hidden bg-gray-950">
            <div className="px-3 py-2 border-b border-gray-800 flex-shrink-0">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide">Stops</p>
              {primaryVehicles.length > 0 && (
                <p
                  className="text-xs text-gray-600 mt-0.5"
                  title={primaryVehicles.length > 1
                    ? `Vehicle handoff — ${primaryVehicles.map((v) => `#${v.vehicle_id} (${v.ping_count} pings)`).join(', ')}`
                    : undefined}
                >
                  {primaryVehicles.length > 1 ? (
                    <span className="text-amber-600">
                      Vehicles {primaryVehicles.map((v) => `#${v.vehicle_id}`).join(' → ')}
                    </span>
                  ) : (
                    `Vehicle #${primaryVehicles[0].vehicle_id}`
                  )}
                </p>
              )}
            </div>
            <div className="overflow-y-auto flex-1 min-h-0">
              {primaryStops.map((s, i) => {
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
                    <span className="text-xs text-gray-600 w-5 shrink-0 pt-0.5 text-right">{i + 1}</span>
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
          disabled={!hasAnyData}
          className="w-9 h-9 rounded-full bg-violet-600 hover:bg-violet-500 disabled:bg-gray-700 disabled:cursor-not-allowed flex items-center justify-center transition-colors flex-shrink-0"
          aria-label={isPlaying ? 'Pause' : 'Play'}
        >
          {isPlaying ? <PauseIcon /> : <PlayIcon />}
        </button>

        {/* Time display */}
        <span className="text-xs text-gray-400 font-mono shrink-0 w-28">
          {hasAnyData ? currentTs : '—'}
        </span>

        {/* Scrubber */}
        <div className="flex-1 flex items-center gap-2">
          <span className="text-xs text-gray-600 font-mono shrink-0">
            {hasAnyData ? fmtDuration(elapsedMs) : '0:00'}
          </span>
          <input
            type="range"
            min={startMs}
            max={endMs || startMs + 1}
            value={playbackMs}
            step={1000}
            onChange={handleScrub}
            disabled={!hasAnyData}
            className="flex-1 accent-violet-500 disabled:opacity-30 disabled:cursor-not-allowed"
          />
          <span className="text-xs text-gray-600 font-mono shrink-0">
            {hasAnyData ? fmtDuration(totalMs) : '0:00'}
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
