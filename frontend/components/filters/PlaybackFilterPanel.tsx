'use client'

import { useRef } from 'react'
import { SingleRouteDropdown } from '@/components/filters/RouteFilterPanel'
import { FilterSection } from '@/components/ui/FilterControls'
import { cn } from '@/lib/utils'
import type { DimRoute } from '@/types'

export interface TripListRow {
  trip_id: string
  direction_id: number | null
  trip_headsign: string | null
  first_ts: string
  first_stop_scheduled_ts: string | null
  first_timepoint_scheduled_ts: string | null
}

export interface TrackConfig {
  routeId: string | null
  selectedTripId: string | null
  tripList: TripListRow[]
  loadingTrips: boolean
  tripCount: number
  color: string
}

interface Props {
  mode: 'trip' | 'window'
  onModeChange: (mode: 'trip' | 'window') => void
  tracks: TrackConfig[]
  routes: DimRoute[]
  routesLoading: boolean
  serviceDate: string
  windowStart: string
  windowEnd: string
  onServiceDateChange: (d: string) => void
  onWindowChange: (start: string, end: string) => void
  onRouteChange: (slotIdx: number, routeId: string | null) => void
  onTripChange: (slotIdx: number, tripId: string | null) => void
  onAddTrack: () => void
  onRemoveTrack: (slotIdx: number) => void
}

function dbToMs(ts: string): number {
  return new Date(ts.replace(' ', 'T') + 'Z').getTime()
}

function fmtPhoenixShort(ms: number): string {
  const d = new Date(ms - 7 * 3600 * 1000)
  const h  = d.getUTCHours()
  const m  = String(d.getUTCMinutes()).padStart(2, '0')
  const ap = h >= 12 ? 'PM' : 'AM'
  return `${h % 12 || 12}:${m} ${ap}`
}

function tripLabel(t: TripListRow): string {
  const ts   = t.first_timepoint_scheduled_ts ?? t.first_stop_scheduled_ts ?? t.first_ts
  const time = fmtPhoenixShort(dbToMs(ts))
  const head = t.trip_headsign ? `To ${t.trip_headsign}` : ''
  return [time, head].filter(Boolean).join(' · ')
}

function fmtDateDisplay(dateStr: string): string {
  return new Date(dateStr + 'T12:00:00Z').toLocaleDateString('en-US', {
    month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC',
  })
}

function adjustDate(dateStr: string, days: number): string {
  const d = new Date(dateStr + 'T12:00:00Z')
  d.setUTCDate(d.getUTCDate() + days)
  return d.toISOString().slice(0, 10)
}

const MAX_TRACKS = 3

export default function PlaybackFilterPanel({
  mode,
  onModeChange,
  tracks,
  routes,
  routesLoading,
  serviceDate,
  windowStart,
  windowEnd,
  onServiceDateChange,
  onWindowChange,
  onRouteChange,
  onTripChange,
  onAddTrack,
  onRemoveTrack,
}: Props) {
  const dateInputRef = useRef<HTMLInputElement>(null)

  return (
    <>
      {/* Mode toggle */}
      <FilterSection label="Mode">
        <div className="flex rounded-lg overflow-hidden border border-gray-700 text-xs">
          <button
            onClick={() => onModeChange('trip')}
            className={cn(
              'flex-1 py-2 font-medium transition-colors',
              mode === 'trip'
                ? 'bg-violet-600 text-white'
                : 'bg-gray-800 text-gray-400 hover:text-white',
            )}
          >
            Trip
          </button>
          <button
            onClick={() => onModeChange('window')}
            className={cn(
              'flex-1 py-2 font-medium transition-colors',
              mode === 'window'
                ? 'bg-violet-600 text-white'
                : 'bg-gray-800 text-gray-400 hover:text-white',
            )}
          >
            Window
          </button>
        </div>
        <p className="text-xs text-gray-600 leading-snug">
          {mode === 'trip'
            ? 'Follow a single trip with stop-by-stop detail'
            : 'All vehicles on selected routes during a time window'}
        </p>
      </FilterSection>

      {/* Service date — styled prev/next + click-to-open */}
      <FilterSection label="Service Date">
        <div className="flex items-center gap-1">
          <button
            onClick={() => onServiceDateChange(adjustDate(serviceDate, -1))}
            className="w-7 h-7 flex items-center justify-center rounded-lg bg-gray-800 border border-gray-700 text-gray-400 hover:text-white hover:border-gray-600 transition-colors text-sm leading-none"
          >
            ‹
          </button>
          <div className="relative flex-1">
            <button
              onClick={() => {
                const input = dateInputRef.current as (HTMLInputElement & { showPicker?: () => void }) | null
                input?.showPicker?.()
              }}
              className="w-full bg-gray-800 border border-gray-700 rounded-lg px-2 py-1.5 text-white text-xs text-center hover:border-gray-600 transition-colors"
            >
              {fmtDateDisplay(serviceDate)}
            </button>
            <input
              ref={dateInputRef}
              type="date"
              value={serviceDate}
              onChange={(e) => onServiceDateChange(e.target.value)}
              className="absolute inset-0 opacity-0 pointer-events-none"
              tabIndex={-1}
            />
          </div>
          <button
            onClick={() => onServiceDateChange(adjustDate(serviceDate, 1))}
            className="w-7 h-7 flex items-center justify-center rounded-lg bg-gray-800 border border-gray-700 text-gray-400 hover:text-white hover:border-gray-600 transition-colors text-sm leading-none"
          >
            ›
          </button>
        </div>
      </FilterSection>

      {/* Route slots */}
      <FilterSection label="Routes">
        <div className="flex flex-col gap-4">
          {tracks.map((track, idx) => (
            <div key={idx} className="flex flex-col gap-1.5">
              {/* Slot header */}
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span
                    className="w-3 h-3 rounded-full shrink-0 border border-gray-700"
                    style={{ background: track.color }}
                  />
                  <span className="text-xs font-semibold text-gray-400 uppercase tracking-wide">
                    Route {idx + 1}
                  </span>
                  {mode === 'window' && track.routeId && (
                    <span className="text-xs text-gray-500">
                      {track.loadingTrips
                        ? '…'
                        : track.tripCount > 0
                        ? `${track.tripCount} trip${track.tripCount !== 1 ? 's' : ''}`
                        : windowStart && windowEnd
                        ? 'no trips'
                        : ''}
                    </span>
                  )}
                </div>
                {tracks.length > 1 && (
                  <button
                    onClick={() => onRemoveTrack(idx)}
                    className="text-xs text-gray-600 hover:text-red-400 transition-colors px-1"
                    aria-label={`Remove route ${idx + 1}`}
                  >
                    ✕
                  </button>
                )}
              </div>

              {/* Route dropdown */}
              <SingleRouteDropdown
                routes={routes}
                selected={track.routeId}
                onSelect={(id) => onRouteChange(idx, id)}
                loading={routesLoading}
              />

              {/* Trip dropdown — trip mode only */}
              {mode === 'trip' && track.routeId && (
                <div>
                  {track.loadingTrips ? (
                    <div className="h-8 bg-gray-800 rounded animate-pulse" />
                  ) : track.tripList.length === 0 ? (
                    <p className="text-xs text-gray-600 px-1">No trips found for this date</p>
                  ) : (
                    <select
                      value={track.selectedTripId ?? ''}
                      onChange={(e) => onTripChange(idx, e.target.value || null)}
                      className="w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-1.5 text-xs text-white focus:outline-none focus:ring-1 focus:ring-violet-500"
                    >
                      <option value="">Select a trip…</option>
                      {track.tripList.map((t) => (
                        <option key={t.trip_id} value={t.trip_id}>
                          {tripLabel(t)}
                        </option>
                      ))}
                    </select>
                  )}
                </div>
              )}
            </div>
          ))}

          {tracks.length < MAX_TRACKS && (
            <button
              onClick={onAddTrack}
              className={cn(
                'w-full py-2 rounded-lg border border-dashed text-xs font-medium transition-colors',
                'border-gray-700 text-gray-500 hover:border-violet-600 hover:text-violet-400',
              )}
            >
              + Add Route
            </button>
          )}
        </div>
      </FilterSection>

      {/* Time window — window mode only */}
      {mode === 'window' && (
        <FilterSection label="Time Window">
          <div className="space-y-3">
            <TimeSlider
              label="Start"
              value={windowStart}
              onChange={(v) => onWindowChange(v, windowEnd)}
            />
            <TimeSlider
              label="End"
              value={windowEnd}
              onChange={(v) => onWindowChange(windowStart, v)}
            />
          </div>
          <p className="text-xs text-gray-600 mt-2 leading-snug">Phoenix local time</p>
        </FilterSection>
      )}
    </>
  )
}

// ── Time slider ────────────────────────────────────────────────────────────────
// 96 steps × 15 min = 00:00 → 23:45

function sliderToHHMM(val: number): string {
  const totalMins = val * 15
  const h = Math.floor(totalMins / 60)
  const m = totalMins % 60
  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`
}

function hhmmToSlider(hhmm: string): number {
  if (!hhmm) return 0
  const [h, m] = hhmm.split(':').map(Number)
  return Math.round((h * 60 + (m || 0)) / 15)
}

function fmtTimeDisplay(hhmm: string): string {
  if (!hhmm) return '—'
  const [h, m] = hhmm.split(':').map(Number)
  const ap = h >= 12 ? 'PM' : 'AM'
  return `${h % 12 || 12}:${String(m).padStart(2, '0')} ${ap}`
}

function TimeSlider({
  label,
  value,
  onChange,
}: {
  label: string
  value: string
  onChange: (v: string) => void
}) {
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between">
        <span className="text-xs text-gray-500">{label}</span>
        <span className="text-xs font-mono text-white tabular-nums">{fmtTimeDisplay(value)}</span>
      </div>
      <input
        type="range"
        min={0}
        max={95}
        step={1}
        value={hhmmToSlider(value)}
        onChange={(e) => onChange(sliderToHHMM(Number(e.target.value)))}
        className="w-full h-1.5 rounded-full appearance-none cursor-pointer accent-violet-500
                   bg-gray-700 [&::-webkit-slider-thumb]:w-3.5 [&::-webkit-slider-thumb]:h-3.5
                   [&::-webkit-slider-thumb]:rounded-full [&::-webkit-slider-thumb]:appearance-none
                   [&::-webkit-slider-thumb]:bg-violet-500 [&::-webkit-slider-thumb]:cursor-pointer"
      />
    </div>
  )
}
