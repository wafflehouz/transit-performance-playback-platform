'use client'

import { SingleRouteDropdown } from '@/components/filters/RouteFilterPanel'
import { FilterSection } from '@/components/ui/FilterControls'
import { cn } from '@/lib/utils'
import type { DimRoute } from '@/types'

// Minimal trip row shape needed by the filter panel
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
  color: string
}

interface Props {
  tracks: TrackConfig[]
  routes: DimRoute[]
  routesLoading: boolean
  serviceDate: string
  onServiceDateChange: (d: string) => void
  onRouteChange: (slotIdx: number, routeId: string | null) => void
  onTripChange: (slotIdx: number, tripId: string | null) => void
  onAddTrack: () => void
  onRemoveTrack: (slotIdx: number) => void
}

// DuckDB timestamp string → epoch ms (UTC)
function dbToMs(ts: string): number {
  return new Date(ts.replace(' ', 'T') + 'Z').getTime()
}

// UTC ms → Phoenix HH:MM (UTC-7, no DST)
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

const MAX_TRACKS = 3

export default function PlaybackFilterPanel({
  tracks,
  routes,
  routesLoading,
  serviceDate,
  onServiceDateChange,
  onRouteChange,
  onTripChange,
  onAddTrack,
  onRemoveTrack,
}: Props) {
  return (
    <>
      {/* Service date */}
      <FilterSection label="Service Date">
        <input
          type="date"
          value={serviceDate}
          onChange={(e) => onServiceDateChange(e.target.value)}
          className="w-full bg-gray-800 border border-gray-700 rounded px-2 py-1 text-white text-xs focus:outline-none focus:ring-1 focus:ring-violet-500"
        />
        <p className="text-xs text-gray-600 mt-1 leading-snug">Single day — switch tabs to use date range</p>
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

              {/* Trip dropdown — shown once a route is selected */}
              {track.routeId && (
                <div className="pl-0">
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

          {/* Add route button */}
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
    </>
  )
}
