'use client'

import { useState, useRef, useEffect } from 'react'
import { cn } from '@/lib/utils'
import { FilterSection, DirectionFilter } from '@/components/ui/FilterControls'
import type { DimRoute } from '@/types'

export type FilterMode = 'all' | 'group' | 'single'

export interface OtpFilterState {
  mode: FilterMode
  groupName: string | null
  routeId: string | null
  startDate: string
  endDate: string
  direction: 0 | 1 | 'both'
  timepointOnly: boolean
}

// Preset date ranges (relative to yesterday, since gold runs nightly)
export type DatePreset = '1d' | '7d' | '14d' | '28d'

function toYMD(d: Date): string {
  // Use local date components — toISOString() is UTC and rolls over at 5 PM Phoenix
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

export function resolveDates(preset: DatePreset): { startDate: string; endDate: string } {
  const end = new Date()
  end.setDate(end.getDate() - 1) // yesterday in local time
  const start = new Date(end)
  const days = preset === '1d' ? 0 : preset === '7d' ? 6 : preset === '14d' ? 13 : 27
  start.setDate(start.getDate() - days)
  return {
    endDate:   toYMD(end),
    startDate: toYMD(start),
  }
}

interface Props {
  filters: OtpFilterState
  onChange: (f: OtpFilterState) => void
  routes: DimRoute[]
  groups: string[]
  showDirection?: boolean
  activePreset: DatePreset
  onPresetChange: (p: DatePreset) => void
  // When true, shows a loading spinner in the route dropdown
  routesLoading?: boolean
  // Optional terminal-stop exclusion toggle (shown when both props provided)
  excludeTerminals?: boolean
  onExcludeTerminalsChange?: (v: boolean) => void
  // When provided, replaces the date range section with a single service-date picker
  scheduleDate?: string
  onScheduleDateChange?: (d: string) => void
}

export default function RouteFilterPanel({
  filters,
  onChange,
  routes,
  groups,
  showDirection = true,
  activePreset,
  onPresetChange,
  routesLoading = false,
  excludeTerminals,
  onExcludeTerminalsChange,
  scheduleDate,
  onScheduleDateChange,
}: Props) {
  const { mode, groupName, routeId, startDate, endDate, direction, timepointOnly } = filters

  function update(partial: Partial<OtpFilterState>) {
    onChange({ ...filters, ...partial })
  }

  const modeLabels: Record<FilterMode, string> = {
    all: 'All Routes',
    group: 'Route Group',
    single: 'Single Route',
  }

  return (
    <>
      {/* Scope mode selector */}
      <FilterSection label="Scope">
        <div className="flex flex-col gap-0.5">
          {(['all', 'group', 'single'] as FilterMode[]).map((m) => (
            <button
              key={m}
              onClick={() => update({ mode: m, groupName: null, routeId: null })}
              className={cn(
                'w-full text-left px-3 py-2 text-sm rounded-lg transition-colors font-medium',
                mode === m
                  ? 'bg-violet-600 text-white'
                  : 'text-gray-400 hover:text-white hover:bg-gray-800'
              )}
            >
              {modeLabels[m]}
            </button>
          ))}
        </div>
      </FilterSection>

      {/* Group selector */}
      {mode === 'group' && (
        <FilterSection label="Group">
          <GroupDropdown groups={groups} selected={groupName} onSelect={(g) => update({ groupName: g })} />
        </FilterSection>
      )}

      {/* Single route selector */}
      {mode === 'single' && (
        <FilterSection label="Route">
          <SingleRouteDropdown routes={routes} selected={routeId} onSelect={(id) => update({ routeId: id })} loading={routesLoading} />
        </FilterSection>
      )}

      {/* Date range — hidden when schedule mode provides its own single date */}
      {scheduleDate !== undefined ? (
        <FilterSection label="Service Date">
          <input
            type="date"
            value={scheduleDate}
            onChange={(e) => onScheduleDateChange?.(e.target.value)}
            className="w-full bg-gray-800 border border-gray-700 rounded px-2 py-1 text-white text-xs focus:outline-none focus:ring-1 focus:ring-violet-500"
          />
          <p className="text-xs text-gray-600 mt-1 leading-snug">Single day — switch tabs to use date range</p>
        </FilterSection>
      ) : (
        <FilterSection label="Date Range">
          <div className="flex rounded-lg overflow-hidden border border-gray-700 text-xs">
            {(['1d', '7d', '14d', '28d'] as DatePreset[]).map((p) => (
              <button
                key={p}
                onClick={() => onPresetChange(p)}
                className={cn(
                  'flex-1 py-2 font-medium transition-colors',
                  activePreset === p
                    ? 'bg-violet-600 text-white'
                    : 'bg-gray-800 text-gray-400 hover:text-white'
                )}
              >
                {p === '1d' ? 'Yesterday' : p}
              </button>
            ))}
          </div>
          <div className="flex items-center gap-1 mt-1.5">
            <input
              type="date"
              value={startDate}
              onChange={(e) => update({ startDate: e.target.value })}
              className="flex-1 bg-gray-800 border border-gray-700 rounded px-2 py-1 text-white text-xs focus:outline-none focus:ring-1 focus:ring-violet-500"
            />
            <span className="text-gray-600 text-xs shrink-0">→</span>
            <input
              type="date"
              value={endDate}
              onChange={(e) => update({ endDate: e.target.value })}
              className="flex-1 bg-gray-800 border border-gray-700 rounded px-2 py-1 text-white text-xs focus:outline-none focus:ring-1 focus:ring-violet-500"
            />
          </div>
        </FilterSection>
      )}

      {/* Direction (all modes when showDirection=true) */}
      {showDirection && (
        <FilterSection label="Direction">
          <DirectionFilter value={direction} onChange={(d) => update({ direction: d })} />
        </FilterSection>
      )}

      {/* Stop Type */}
      <FilterSection label="Stop Type">
        <div className="flex rounded-lg overflow-hidden border border-gray-700 text-xs">
          <button
            onClick={() => update({ timepointOnly: false })}
            className={cn(
              'flex-1 py-2 font-medium transition-colors',
              !timepointOnly ? 'bg-violet-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'
            )}
          >
            All Stops
          </button>
          <button
            onClick={() => update({ timepointOnly: true })}
            className={cn(
              'flex-1 py-2 font-medium transition-colors',
              timepointOnly ? 'bg-violet-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'
            )}
          >
            Timepoints
          </button>
        </div>
      </FilterSection>

      {/* Terminal stop exclusion (opt-in — only shown when parent passes the prop) */}
      {excludeTerminals !== undefined && onExcludeTerminalsChange && (
        <FilterSection label="Terminal Stops">
          <div className="flex rounded-lg overflow-hidden border border-gray-700 text-xs">
            <button
              onClick={() => onExcludeTerminalsChange(false)}
              className={cn(
                'flex-1 py-2 font-medium transition-colors',
                !excludeTerminals ? 'bg-violet-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'
              )}
            >
              Include
            </button>
            <button
              onClick={() => onExcludeTerminalsChange(true)}
              className={cn(
                'flex-1 py-2 font-medium transition-colors',
                excludeTerminals ? 'bg-violet-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'
              )}
            >
              Exclude
            </button>
          </div>
          {excludeTerminals && (
            <p className="text-xs text-gray-600 mt-1 leading-snug">
              First + last stops per trip removed — suppresses layover dwell
            </p>
          )}
        </FilterSection>
      )}
    </>
  )
}

// ── Group dropdown ─────────────────────────────────────────────────────────────

function GroupDropdown({
  groups,
  selected,
  onSelect,
}: {
  groups: string[]
  selected: string | null
  onSelect: (g: string | null) => void
}) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    function handler(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-left hover:border-gray-600 transition-colors"
      >
        <span className={selected ? 'text-white' : 'text-gray-500'}>
          {selected ?? 'Select group…'}
        </span>
        <ChevronDown />
      </button>

      {open && (
        <div className="absolute top-full left-0 right-0 mt-1 bg-gray-800 border border-gray-700 rounded-lg shadow-xl z-50 max-h-56 overflow-y-auto">
          {selected && (
            <button
              onClick={() => { onSelect(null); setOpen(false) }}
              className="w-full text-left px-3 py-2 text-xs text-violet-400 hover:bg-gray-700 border-b border-gray-700"
            >
              Clear
            </button>
          )}
          {groups.map((g) => (
            <button
              key={g}
              onClick={() => { onSelect(g); setOpen(false) }}
              className={cn(
                'w-full text-left px-3 py-2 text-sm hover:bg-gray-700 transition-colors',
                selected === g ? 'text-white font-medium' : 'text-gray-300'
              )}
            >
              {g}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Single route dropdown with search ──────────────────────────────────────────

function SingleRouteDropdown({
  routes,
  selected,
  onSelect,
  loading = false,
}: {
  routes: DimRoute[]
  selected: string | null
  onSelect: (id: string | null) => void
  loading?: boolean
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

  const selectedRoute = routes.find((r) => r.route_id === selected)

  const filtered = routes.filter(
    (r) =>
      r.route_short_name.toLowerCase().includes(search.toLowerCase()) ||
      r.route_long_name?.toLowerCase().includes(search.toLowerCase())
  )

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => { setOpen((v) => !v); setSearch('') }}
        className="w-full flex items-center justify-between bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-left hover:border-gray-600 transition-colors"
      >
        <span className={selected ? 'text-white' : 'text-gray-500'}>
          {selectedRoute
            ? `${selectedRoute.route_short_name} — ${selectedRoute.route_long_name}`
            : loading ? 'Loading routes…' : 'Select route…'}
        </span>
        {loading ? (
          <svg className="w-4 h-4 text-gray-500 shrink-0 animate-spin" viewBox="0 0 24 24" fill="none">
            <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth={3} strokeOpacity={0.25} />
            <path d="M12 2a10 10 0 0 1 10 10" stroke="currentColor" strokeWidth={3} strokeLinecap="round" />
          </svg>
        ) : (
          <ChevronDown />
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
              placeholder="Search routes…"
              className="w-full bg-gray-900 rounded px-2 py-1.5 text-sm text-white placeholder-gray-500 focus:outline-none"
            />
          </div>
          <div className="overflow-y-auto flex-1">
            {loading ? (
              <div className="flex flex-col items-center justify-center py-6 gap-2">
                <svg className="w-5 h-5 text-violet-400 animate-spin" viewBox="0 0 24 24" fill="none">
                  <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth={3} strokeOpacity={0.25} />
                  <path d="M12 2a10 10 0 0 1 10 10" stroke="currentColor" strokeWidth={3} strokeLinecap="round" />
                </svg>
                <p className="text-gray-500 text-xs">Fetching routes…</p>
              </div>
            ) : (
              <>
                {selected && (
                  <button
                    onClick={() => { onSelect(null); setOpen(false) }}
                    className="w-full text-left px-3 py-2 text-xs text-violet-400 hover:bg-gray-700 border-b border-gray-700"
                  >
                    Clear selection
                  </button>
                )}
                {filtered.map((r) => (
                  <button
                    key={r.route_id}
                    onClick={() => { onSelect(r.route_id); setOpen(false) }}
                    className={cn(
                      'w-full text-left px-3 py-2 text-sm flex items-center gap-2 hover:bg-gray-700 transition-colors',
                      selected === r.route_id ? 'text-white bg-gray-700/50' : 'text-gray-300'
                    )}
                  >
                    <span className="font-medium shrink-0 w-10">{r.route_short_name}</span>
                    <span className="text-gray-500 text-xs truncate">{r.route_long_name}</span>
                  </button>
                ))}
              </>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

function ChevronDown() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-4 h-4 text-gray-500 shrink-0">
      <path strokeLinecap="round" d="M19 9l-7 7-7-7" />
    </svg>
  )
}
