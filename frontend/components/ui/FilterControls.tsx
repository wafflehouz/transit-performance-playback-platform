'use client'

import { cn } from '@/lib/utils'

// ── Section label ──────────────────────────────────────────────────────────────
export function FilterSection({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="space-y-2">
      <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider">{label}</div>
      {children}
    </div>
  )
}

// ── Date input ─────────────────────────────────────────────────────────────────
export function DateFilter({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  return (
    <input
      type="date"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-white text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
    />
  )
}

// ── Direction toggle (matches Swiftly both/outbound/inbound) ───────────────────
export function DirectionFilter({
  value,
  onChange,
}: {
  value: 0 | 1 | 'both'
  onChange: (v: 0 | 1 | 'both') => void
}) {
  const options: { val: 0 | 1 | 'both'; label: string }[] = [
    { val: 'both', label: '⇄' },
    { val: 0, label: '→' },
    { val: 1, label: '←' },
  ]
  return (
    <div className="flex rounded-lg overflow-hidden border border-gray-700">
      {options.map((opt) => (
        <button
          key={String(opt.val)}
          onClick={() => onChange(opt.val)}
          className={cn(
            'flex-1 py-2 text-sm font-medium transition-colors',
            value === opt.val
              ? 'bg-blue-600 text-white'
              : 'bg-gray-800 text-gray-400 hover:text-white'
          )}
        >
          {opt.label}
        </button>
      ))}
    </div>
  )
}

// ── Route multi-select ─────────────────────────────────────────────────────────
import { useState, useRef, useEffect } from 'react'
import type { DimRoute } from '@/types'

export function RouteFilter({
  routes,
  selected,
  onChange,
}: {
  routes: DimRoute[]
  selected: string[]
  onChange: (ids: string[]) => void
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

  const filtered = routes.filter(
    (r) =>
      r.route_short_name.toLowerCase().includes(search.toLowerCase()) ||
      r.route_long_name?.toLowerCase().includes(search.toLowerCase())
  )

  function toggle(id: string) {
    onChange(selected.includes(id) ? selected.filter((x) => x !== id) : [...selected, id])
  }

  const label =
    selected.length === 0
      ? 'All routes'
      : selected.length === 1
      ? `Route ${routes.find((r) => r.route_id === selected[0])?.route_short_name ?? selected[0]}`
      : `${selected.length} routes selected`

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-left transition-colors hover:border-gray-600"
      >
        <span className={selected.length === 0 ? 'text-gray-500' : 'text-white'}>{label}</span>
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-4 h-4 text-gray-500 shrink-0">
          <path strokeLinecap="round" d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {open && (
        <div className="absolute top-full left-0 right-0 mt-1 bg-gray-800 border border-gray-700 rounded-lg shadow-xl z-50 max-h-64 flex flex-col">
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
            {selected.length > 0 && (
              <button
                onClick={() => onChange([])}
                className="w-full text-left px-3 py-2 text-xs text-blue-400 hover:bg-gray-700 border-b border-gray-700"
              >
                Clear selection
              </button>
            )}
            {filtered.map((r) => (
              <button
                key={r.route_id}
                onClick={() => toggle(r.route_id)}
                className={cn(
                  'w-full text-left px-3 py-2 text-sm flex items-center gap-2 hover:bg-gray-700 transition-colors',
                  selected.includes(r.route_id) ? 'text-white' : 'text-gray-300'
                )}
              >
                <div className={cn(
                  'w-4 h-4 rounded border flex items-center justify-center shrink-0',
                  selected.includes(r.route_id) ? 'bg-blue-600 border-blue-600' : 'border-gray-600'
                )}>
                  {selected.includes(r.route_id) && (
                    <svg viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth={3} className="w-3 h-3">
                      <path strokeLinecap="round" d="M5 13l4 4L19 7" />
                    </svg>
                  )}
                </div>
                <span className="font-medium">{r.route_short_name}</span>
                <span className="text-gray-500 text-xs truncate">{r.route_long_name}</span>
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
