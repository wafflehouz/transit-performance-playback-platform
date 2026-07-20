import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/** Convert UTC delay seconds to a severity label */
export function delaySeverity(seconds: number): 'on-time' | 'warning' | 'critical' {
  if (seconds < 180) return 'on-time'
  if (seconds < 360) return 'warning'
  return 'critical'
}

/** Severity → Tailwind bg color class */
export const SEVERITY_COLOR: Record<string, string> = {
  'on-time': 'bg-emerald-500',
  warning: 'bg-amber-400',
  critical: 'bg-red-500',
}

/** Format epoch/ISO timestamp to Phoenix local time string */
export function toPhoenixTime(ts: string | number): string {
  const date = typeof ts === 'number' ? new Date(ts * 1000) : new Date(ts)
  return date.toLocaleString('en-US', {
    timeZone: 'America/Phoenix',
    hour: '2-digit',
    minute: '2-digit',
  })
}

export function formatDelay(seconds: number): string {
  const abs = Math.abs(seconds)
  const sign = seconds < 0 ? '-' : '+'
  if (abs < 60) return `${sign}${abs}s`
  const m = Math.floor(abs / 60)
  const s = abs % 60
  return s > 0 ? `${sign}${m}m ${s}s` : `${sign}${m}m`
}

/** Local-date YMD string — avoids toISOString()'s UTC rollover at 5 PM Phoenix */
export function toYMD(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

/**
 * The "last complete service day" dashboards should default to. Normally that's
 * yesterday in local time, but NEXT_PUBLIC_DATA_END_DATE pins it to a fixed date —
 * used while the ingestion pollers are paused so every date picker/preset keeps
 * defaulting to the last real pipeline run instead of an empty "yesterday".
 */
export function getDataAnchorDate(): Date {
  const anchor = process.env.NEXT_PUBLIC_DATA_END_DATE
  const d = anchor ? new Date(anchor + 'T12:00:00') : new Date()
  if (!anchor) d.setDate(d.getDate() - 1)
  return d
}
