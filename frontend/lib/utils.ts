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
