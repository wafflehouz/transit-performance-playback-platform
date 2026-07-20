'use client'

import { useEffect, useState } from 'react'
import { AlertTriangle, X } from 'lucide-react'

// Manually maintained — not derivable from NEXT_PUBLIC_DATA_END_DATE, which only
// marks the last date with data, not when polling actually stopped.
const PAUSED_SINCE = 'July 3, 2026'

const SESSION_KEY = 'pipelineNoticeSeen_v1'

function formatAnchor(anchor: string): string {
  return new Date(anchor + 'T12:00:00').toLocaleDateString('en-US', {
    month: 'long',
    day: 'numeric',
    year: 'numeric',
  })
}

/**
 * Surfaces the Databricks pipeline pause. Presence of NEXT_PUBLIC_DATA_END_DATE is
 * the single source of truth: when the pipeline resumes, unset that env var and
 * both this notice and the anchored date-picker defaults (see lib/utils.ts) revert
 * automatically — nothing else to remember to turn off.
 */
export default function PipelineNotice() {
  const anchor = process.env.NEXT_PUBLIC_DATA_END_DATE
  const [showModal, setShowModal] = useState(false)

  useEffect(() => {
    if (!anchor) return
    if (sessionStorage.getItem(SESSION_KEY)) return
    setShowModal(true)
  }, [anchor])

  if (!anchor) return null

  const anchorLabel = formatAnchor(anchor)

  function dismissModal() {
    sessionStorage.setItem(SESSION_KEY, '1')
    setShowModal(false)
  }

  return (
    <>
      <div className="shrink-0 flex items-center gap-2 px-4 py-1.5 bg-amber-900/40 border-b border-amber-700/50 text-amber-200 text-xs">
        <AlertTriangle className="w-3.5 h-3.5 shrink-0" />
        <span>
          <strong className="font-semibold">Data pipeline paused</strong> since {PAUSED_SINCE} (compute cost).
          Historical dashboards reflect data through <strong className="font-semibold">{anchorLabel}</strong>.
          Live map is unaffected.
        </span>
      </div>

      {showModal && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/60 px-4">
          <div className="w-full max-w-md bg-gray-900 border border-gray-800 rounded-xl shadow-2xl p-6">
            <div className="flex items-start gap-3 mb-3">
              <div className="w-9 h-9 rounded-lg bg-amber-900/40 flex items-center justify-center shrink-0">
                <AlertTriangle className="w-5 h-5 text-amber-400" />
              </div>
              <div>
                <h2 className="text-white font-semibold text-sm">Data pipeline paused</h2>
                <p className="text-gray-500 text-xs mt-0.5">Since {PAUSED_SINCE}</p>
              </div>
              <button
                onClick={dismissModal}
                title="Close"
                className="ml-auto text-gray-500 hover:text-white transition-colors"
              >
                <X className="w-4 h-4" />
              </button>
            </div>
            <p className="text-sm text-gray-300 leading-relaxed mb-3">
              The ingestion pollers that feed this platform&apos;s Bronze/Silver/Gold tables have been
              paused — the compute cost ran ~$100&ndash;200/week, more than this project&apos;s budget supports.
            </p>
            <p className="text-sm text-gray-300 leading-relaxed mb-3">
              <strong className="text-white">On-Time Performance, Dwell, Route Grid, and Trip Playback</strong>{' '}
              are frozen at the last completed run and reflect data through{' '}
              <strong className="text-white">{anchorLabel}</strong>. Date pickers default to that day.
            </p>
            <p className="text-sm text-gray-300 leading-relaxed mb-4">
              <strong className="text-white">Live Operations</strong> is unaffected — it polls vehicle
              positions independently and keeps showing real-time data.
            </p>
            <button
              onClick={dismissModal}
              className="w-full py-2 rounded-lg text-sm font-medium bg-amber-700/80 hover:bg-amber-700 text-white transition-colors"
            >
              Got it
            </button>
          </div>
        </div>
      )}
    </>
  )
}
