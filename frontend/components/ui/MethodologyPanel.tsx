'use client'

import { useEffect, useRef } from 'react'
import { X } from 'lucide-react'
import { useMethodologyPanel, type MethodologySection } from '@/lib/methodology-panel-context'

export default function MethodologyPanel() {
  const { isOpen, section, closePanel } = useMethodologyPanel()

  const sectionRefs = useRef<Partial<Record<MethodologySection, HTMLElement | null>>>({})

  useEffect(() => {
    if (!isOpen) return
    const el = sectionRefs.current[section]
    if (el) {
      // Small delay lets the panel finish rendering before scrolling
      setTimeout(() => el.scrollIntoView({ behavior: 'smooth', block: 'start' }), 80)
    }
  }, [isOpen, section])

  if (!isOpen) return null

  return (
    <div className="fixed inset-y-0 right-0 w-[22rem] bg-gray-900 border-l border-gray-800 shadow-2xl z-50 flex flex-col">
      {/* Header */}
      <div className="flex items-center justify-between px-5 py-3.5 border-b border-gray-800 shrink-0">
        <span className="text-white font-semibold text-sm">Methodology & Definitions</span>
        <button
          onClick={closePanel}
          title="Close"
          className="text-gray-500 hover:text-white transition-colors"
        >
          <X className="w-4 h-4" />
        </button>
      </div>

      {/* Scrollable content */}
      <div className="flex-1 overflow-y-auto px-5 py-4 space-y-6 text-sm">

        {/* On-Time Performance */}
        <section ref={(el) => { sectionRefs.current.otp = el }}>
          <SectionHeading>On-Time Performance</SectionHeading>
          <p className="text-gray-400 mb-3">
            A stop observation is classified using the vehicle&apos;s actual arrival vs. its scheduled arrival:
          </p>
          <div className="space-y-2 mb-3">
            <Def term="On Time" color="text-teal-400">
              Arrived between 1 minute early and 6 minutes late (−60s to +360s). Stops marked{' '}
              <Code>early_allowed = 1</Code> are also on-time if early.
            </Def>
            <Def term="Early" color="text-red-400">
              More than 1 minute early AND <Code>early_allowed = 0</Code> (vehicle is required to hold).
            </Def>
            <Def term="Late" color="text-orange-400">
              More than 6 minutes late (+360s).
            </Def>
          </div>
          <Note>
            Only stops with an observed arrival are counted — unobserved stops are excluded.
            Optional filters: <strong>Timepoint only</strong> restricts to schedule-critical stops;{' '}
            <strong>Exclude terminals</strong> removes the first and last stop of each trip.
          </Note>
          <Source>gold_stop_dwell_fact</Source>
        </section>

        <Divider />

        {/* Dwell Analysis */}
        <section ref={(el) => { sectionRefs.current.dwell = el }}>
          <SectionHeading>Dwell Analysis</SectionHeading>
          <p className="text-gray-400 mb-3">
            Dwell is <strong className="text-gray-300">VP-inferred</strong>: a vehicle is considered
            dwelling when its GPS position stays within 40 meters of a stop and its speed is ≤ 1.0 m/s.
            Vehicle positions are downsampled to 30-second buckets, so the minimum detectable dwell
            is 30 seconds — events shorter than this are absent from the data.
          </p>
          <div className="space-y-2 mb-3">
            <Def term="Normal" color="text-green-400">
              Dwell &lt; 60 seconds — typical boarding/alighting.
            </Def>
            <Def term="High Pax" color="text-orange-400">
              60–120 seconds — elevated demand or slow boarding.
            </Def>
            <Def term="Outlier" color="text-red-400">
              120+ seconds — wheelchair boarding, crowding, or an incident. Transit
              operating standards typically assume 120s for accessibility boarding.
            </Def>
          </div>
          <Note>
            <strong>Stops over 2 min</strong> counts stops averaging more than 120s across the
            selected period. Terminal stops may show high values due to scheduled layover behavior —
            use <strong>Exclude terminals</strong> to filter them.
          </Note>
          <Source>gold_stop_dwell_inferred</Source>
        </section>

        <Divider />

        {/* Route Grid */}
        <section ref={(el) => { sectionRefs.current.grid = el }}>
          <SectionHeading>Route Grid</SectionHeading>
          <p className="text-gray-400 mb-3">
            Shows each route&apos;s average trip delay aggregated into{' '}
            <strong className="text-gray-300">15-minute time buckets</strong> for a selected service date.
            Delay is measured at timepoint stops vs. scheduled arrival. The average across all trips in
            that window is color-coded:
          </p>
          <div className="space-y-2 mb-3">
            <Def term="< 1 min" color="text-emerald-400">Green — negligible deviation.</Def>
            <Def term="1–3 min" color="text-yellow-400">Yellow — moderate delay.</Def>
            <Def term="3–6 min" color="text-orange-400">Orange — significant delay.</Def>
            <Def term="> 6 min" color="text-red-400">Red — severe delay.</Def>
          </div>
          <Note>
            These bands align with standard CAD/AVL deviation tiers used in transit schedule
            revision. Clicking a cell opens the Anomaly Drawer with per-trip detail.
          </Note>
          <Source>gold_route_metrics_15min</Source>
        </section>

        <Divider />

        {/* Anomaly Detection */}
        <section ref={(el) => { sectionRefs.current.anomaly = el }}>
          <SectionHeading>Anomaly Detection</SectionHeading>
          <p className="text-gray-400 mb-3">
            Anomalies are detected by comparing each 15-minute bucket to a{' '}
            <strong className="text-gray-300">rolling 28-day baseline</strong> (grouped by route,
            direction, service type Mon–Fri / Sat / Sun, and time bucket). At least 3 sample days
            are required before any event can fire.
          </p>
          <div className="space-y-2 mb-3">
            <Def term="Warning" color="text-amber-400">
              Delay ≥ 3 min AND above baseline avg + 1.5× std. deviation, OR on-time rate
              dropped ≥ 15 percentage points below baseline.
            </Def>
            <Def term="Critical" color="text-red-400">
              Delay ≥ 6 min AND above baseline + 2σ, OR on-time rate dropped ≥ 25 percentage
              points below baseline.
            </Def>
          </div>
          <Note>
            <strong>Early in the data collection period</strong>, false positives are elevated —
            with only a few samples the standard deviation is near zero, so minor deviations can
            fire as critical. Counts stabilize after roughly 28 days. Quarterly schedule
            updates also temporarily corrupt the baseline window.
          </Note>
          <Source>gold_anomaly_events · gold_route_metrics_baseline</Source>
        </section>

        <Divider />

        {/* Data Freshness */}
        <section ref={(el) => { sectionRefs.current.pipeline = el }}>
          <SectionHeading>Data Freshness</SectionHeading>
          <div className="space-y-2 mb-3">
            <FreshnessRow label="GTFS static schedules" value="Refreshed daily · 1:00 AM" />
            <FreshnessRow label="Real-time feed (VP + TripUpdates)" value="Polled hourly" />
            <FreshnessRow label="Bronze → Silver processing" value="Nightly · 2:30 AM" />
            <FreshnessRow label="Gold tables (OTP, Dwell, Grid, Anomaly)" value="Nightly · 4:00 AM" />
          </div>
          <Note>
            All historical dashboard views (OTP, Dwell, Route Grid) reflect the previous night&apos;s
            pipeline run. The Live map shows the current state of the vehicle position feed,
            updated each hourly poll cycle.
          </Note>
        </section>

        {/* Bottom padding */}
        <div className="h-2" />
      </div>
    </div>
  )
}

// ── Sub-components ─────────────────────────────────────────────────────────────

function SectionHeading({ children }: { children: React.ReactNode }) {
  return (
    <h2 className="text-white font-semibold text-sm mb-3 flex items-center gap-2">
      {children}
    </h2>
  )
}

function Def({ term, color, children }: { term: string; color: string; children: React.ReactNode }) {
  return (
    <div className="bg-gray-800/60 rounded-lg px-3 py-2">
      <span className={`font-semibold ${color}`}>{term}</span>
      <span className="text-gray-400"> — {children}</span>
    </div>
  )
}

function Note({ children }: { children: React.ReactNode }) {
  return (
    <p className="text-xs text-gray-500 leading-relaxed mt-2">{children}</p>
  )
}

function Code({ children }: { children: React.ReactNode }) {
  return (
    <code className="text-xs bg-gray-800 text-gray-300 px-1 py-0.5 rounded font-mono">{children}</code>
  )
}

function Source({ children }: { children: React.ReactNode }) {
  return (
    <p className="text-xs text-gray-600 mt-2 font-mono">Source: {children}</p>
  )
}

function Divider() {
  return <div className="border-t border-gray-800" />
}

function FreshnessRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-start justify-between gap-3 bg-gray-800/60 rounded-lg px-3 py-2">
      <span className="text-gray-400 text-xs">{label}</span>
      <span className="text-gray-300 text-xs font-medium shrink-0">{value}</span>
    </div>
  )
}
