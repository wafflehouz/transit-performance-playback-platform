'use client'

import { useState, useEffect } from 'react'
import { createClient } from '@/lib/supabase/client'
import type { User } from '@supabase/supabase-js'

const MAX_SUBSCRIPTIONS = 5

interface Subscription {
  id: string
  route_id: string | null
  group_name: string | null
  frequency: string
  created_at: string
}

interface RouteOption {
  route_id: string
  route_short_name: string
  route_long_name: string
}

async function fetchRoutes(): Promise<RouteOption[]> {
  const res = await fetch('/api/databricks/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      sql: `
        SELECT DISTINCT r.route_id, r.route_short_name, r.route_long_name
        FROM silver_dim_route r
        INNER JOIN gold_stop_dwell_fact f ON r.route_id = f.route_id
        ORDER BY
          CASE WHEN r.route_short_name RLIKE '^[0-9]+$'
               THEN CAST(r.route_short_name AS INT) ELSE 9999 END,
          r.route_short_name
      `,
    }),
  })
  const data = await res.json()
  return data.rows ?? []
}

export default function SubscriptionsClient({ user }: { user: User }) {
  const supabase = createClient()

  const [subscriptions, setSubscriptions] = useState<Subscription[]>([])
  const [routes, setRoutes] = useState<RouteOption[]>([])
  const [selectedRouteId, setSelectedRouteId] = useState('')
  const [loadingSubs, setLoadingSubs] = useState(true)
  const [loadingRoutes, setLoadingRoutes] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)

  async function loadSubscriptions() {
    setLoadingSubs(true)
    const { data, error } = await supabase
      .from('route_subscriptions')
      .select('id, route_id, group_name, frequency, created_at')
      .eq('user_id', user.id)
      .order('created_at', { ascending: true })
    if (error) setError(error.message)
    else setSubscriptions(data ?? [])
    setLoadingSubs(false)
  }

  useEffect(() => {
    loadSubscriptions()
    fetchRoutes()
      .then(setRoutes)
      .catch(() => {})
      .finally(() => setLoadingRoutes(false))
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Resolve display name from loaded routes list
  function routeLabel(sub: Subscription): string {
    if (sub.group_name) return `Group: ${sub.group_name}`
    const r = routes.find((r) => r.route_id === sub.route_id)
    if (r) return `${r.route_short_name} — ${r.route_long_name}`
    return sub.route_id ?? '—'
  }

  async function handleAdd() {
    if (!selectedRouteId) return
    if (subscriptions.length >= MAX_SUBSCRIPTIONS) {
      setError(`Maximum of ${MAX_SUBSCRIPTIONS} subscriptions allowed.`)
      return
    }

    setSaving(true)
    setError(null)
    setSuccess(null)

    const { error } = await supabase.from('route_subscriptions').insert({
      user_id:   user.id,
      route_id:  selectedRouteId,
      frequency: 'weekly',
    })

    if (error) {
      setError(error.code === '23505' ? 'Already subscribed to this route.' : error.message)
    } else {
      const r = routes.find((r) => r.route_id === selectedRouteId)
      setSuccess(`Subscribed to Route ${r?.route_short_name ?? selectedRouteId}.`)
      setSelectedRouteId('')
      await loadSubscriptions()
    }
    setSaving(false)
  }

  async function handleRemove(sub: Subscription) {
    setSaving(true)
    setError(null)
    setSuccess(null)
    const { error } = await supabase
      .from('route_subscriptions')
      .delete()
      .eq('id', sub.id)
    if (error) {
      setError(error.message)
    } else {
      setSuccess(`Removed ${routeLabel(sub)}.`)
      await loadSubscriptions()
    }
    setSaving(false)
  }

  const subscribedRouteIds = new Set(subscriptions.map((s) => s.route_id).filter(Boolean))
  const availableRoutes = routes.filter((r) => !subscribedRouteIds.has(r.route_id))
  const atLimit = subscriptions.length >= MAX_SUBSCRIPTIONS

  return (
    <div className="max-w-2xl">
      <h1 className="text-xl font-semibold text-white mb-1">Weekly Report Subscriptions</h1>
      <p className="text-gray-400 text-sm mb-6">
        Receive a weekly AI-generated performance brief for up to {MAX_SUBSCRIPTIONS} routes,
        delivered every Monday morning to{' '}
        <span className="text-gray-300">{user.email}</span>.
      </p>

      {error && (
        <div className="mb-4 px-4 py-3 bg-red-900/20 border border-red-800 rounded-lg text-red-400 text-sm">
          {error}
        </div>
      )}
      {success && (
        <div className="mb-4 px-4 py-3 bg-emerald-900/20 border border-emerald-800 rounded-lg text-emerald-400 text-sm">
          {success}
        </div>
      )}

      {/* Add subscription */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-5 mb-6">
        <h2 className="text-sm font-semibold text-gray-300 mb-3">Add a route</h2>
        {atLimit ? (
          <p className="text-sm text-amber-500">
            You've reached the {MAX_SUBSCRIPTIONS}-route limit. Remove a subscription to add another.
          </p>
        ) : (
          <div className="flex gap-3">
            <select
              value={selectedRouteId}
              onChange={(e) => setSelectedRouteId(e.target.value)}
              disabled={loadingRoutes || saving}
              className="flex-1 bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:ring-1 focus:ring-violet-500 disabled:opacity-50"
            >
              <option value="">
                {loadingRoutes ? 'Loading routes…' : 'Select a route…'}
              </option>
              {availableRoutes.map((r) => (
                <option key={r.route_id} value={r.route_id}>
                  {r.route_short_name} — {r.route_long_name}
                </option>
              ))}
            </select>
            <button
              onClick={handleAdd}
              disabled={!selectedRouteId || saving}
              className="px-4 py-2 bg-violet-600 hover:bg-violet-500 disabled:bg-gray-700 disabled:cursor-not-allowed rounded-lg text-sm font-medium text-white transition-colors"
            >
              Subscribe
            </button>
          </div>
        )}
      </div>

      {/* Subscription list */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
        <div className="px-5 py-3 border-b border-gray-800 flex items-center justify-between">
          <h2 className="text-sm font-semibold text-gray-300">Active subscriptions</h2>
          <span className="text-xs text-gray-600">{subscriptions.length} / {MAX_SUBSCRIPTIONS}</span>
        </div>

        {loadingSubs ? (
          <div className="px-5 py-8 text-center text-gray-600 text-sm animate-pulse">Loading…</div>
        ) : subscriptions.length === 0 ? (
          <div className="px-5 py-8 text-center text-gray-600 text-sm">
            No subscriptions yet. Add a route above to get started.
          </div>
        ) : (
          <ul>
            {subscriptions.map((sub, i) => (
              <li
                key={sub.id}
                className={`px-5 py-4 flex items-center gap-4 ${i < subscriptions.length - 1 ? 'border-b border-gray-800' : ''}`}
              >
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-white truncate">{routeLabel(sub)}</p>
                  <p className="text-xs text-gray-600 mt-0.5">
                    Weekly · every Monday · subscribed{' '}
                    {new Date(sub.created_at).toLocaleDateString('en-US', {
                      month: 'short', day: 'numeric', year: 'numeric',
                    })}
                  </p>
                </div>
                <button
                  onClick={() => handleRemove(sub)}
                  disabled={saving}
                  title="Remove subscription"
                  className="text-gray-600 hover:text-red-400 transition-colors disabled:opacity-50 shrink-0"
                >
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-4 h-4">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>

      <p className="text-xs text-gray-600 mt-4">
        Reports cover the prior 7 days and include OTP trend, top delayed stops, dwell summary,
        and an AI-generated brief written for transit planners.
      </p>
    </div>
  )
}
