'use client'

import { useState, useEffect } from 'react'
import { createClient } from '@/lib/supabase/client'

function ResetPasswordForm() {
  const [password, setPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [done, setDone] = useState(false)
  const [ready, setReady] = useState(false)
  const [expired, setExpired] = useState(false)

  const supabase = createClient()

  useEffect(() => {
    // Subscribing to onAuthStateChange triggers the Supabase client to initialize.
    // In PKCE flow, it auto-detects ?code= in the URL and calls exchangeCodeForSession
    // using the code_verifier stored in cookies from when resetPasswordForEmail was called.
    // On success it fires PASSWORD_RECOVERY.
    let isReady = false

    const { data: { subscription } } = supabase.auth.onAuthStateChange((event) => {
      if (event === 'PASSWORD_RECOVERY') {
        isReady = true
        setReady(true)
      }
    })

    // Handle page refresh where session already exists
    supabase.auth.getSession().then(({ data: { session } }) => {
      if (session) { isReady = true; setReady(true) }
    })

    const timer = setTimeout(() => {
      if (!isReady) setExpired(true)
    }, 10000)

    return () => { subscription.unsubscribe(); clearTimeout(timer) }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  async function handleReset(e: React.FormEvent) {
    e.preventDefault()
    if (password !== confirmPassword) {
      setError('Passwords do not match.')
      return
    }
    if (password.length < 8) {
      setError('Password must be at least 8 characters.')
      return
    }
    setLoading(true)
    setError(null)
    const { error } = await supabase.auth.updateUser({ password })
    if (error) {
      setError(error.message)
      setLoading(false)
      return
    }
    // Sign out the recovery session so the Supabase client doesn't fire
    // a post-auth redirect to /api/auth/callback after password update.
    await supabase.auth.signOut()
    setDone(true)
    setLoading(false)
  }

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
      {done ? (
        <div className="text-center py-4">
          <div className="w-12 h-12 rounded-full bg-emerald-500/10 flex items-center justify-center mx-auto mb-3">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-6 h-6 text-emerald-400">
              <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
            </svg>
          </div>
          <p className="text-white font-semibold mb-1">Password updated</p>
          <p className="text-gray-400 text-sm mb-4">You can now sign in with your new password.</p>
          <a href="/login" className="text-sm text-blue-400 hover:text-blue-300 transition-colors">
            Go to sign in
          </a>
        </div>
      ) : expired ? (
        <div className="text-center py-4">
          <p className="text-white font-semibold mb-1">Link expired or invalid</p>
          <p className="text-gray-400 text-sm mb-4">Password reset links expire after 1 hour. Please request a new one.</p>
          <a href="/login" className="text-sm text-blue-400 hover:text-blue-300 transition-colors">
            Back to sign in
          </a>
        </div>
      ) : !ready ? (
        <div className="text-center py-6">
          <p className="text-gray-400 text-sm animate-pulse">Verifying reset link…</p>
        </div>
      ) : (
        <>
          <h2 className="text-xl font-bold text-white mb-1">Set new password</h2>
          <p className="text-gray-400 text-sm mb-5">Choose a strong password for your account.</p>
          <form onSubmit={handleReset} className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1.5">New password</label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="••••••••"
                required
                className="w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-2.5 text-white placeholder-gray-500 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1.5">Confirm password</label>
              <input
                type="password"
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
                placeholder="••••••••"
                required
                className="w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-2.5 text-white placeholder-gray-500 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
            {error && (
              <div className="px-3 py-2.5 bg-red-900/20 border border-red-800 rounded-lg text-red-400 text-sm">
                {error}
              </div>
            )}
            <button
              type="submit"
              disabled={loading || !password || !confirmPassword}
              className="w-full bg-blue-600 hover:bg-blue-500 disabled:bg-blue-600/40 disabled:cursor-not-allowed text-white font-medium py-2.5 px-4 rounded-lg text-sm transition-colors"
            >
              {loading ? 'Updating…' : 'Update password'}
            </button>
          </form>
        </>
      )}
    </div>
  )
}

export default function ResetPasswordPage() {
  return (
    <div className="min-h-screen bg-gray-950 flex items-center justify-center px-4">
      <div className="w-full max-w-sm">
        <div className="flex items-center gap-2.5 mb-8 justify-center">
          <div className="w-8 h-8 rounded-lg bg-blue-500 flex items-center justify-center">
            <svg viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth={2.5} className="w-5 h-5">
              <path d="M3 12h18M3 6l9-3 9 3M3 18l9 3 9-3" />
            </svg>
          </div>
          <span className="text-white font-semibold text-lg tracking-tight">Phoenix Transit Analytics</span>
        </div>
        <ResetPasswordForm />
      </div>
    </div>
  )
}
