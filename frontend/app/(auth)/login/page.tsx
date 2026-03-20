'use client'

import { useState } from 'react'
import { createClient } from '@/lib/supabase/client'

type Mode = 'password' | 'magic'

export default function LoginPage() {
  const [mode, setMode] = useState<Mode>('password')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [sent, setSent] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  const supabase = createClient()

  async function handlePasswordLogin(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setError(null)

    const { error } = await supabase.auth.signInWithPassword({ email, password })

    if (error) {
      setError(error.message)
    } else {
      window.location.href = '/'
    }
    setLoading(false)
  }

  async function handleMagicLink(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setError(null)

    const { error } = await supabase.auth.signInWithOtp({
      email,
      options: { emailRedirectTo: `${window.location.origin}/api/auth/callback` },
    })

    if (error) {
      setError(error.message)
    } else {
      setSent(true)
    }
    setLoading(false)
  }

  return (
    <div className="min-h-screen bg-gray-950 flex items-center justify-center px-4">
      <div className="w-full max-w-sm">
        {/* Logo / title */}
        <div className="mb-8 text-center">
          <div className="inline-flex items-center gap-2 mb-3">
            <div className="w-8 h-8 rounded-lg bg-blue-500 flex items-center justify-center">
              <svg viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth={2.5} className="w-5 h-5">
                <path d="M3 12h18M3 6l9-3 9 3M3 18l9 3 9-3" />
              </svg>
            </div>
            <span className="text-white font-semibold text-lg tracking-tight">Transit Platform</span>
          </div>
          <p className="text-gray-400 text-sm">Planner performance intelligence</p>
        </div>

        <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
          {sent ? (
            <div className="text-center py-4">
              <div className="w-12 h-12 rounded-full bg-emerald-500/10 flex items-center justify-center mx-auto mb-3">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-6 h-6 text-emerald-400">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                </svg>
              </div>
              <p className="text-white font-medium mb-1">Check your email</p>
              <p className="text-gray-400 text-sm">
                We sent a magic link to <span className="text-gray-200">{email}</span>
              </p>
              <button
                onClick={() => { setSent(false); setMode('password') }}
                className="mt-4 text-xs text-gray-500 hover:text-gray-300 transition-colors"
              >
                Back to sign in
              </button>
            </div>
          ) : (
            <>
              {/* Mode toggle */}
              <div className="flex rounded-lg bg-gray-800 p-0.5 mb-5">
                <button
                  onClick={() => { setMode('password'); setError(null) }}
                  className={`flex-1 text-sm py-1.5 rounded-md font-medium transition-colors ${
                    mode === 'password' ? 'bg-gray-700 text-white' : 'text-gray-400 hover:text-gray-200'
                  }`}
                >
                  Password
                </button>
                <button
                  onClick={() => { setMode('magic'); setError(null) }}
                  className={`flex-1 text-sm py-1.5 rounded-md font-medium transition-colors ${
                    mode === 'magic' ? 'bg-gray-700 text-white' : 'text-gray-400 hover:text-gray-200'
                  }`}
                >
                  Magic link
                </button>
              </div>

              <form onSubmit={mode === 'password' ? handlePasswordLogin : handleMagicLink} className="space-y-4">
                <div>
                  <label htmlFor="email" className="block text-sm font-medium text-gray-300 mb-1.5">
                    Email
                  </label>
                  <input
                    id="email"
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    placeholder="you@valleymetro.org"
                    required
                    className="w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-2.5 text-white placeholder-gray-500 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  />
                </div>

                {mode === 'password' && (
                  <div>
                    <label htmlFor="password" className="block text-sm font-medium text-gray-300 mb-1.5">
                      Password
                    </label>
                    <input
                      id="password"
                      type="password"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      placeholder="••••••••"
                      required
                      className="w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-2.5 text-white placeholder-gray-500 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    />
                  </div>
                )}

                {error && <p className="text-red-400 text-sm">{error}</p>}

                <button
                  type="submit"
                  disabled={loading || !email || (mode === 'password' && !password)}
                  className="w-full bg-blue-600 hover:bg-blue-500 disabled:bg-blue-600/40 disabled:cursor-not-allowed text-white font-medium py-2.5 px-4 rounded-lg text-sm transition-colors"
                >
                  {loading ? 'Signing in…' : mode === 'password' ? 'Sign in' : 'Send magic link'}
                </button>
              </form>
            </>
          )}
        </div>

        <p className="text-center text-gray-600 text-xs mt-6">
          Valley Metro Transit Performance Platform
        </p>
      </div>
    </div>
  )
}
