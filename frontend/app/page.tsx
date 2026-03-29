'use client'

import { useEffect } from 'react'
import { useRouter } from 'next/navigation'

export default function RootPage() {
  const router = useRouter()

  useEffect(() => {
    const hash = window.location.hash
    const search = window.location.search

    // Implicit flow: tokens in hash fragment
    if (hash.includes('type=recovery')) {
      router.replace('/reset-password' + hash)
      return
    }

    // PKCE flow: code or token_hash in query params
    const params = new URLSearchParams(search)
    if (
      params.get('type') === 'recovery' ||
      params.has('code') ||
      params.has('token_hash')
    ) {
      router.replace('/reset-password' + search)
      return
    }

    router.replace('/dashboard')
  }, [router])

  return null
}
