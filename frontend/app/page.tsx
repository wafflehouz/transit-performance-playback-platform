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

    // Only forward recovery-type tokens to reset-password
    const params = new URLSearchParams(search)
    if (params.get('type') === 'recovery') {
      router.replace('/reset-password' + search)
      return
    }

    router.replace('/otp')
  }, [router])

  return null
}
