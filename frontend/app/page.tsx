'use client'

import { useEffect } from 'react'
import { useRouter } from 'next/navigation'

export default function RootPage() {
  const router = useRouter()

  useEffect(() => {
    // Supabase implicit flow puts recovery tokens in the URL hash
    if (window.location.hash.includes('type=recovery')) {
      router.replace('/reset-password' + window.location.hash)
    } else {
      router.replace('/dashboard')
    }
  }, [router])

  return null
}
