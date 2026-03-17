import { Suspense } from 'react'
import LivePageClient from './LivePageClient'

export const dynamic = 'force-dynamic'

export default function LivePage() {
  return (
    <Suspense fallback={<div className="flex-1 bg-gray-950" />}>
      <LivePageClient />
    </Suspense>
  )
}
