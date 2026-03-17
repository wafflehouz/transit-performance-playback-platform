import { Suspense } from 'react'
import GridPageClient from './GridPageClient'

export const dynamic = 'force-dynamic'

export default function GridPage() {
  return (
    <Suspense fallback={<GridSkeleton />}>
      <GridPageClient />
    </Suspense>
  )
}

function GridSkeleton() {
  return (
    <div className="p-6 animate-pulse space-y-3">
      <div className="h-8 w-64 bg-gray-800 rounded" />
      <div className="h-6 w-48 bg-gray-800 rounded" />
      {Array.from({ length: 8 }).map((_, i) => (
        <div key={i} className="h-7 bg-gray-800 rounded" />
      ))}
    </div>
  )
}
