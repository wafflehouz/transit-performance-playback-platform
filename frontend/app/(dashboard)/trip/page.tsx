import { Suspense } from 'react'
import PlaybackPageClient from './PlaybackPageClient'

export default function PlaybackPage() {
  return (
    <Suspense>
      <PlaybackPageClient />
    </Suspense>
  )
}
