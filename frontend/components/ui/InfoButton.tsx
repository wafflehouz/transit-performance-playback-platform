'use client'

import { Info } from 'lucide-react'
import { useMethodologyPanel, type MethodologySection } from '@/lib/methodology-panel-context'

interface Props {
  section: MethodologySection
}

export default function InfoButton({ section }: Props) {
  const { openPanel } = useMethodologyPanel()

  return (
    <button
      onClick={() => openPanel(section)}
      title="Methodology & definitions"
      className="text-gray-600 hover:text-gray-400 transition-colors"
    >
      <Info className="w-3.5 h-3.5" />
    </button>
  )
}
