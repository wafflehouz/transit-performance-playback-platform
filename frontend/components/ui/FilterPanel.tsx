'use client'

import { useFilterPanel } from '@/lib/filter-panel-context'

export default function FilterPanel() {
  const { content, isOpen, toggle } = useFilterPanel()

  if (!isOpen || !content) return null

  return (
    <div className="w-72 shrink-0 flex flex-col border-r border-gray-800 bg-gray-950 overflow-y-auto">
      {/* Panel header */}
      <div className="flex items-center justify-between px-4 py-3.5 border-b border-gray-800">
        <span className="text-white font-semibold text-sm">Filters</span>
        <button
          onClick={toggle}
          title="Collapse filters"
          className="text-gray-500 hover:text-white transition-colors"
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-4 h-4">
            <path strokeLinecap="round" d="M15 18l-6-6 6-6" />
          </svg>
        </button>
      </div>

      {/* Injected filter content */}
      <div className="flex-1 px-4 py-4 space-y-5">
        {content}
      </div>
    </div>
  )
}
