'use client'

import { createContext, useContext, useState, useCallback, type ReactNode } from 'react'

interface FilterPanelContextValue {
  content: ReactNode
  setContent: (node: ReactNode) => void
  isOpen: boolean
  toggle: () => void
  open: () => void
}

const FilterPanelContext = createContext<FilterPanelContextValue>({
  content: null,
  setContent: () => {},
  isOpen: true,
  toggle: () => {},
  open: () => {},
})

export function FilterPanelProvider({ children }: { children: ReactNode }) {
  const [content, setContentState] = useState<ReactNode>(null)
  const [isOpen, setIsOpen] = useState(true)

  const setContent = useCallback((node: ReactNode) => {
    setContentState(node)
    setIsOpen(true)
  }, [])

  const toggle = useCallback(() => setIsOpen((v) => !v), [])
  const open = useCallback(() => setIsOpen(true), [])

  return (
    <FilterPanelContext.Provider value={{ content, setContent, isOpen, toggle, open }}>
      {children}
    </FilterPanelContext.Provider>
  )
}

export function useFilterPanel() {
  return useContext(FilterPanelContext)
}
