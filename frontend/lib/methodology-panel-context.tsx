'use client'

import { createContext, useContext, useState, useCallback, type ReactNode } from 'react'

export type MethodologySection = 'otp' | 'dwell' | 'grid' | 'anomaly' | 'pipeline'

interface MethodologyPanelContextValue {
  isOpen: boolean
  section: MethodologySection
  openPanel: (section: MethodologySection) => void
  closePanel: () => void
}

const MethodologyPanelContext = createContext<MethodologyPanelContextValue>({
  isOpen: false,
  section: 'otp',
  openPanel: () => {},
  closePanel: () => {},
})

export function MethodologyPanelProvider({ children }: { children: ReactNode }) {
  const [isOpen, setIsOpen] = useState(false)
  const [section, setSection] = useState<MethodologySection>('otp')

  const openPanel = useCallback((s: MethodologySection) => {
    setSection(s)
    setIsOpen(true)
  }, [])

  const closePanel = useCallback(() => setIsOpen(false), [])

  return (
    <MethodologyPanelContext.Provider value={{ isOpen, section, openPanel, closePanel }}>
      {children}
    </MethodologyPanelContext.Provider>
  )
}

export function useMethodologyPanel() {
  return useContext(MethodologyPanelContext)
}
