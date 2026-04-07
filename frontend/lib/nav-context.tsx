'use client'

import { createContext, useContext, useState, useCallback, type ReactNode } from 'react'

export interface NavFilter {
  scope: 'group' | 'single' | null
  groupName: string | null
  routeId: string | null
  timepointOnly?: boolean
  excludeTerminals?: boolean
}

interface NavContextValue {
  expanded: boolean
  toggle: () => void
  navFilter: NavFilter
  setNavFilter: (f: NavFilter) => void
}

const NavContext = createContext<NavContextValue>({
  expanded: false,
  toggle: () => {},
  navFilter: { scope: null, groupName: null, routeId: null },
  setNavFilter: () => {},
})

export function NavProvider({ children }: { children: ReactNode }) {
  const [expanded, setExpanded] = useState(false)
  const [navFilter, setNavFilter] = useState<NavFilter>({ scope: null, groupName: null, routeId: null })
  const toggle = useCallback(() => setExpanded((v) => !v), [])
  return (
    <NavContext.Provider value={{ expanded, toggle, navFilter, setNavFilter }}>
      {children}
    </NavContext.Provider>
  )
}

export function useNav() {
  return useContext(NavContext)
}
