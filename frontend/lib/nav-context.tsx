'use client'

import { createContext, useContext, useState, useCallback, type ReactNode } from 'react'

interface NavContextValue {
  expanded: boolean
  toggle: () => void
}

const NavContext = createContext<NavContextValue>({ expanded: false, toggle: () => {} })

export function NavProvider({ children }: { children: ReactNode }) {
  const [expanded, setExpanded] = useState(false)
  const toggle = useCallback(() => setExpanded((v) => !v), [])
  return <NavContext.Provider value={{ expanded, toggle }}>{children}</NavContext.Provider>
}

export function useNav() {
  return useContext(NavContext)
}
