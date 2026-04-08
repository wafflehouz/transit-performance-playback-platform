'use client'

import Link from 'next/link'
import { usePathname, useRouter } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'
import { useFilterPanel } from '@/lib/filter-panel-context'
import { useNav, type NavFilter } from '@/lib/nav-context'
import { cn } from '@/lib/utils'
import type { User } from '@supabase/supabase-js'

// Build a context-aware href so navigating between dashboards carries
// the current group/route selection as URL params.
function buildNavHref(base: string, f: NavFilter): string {
  const p = new URLSearchParams()

  if (base === '/trip') {
    if (f.scope === 'single' && f.routeId) p.set('routeId', f.routeId)
    if (f.timepointOnly) p.set('timepointOnly', 'true')
    const qs = p.toString()
    return qs ? `${base}?${qs}` : base
  }

  if (base === '/otp' || base === '/dwell') {
    if (f.scope === 'group' && f.groupName)
      { p.set('scope', 'group'); p.set('group', f.groupName) }
    else if (f.scope === 'single' && f.routeId)
      { p.set('scope', 'single'); p.set('routeId', f.routeId) }
    if (f.timepointOnly)    p.set('timepointOnly', 'true')
    if (f.excludeTerminals) p.set('excludeTerminals', 'true')
    if (f.preset && f.preset !== '7d') p.set('preset', f.preset)
    const qs = p.toString()
    return qs ? `${base}?${qs}` : base
  }

  return base
}

const NAV = [
  {
    href: '/otp',
    label: 'On-Time Performance',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5 shrink-0">
        <rect x="2" y="3" width="20" height="14" rx="2" />
        <path strokeLinecap="round" d="M8 21h8M12 17v4" />
        <path strokeLinecap="round" d="M6 8l3 3 3-3 3 3 3-3" />
      </svg>
    ),
  },
  {
    href: '/live',
    label: 'Live Operations',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5 shrink-0">
        <circle cx="12" cy="12" r="3" fill="currentColor" stroke="none" />
        <path strokeLinecap="round" d="M6.3 6.3a8 8 0 000 11.4M17.7 6.3a8 8 0 010 11.4" />
        <path strokeLinecap="round" d="M9.2 9.2a4 4 0 000 5.6M14.8 9.2a4 4 0 010 5.6" />
      </svg>
    ),
  },
  {
    href: '/dwell',
    label: 'Dwell Analysis',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5 shrink-0">
        <rect x="3" y="4" width="3" height="16" rx="1" />
        <rect x="10" y="8" width="3" height="12" rx="1" />
        <rect x="17" y="11" width="3" height="9" rx="1" />
      </svg>
    ),
  },
  {
    href: '/trip',
    label: 'Trip Playback',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5 shrink-0">
        <circle cx="12" cy="12" r="9" />
        <path d="M10 8l6 4-6 4V8z" fill="currentColor" stroke="none" />
      </svg>
    ),
  },
]

export default function IconNav({ user }: { user: User }) {
  const pathname = usePathname()
  const router = useRouter()
  const supabase = createClient()
  const { toggle: toggleFilter, isOpen } = useFilterPanel()
  const { expanded, toggle: toggleNav, navFilter } = useNav()

  async function handleSignOut() {
    await supabase.auth.signOut()
    router.push('/login')
  }

  return (
    <aside className={cn(
      'flex flex-col border-r border-gray-800 bg-gray-950 shrink-0 py-2 transition-all duration-200 ease-in-out overflow-hidden',
      expanded ? 'w-52' : 'w-14'
    )}>
      {/* Hamburger / collapse toggle */}
      <button
        onClick={toggleNav}
        title={expanded ? 'Collapse menu' : 'Expand menu'}
        className="w-10 h-10 mx-auto mb-1 rounded-lg flex items-center justify-center text-gray-400 hover:text-white hover:bg-gray-800 transition-colors shrink-0"
      >
        {expanded ? (
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-5 h-5">
            <path strokeLinecap="round" d="M6 18L18 6M6 6l12 12" />
          </svg>
        ) : (
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} className="w-5 h-5">
            <path strokeLinecap="round" d="M4 6h16M4 12h16M4 18h16" />
          </svg>
        )}
      </button>

      {/* Brand */}
      <div className={cn(
        'flex items-center mb-4 shrink-0',
        expanded ? 'px-3 gap-2.5' : 'justify-center'
      )}>
        <div className="w-8 h-8 rounded-lg bg-blue-500 flex items-center justify-center shrink-0">
          <svg viewBox="0 0 24 24" fill="white" className="w-6 h-6">
            <path d="M4 16c0 .88.39 1.67 1 2.22V20c0 .55.45 1 1 1h1c.55 0 1-.45 1-1v-1h8v1c0 .55.45 1 1 1h1c.55 0 1-.45 1-1v-1.78c.61-.55 1-1.34 1-2.22V6c0-3.5-3.58-4-8-4s-8 .5-8 4v10zm3.5 1c-.83 0-1.5-.67-1.5-1.5S6.67 14 7.5 14s1.5.67 1.5 1.5-.67 1.5-1.5 1.5zm9 0c-.83 0-1.5-.67-1.5-1.5s.67-1.5 1.5-1.5 1.5.67 1.5 1.5-.67 1.5-1.5 1.5zm1.5-6H6V6h12v5z"/>
          </svg>
        </div>
        {expanded && (
          <span className="text-white text-sm font-semibold whitespace-nowrap">Transit Platform</span>
        )}
      </div>

      {/* Primary nav */}
      <nav className="flex flex-col gap-0.5 flex-1 px-2">
        {NAV.map((item) => {
          const active = pathname.startsWith(item.href)
          const href = active ? item.href : buildNavHref(item.href, navFilter)
          return (
            <NavItem
              key={item.href}
              href={href}
              label={item.label}
              active={active}
              expanded={expanded}
              onToggle={active ? toggleFilter : undefined}
            >
              {item.icon}
            </NavItem>
          )
        })}
      </nav>

      {/* Bottom controls */}
      <div className={cn('flex flex-col gap-0.5 pb-1 px-2', expanded ? '' : 'items-center')}>
        {/* Filter toggle */}
        <button
          onClick={toggleFilter}
          title={isOpen ? 'Collapse filters' : 'Expand filters'}
          className={cn(
            'h-10 rounded-lg flex items-center transition-colors text-gray-500 hover:text-white hover:bg-gray-800',
            expanded ? 'px-2 gap-3 w-full' : 'w-10 justify-center'
          )}
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5 shrink-0">
            <path strokeLinecap="round" d="M3 6h18M6 12h12M9 18h6" />
          </svg>
          {expanded && <span className="text-sm">Filters</span>}
        </button>

        {/* Account */}
        <Link
          href="/settings/account"
          title="Account"
          className={cn(
            'h-10 rounded-lg flex items-center transition-colors',
            pathname.startsWith('/settings/account')
              ? 'bg-blue-600/20 text-blue-400'
              : 'text-gray-500 hover:text-white hover:bg-gray-800',
            expanded ? 'px-2 gap-3 w-full' : 'w-10 justify-center'
          )}
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5 shrink-0">
            <circle cx="12" cy="8" r="4" />
            <path strokeLinecap="round" d="M4 20c0-4 3.6-7 8-7s8 3 8 7" />
          </svg>
          {expanded && <span className="text-sm">Account</span>}
        </Link>

        {/* Subscriptions */}
        <Link
          href="/settings/subscriptions"
          title="Subscriptions"
          className={cn(
            'h-10 rounded-lg flex items-center transition-colors',
            pathname.startsWith('/settings/subscriptions')
              ? 'bg-blue-600/20 text-blue-400'
              : 'text-gray-500 hover:text-white hover:bg-gray-800',
            expanded ? 'px-2 gap-3 w-full' : 'w-10 justify-center'
          )}
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5 shrink-0">
            <path strokeLinecap="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
            <circle cx="12" cy="12" r="3" />
          </svg>
          {expanded && <span className="text-sm">Subscriptions</span>}
        </Link>

        {/* Avatar / sign out */}
        <button
          onClick={handleSignOut}
          title={`Sign out (${user.email})`}
          className={cn(
            'h-10 rounded-lg flex items-center transition-colors text-gray-500 hover:text-white hover:bg-gray-800 mt-1',
            expanded ? 'px-2 gap-3 w-full' : 'w-10 justify-center'
          )}
        >
          <div className="w-7 h-7 rounded-full bg-gray-700 flex items-center justify-center text-xs text-gray-300 shrink-0">
            {user.email?.[0]?.toUpperCase() ?? '?'}
          </div>
          {expanded && (
            <span className="text-sm truncate">{user.email}</span>
          )}
        </button>
      </div>
    </aside>
  )
}

function NavItem({
  href, label, active, expanded, children, onToggle,
}: {
  href: string
  label: string
  active: boolean
  expanded: boolean
  children: React.ReactNode
  onToggle?: () => void
}) {
  const baseClass = cn(
    'h-10 rounded-lg flex items-center transition-colors',
    active ? 'bg-blue-600/20 text-blue-400' : 'text-gray-500 hover:text-white hover:bg-gray-800',
    expanded ? 'px-2 gap-3 w-full' : 'w-10 justify-center'
  )

  if (active && onToggle) {
    return (
      <button onClick={onToggle} title={!expanded ? label : undefined} className={baseClass}>
        {children}
        {expanded && <span className="text-sm font-medium">{label}</span>}
      </button>
    )
  }
  return (
    <Link href={href} title={!expanded ? label : undefined} className={baseClass}>
      {children}
      {expanded && <span className="text-sm font-medium">{label}</span>}
    </Link>
  )
}
