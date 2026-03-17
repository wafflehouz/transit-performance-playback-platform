'use client'

import Link from 'next/link'
import { usePathname, useRouter } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'
import { useFilterPanel } from '@/lib/filter-panel-context'
import { cn } from '@/lib/utils'
import type { User } from '@supabase/supabase-js'

const NAV = [
  {
    href: '/grid',
    label: 'Route Grid',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5">
        <rect x="3" y="3" width="7" height="7" rx="1" />
        <rect x="14" y="3" width="7" height="7" rx="1" />
        <rect x="3" y="14" width="7" height="7" rx="1" />
        <rect x="14" y="14" width="7" height="7" rx="1" />
      </svg>
    ),
  },
  {
    href: '/playback',
    label: 'GPS Playback',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5">
        <circle cx="12" cy="12" r="9" />
        <path d="M10 8l6 4-6 4V8z" fill="currentColor" stroke="none" />
      </svg>
    ),
  },
  {
    href: '/dwell',
    label: 'Dwell Analysis',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5">
        <rect x="3" y="4" width="3" height="16" rx="1" />
        <rect x="10" y="8" width="3" height="12" rx="1" />
        <rect x="17" y="11" width="3" height="9" rx="1" />
      </svg>
    ),
  },
  {
    href: '/incidents',
    label: 'Incidents',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5">
        <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v4m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
      </svg>
    ),
  },
  {
    href: '/settings/subscriptions',
    label: 'Subscriptions',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5">
        <path strokeLinecap="round" strokeLinejoin="round" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6 6 0 10-12 0v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
      </svg>
    ),
  },
]

export default function IconNav({ user }: { user: User }) {
  const pathname = usePathname()
  const router = useRouter()
  const supabase = createClient()
  const { toggle, isOpen } = useFilterPanel()

  async function handleSignOut() {
    await supabase.auth.signOut()
    router.push('/login')
  }

  return (
    <aside className="w-14 flex flex-col items-center border-r border-gray-800 bg-gray-950 shrink-0 py-2">
      {/* Brand mark */}
      <div className="w-9 h-9 rounded-lg bg-blue-500 flex items-center justify-center mb-4 shrink-0">
        <svg viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth={2.5} className="w-5 h-5">
          <path d="M3 12h18M3 6l9-3 9 3M3 18l9 3 9-3" />
        </svg>
      </div>

      {/* Nav items */}
      <nav className="flex flex-col items-center gap-1 flex-1">
        {NAV.map((item) => {
          const active = pathname.startsWith(item.href)
          return (
            <NavIcon
              key={item.href}
              href={item.href}
              label={item.label}
              active={active}
              onClick={active ? toggle : undefined}
            >
              {item.icon}
            </NavIcon>
          )
        })}
      </nav>

      {/* Bottom: filter toggle + settings + user */}
      <div className="flex flex-col items-center gap-1 pb-1">
        {/* Filter panel toggle */}
        <button
          onClick={toggle}
          title={isOpen ? 'Hide filters' : 'Show filters'}
          className="w-10 h-10 rounded-lg flex items-center justify-center text-gray-500 hover:text-white hover:bg-gray-800 transition-colors"
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5">
            <path strokeLinecap="round" d="M3 6h18M6 12h12M9 18h6" />
          </svg>
        </button>

        {/* Sign out */}
        <button
          onClick={handleSignOut}
          title="Sign out"
          className="w-10 h-10 rounded-lg flex items-center justify-center text-gray-500 hover:text-white hover:bg-gray-800 transition-colors"
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5">
            <path strokeLinecap="round" d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4M16 17l5-5-5-5M21 12H9" />
          </svg>
        </button>

        {/* User avatar */}
        <div
          title={user.email}
          className="w-8 h-8 rounded-full bg-gray-700 flex items-center justify-center text-xs text-gray-300 mt-1"
        >
          {user.email?.[0]?.toUpperCase() ?? '?'}
        </div>
      </div>
    </aside>
  )
}

function NavIcon({
  href,
  label,
  active,
  children,
  onClick,
}: {
  href: string
  label: string
  active: boolean
  children: React.ReactNode
  onClick?: () => void
}) {
  const pathname = usePathname()

  if (active && onClick) {
    return (
      <button
        onClick={onClick}
        title={label}
        className={cn(
          'w-10 h-10 rounded-lg flex items-center justify-center transition-colors',
          'bg-blue-600/20 text-blue-400'
        )}
      >
        {children}
      </button>
    )
  }

  return (
    <Link
      href={href}
      title={label}
      className={cn(
        'w-10 h-10 rounded-lg flex items-center justify-center transition-colors',
        active ? 'bg-blue-600/20 text-blue-400' : 'text-gray-500 hover:text-white hover:bg-gray-800'
      )}
    >
      {children}
    </Link>
  )
}
