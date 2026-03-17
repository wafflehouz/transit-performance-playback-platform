'use client'

import Link from 'next/link'
import { usePathname, useRouter } from 'next/navigation'
import { createClient } from '@/lib/supabase/client'
import { useFilterPanel } from '@/lib/filter-panel-context'
import { cn } from '@/lib/utils'
import type { User } from '@supabase/supabase-js'

const NAV = [
  {
    href: '/live',
    label: 'Live Operations',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5">
        <circle cx="12" cy="12" r="3" fill="currentColor" stroke="none" />
        <path strokeLinecap="round" d="M6.3 6.3a8 8 0 000 11.4M17.7 6.3a8 8 0 010 11.4" />
        <path strokeLinecap="round" d="M9.2 9.2a4 4 0 000 5.6M14.8 9.2a4 4 0 010 5.6" />
      </svg>
    ),
  },
  {
    href: '/otp',
    label: 'On-Time Performance',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5">
        <rect x="2" y="3" width="20" height="14" rx="2" />
        <path strokeLinecap="round" d="M8 21h8M12 17v4" />
        <path strokeLinecap="round" d="M6 8l3 3 3-3 3 3 3-3" />
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
    href: '/trip',
    label: 'Trip Playback',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5">
        <circle cx="12" cy="12" r="9" />
        <path d="M10 8l6 4-6 4V8z" fill="currentColor" stroke="none" />
      </svg>
    ),
  },
  {
    href: '/route',
    label: 'Route Performance',
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
    href: '/anomaly',
    label: 'Anomaly Monitor',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5">
        <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v4m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
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

      {/* Primary nav */}
      <nav className="flex flex-col items-center gap-1 flex-1">
        {NAV.map((item) => {
          const active = pathname.startsWith(item.href)
          return (
            <NavIcon key={item.href} href={item.href} label={item.label} active={active} onToggle={active ? toggle : undefined}>
              {item.icon}
            </NavIcon>
          )
        })}
      </nav>

      {/* Bottom: filter toggle + settings + avatar */}
      <div className="flex flex-col items-center gap-1 pb-1">
        <button
          onClick={toggle}
          title={isOpen ? 'Collapse filters' : 'Expand filters'}
          className="w-10 h-10 rounded-lg flex items-center justify-center text-gray-500 hover:text-white hover:bg-gray-800 transition-colors"
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5">
            <path strokeLinecap="round" d="M3 6h18M6 12h12M9 18h6" />
          </svg>
        </button>

        <Link
          href="/settings/subscriptions"
          title="Settings"
          className={cn(
            'w-10 h-10 rounded-lg flex items-center justify-center transition-colors',
            pathname.startsWith('/settings')
              ? 'bg-blue-600/20 text-blue-400'
              : 'text-gray-500 hover:text-white hover:bg-gray-800'
          )}
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.75} className="w-5 h-5">
            <path strokeLinecap="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
            <circle cx="12" cy="12" r="3" />
          </svg>
        </Link>

        <button
          onClick={handleSignOut}
          title={`Sign out (${user.email})`}
          className="w-8 h-8 rounded-full bg-gray-700 flex items-center justify-center text-xs text-gray-300 hover:bg-gray-600 transition-colors mt-1"
        >
          {user.email?.[0]?.toUpperCase() ?? '?'}
        </button>
      </div>
    </aside>
  )
}

function NavIcon({
  href, label, active, children, onToggle,
}: {
  href: string
  label: string
  active: boolean
  children: React.ReactNode
  onToggle?: () => void
}) {
  if (active && onToggle) {
    return (
      <button
        onClick={onToggle}
        title={label}
        className="w-10 h-10 rounded-lg flex items-center justify-center transition-colors bg-blue-600/20 text-blue-400"
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
