import { redirect } from 'next/navigation'
import { createClient } from '@/lib/supabase/server'
import { FilterPanelProvider } from '@/lib/filter-panel-context'
import IconNav from '@/components/ui/IconNav'
import FilterPanel from '@/components/ui/FilterPanel'

export default async function DashboardLayout({ children }: { children: React.ReactNode }) {
  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()

  if (!user) redirect('/login')

  return (
    <FilterPanelProvider>
      <div className="flex h-screen bg-gray-950 text-white overflow-hidden">
        {/* Narrow icon nav */}
        <IconNav user={user} />

        {/* Filter slideout — content injected by each page */}
        <FilterPanel />

        {/* Main content */}
        <main className="flex-1 overflow-hidden min-w-0 flex flex-col">{children}</main>
      </div>
    </FilterPanelProvider>
  )
}
