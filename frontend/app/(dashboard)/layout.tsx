import { redirect } from 'next/navigation'
import { createClient } from '@/lib/supabase/server'
import { FilterPanelProvider } from '@/lib/filter-panel-context'
import { MethodologyPanelProvider } from '@/lib/methodology-panel-context'
import { NavProvider } from '@/lib/nav-context'
import IconNav from '@/components/ui/IconNav'
import FilterPanel from '@/components/ui/FilterPanel'
import MethodologyPanel from '@/components/ui/MethodologyPanel'
import PipelineNotice from '@/components/ui/PipelineNotice'

export default async function DashboardLayout({ children }: { children: React.ReactNode }) {
  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()

  if (!user) redirect('/login')

  return (
    <NavProvider>
      <FilterPanelProvider>
        <MethodologyPanelProvider>
          <div className="flex flex-col h-screen bg-gray-950 text-white overflow-hidden">
            <PipelineNotice />
            <div className="flex flex-1 min-h-0 overflow-hidden">
              <IconNav user={user} />
              <FilterPanel />
              <main className="flex-1 overflow-hidden min-w-0 flex flex-col">{children}</main>
              <MethodologyPanel />
            </div>
          </div>
        </MethodologyPanelProvider>
      </FilterPanelProvider>
    </NavProvider>
  )
}
