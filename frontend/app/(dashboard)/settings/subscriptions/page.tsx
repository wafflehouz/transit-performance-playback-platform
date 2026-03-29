import { redirect } from 'next/navigation'
import { createClient } from '@/lib/supabase/server'
import SubscriptionsClient from './SubscriptionsClient'

export default async function SubscriptionsPage() {
  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()
  if (!user) redirect('/login')

  return (
    <div className="p-6 overflow-auto h-full">
      <SubscriptionsClient user={user} />
    </div>
  )
}
