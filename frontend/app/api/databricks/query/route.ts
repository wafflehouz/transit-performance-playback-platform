import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase/server'

export async function POST(request: NextRequest) {
  // Require authenticated session
  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()
  if (!user) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  const body = await request.json()
  const { sql, params } = body as { sql: string; params?: Record<string, string | number | boolean | null> }

  if (!sql || typeof sql !== 'string') {
    return NextResponse.json({ error: 'Missing sql' }, { status: 400 })
  }

  try {
    // Dynamic import keeps any native-addon load failure inside the try-catch
    // so errors are returned as JSON rather than an HTML 500 from Next.js.
    const { queryMotherDuck } = await import('@/lib/motherduck')
    const result = await queryMotherDuck(sql, params ?? {})
    return NextResponse.json(result)
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Unknown error'
    console.error('[/api/databricks/query]', message)
    return NextResponse.json({ error: message }, { status: 500 })
  }
}
