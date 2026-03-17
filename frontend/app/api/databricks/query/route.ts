import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase/server'
import { queryDatabricks } from '@/lib/databricks'

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
    const result = await queryDatabricks(sql, params ?? {})
    return NextResponse.json(result)
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Unknown error'
    return NextResponse.json({ error: message }, { status: 500 })
  }
}
