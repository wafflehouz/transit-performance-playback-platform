/**
 * Databricks SQL Statements API client.
 * All queries run server-side — token is never exposed to the browser.
 */

const DATABRICKS_HOST = process.env.DATABRICKS_HOST!
const DATABRICKS_TOKEN = process.env.DATABRICKS_TOKEN!
const DATABRICKS_WAREHOUSE_ID = process.env.DATABRICKS_WAREHOUSE_ID!
const CATALOG = process.env.DATABRICKS_CATALOG ?? 'tabular'
const SCHEMA = process.env.DATABRICKS_SCHEMA ?? 'dataexpert'

const BASE_URL = `https://${DATABRICKS_HOST}/api/2.0/sql/statements`

export interface QueryResult<T = Record<string, unknown>> {
  rows: T[]
  schema: { name: string; type_text: string }[]
}

/**
 * Execute a SQL statement against the Databricks SQL Warehouse.
 * Uses WAIT disposition — waits up to 30s for result inline, then polls.
 */
export async function queryDatabricks<T = Record<string, unknown>>(
  sql: string,
  params: Record<string, string | number | boolean | null> = {}
): Promise<QueryResult<T>> {
  // Simple positional param substitution (safe for server-side use)
  let resolvedSql = sql
  for (const [key, value] of Object.entries(params)) {
    const escaped =
      value === null ? 'NULL' : typeof value === 'string' ? `'${value.replace(/'/g, "''")}'` : String(value)
    resolvedSql = resolvedSql.replaceAll(`:${key}`, escaped)
  }

  const body = {
    statement: resolvedSql,
    warehouse_id: DATABRICKS_WAREHOUSE_ID,
    catalog: CATALOG,
    schema: SCHEMA,
    wait_timeout: '30s',
    disposition: 'INLINE',
    format: 'JSON_ARRAY',
  }

  const res = await fetch(BASE_URL, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${DATABRICKS_TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  })

  if (!res.ok) {
    const text = await res.text()
    throw new Error(`Databricks API error ${res.status}: ${text}`)
  }

  const data = await res.json()

  // Poll if still running
  if (data.status?.state === 'RUNNING' || data.status?.state === 'PENDING') {
    return pollStatement<T>(data.statement_id)
  }

  if (data.status?.state === 'FAILED') {
    throw new Error(`Query failed: ${data.status.error?.message ?? 'unknown error'}`)
  }

  return parseResult<T>(data)
}

async function pollStatement<T>(statementId: string): Promise<QueryResult<T>> {
  const maxAttempts = 20
  for (let i = 0; i < maxAttempts; i++) {
    await sleep(2000)
    const res = await fetch(`${BASE_URL}/${statementId}`, {
      headers: { Authorization: `Bearer ${DATABRICKS_TOKEN}` },
    })
    const data = await res.json()
    const state = data.status?.state
    if (state === 'SUCCEEDED') return parseResult<T>(data)
    if (state === 'FAILED') throw new Error(`Query failed: ${data.status.error?.message}`)
  }
  throw new Error('Query timed out after polling')
}

function parseResult<T>(data: Record<string, unknown>): QueryResult<T> {
  const manifest = data.manifest as { schema?: { columns?: { name: string; type_text: string }[] } } | undefined
  const result = data.result as { data_array?: unknown[][] } | undefined
  const columns = manifest?.schema?.columns ?? []
  const rows = (result?.data_array ?? []) as unknown[][]

  const parsed = rows.map((row) => {
    const obj: Record<string, unknown> = {}
    columns.forEach((col, i) => {
      obj[col.name] = row[i] ?? null
    })
    return obj as T
  })

  return {
    rows: parsed,
    schema: columns.map((c) => ({ name: c.name, type_text: c.type_text })),
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
