import { DuckDBInstance } from '@duckdb/node-api'
import type { QueryResult } from './databricks'

const TOKEN = process.env.MOTHERDUCK_TOKEN!

// Cache the instance (expensive MotherDuck handshake) but open a fresh
// connection per query — DuckDB allows only one active statement per connection.
let _instancePromise: Promise<DuckDBInstance> | null = null

function getInstance(): Promise<DuckDBInstance> {
  if (!_instancePromise) {
    _instancePromise = DuckDBInstance.create('md:transit', { motherduck_token: TOKEN })
  }
  return _instancePromise
}

export async function queryMotherDuck<T = Record<string, unknown>>(
  sql: string,
  params: Record<string, string | number | boolean | null> = {}
): Promise<QueryResult<T>> {
  let resolvedSql = sql
  for (const [key, value] of Object.entries(params)) {
    const escaped =
      value === null
        ? 'NULL'
        : typeof value === 'string'
          ? `'${value.replace(/'/g, "''")}'`
          : String(value)
    resolvedSql = resolvedSql.replaceAll(`:${key}`, escaped)
  }

  const instance = await getInstance()
  const conn = await instance.connect()
  try {
    const reader = await conn.runAndReadAll(resolvedSql)
    const rows = reader.getRowObjectsJson() as T[]
    const schema =
      rows.length > 0
        ? Object.keys(rows[0] as object).map((name) => ({ name, type_text: 'STRING' }))
        : []
    return { rows, schema }
  } finally {
    conn.disconnectSync()
  }
}
