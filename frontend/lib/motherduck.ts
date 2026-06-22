import { DuckDBInstance } from '@duckdb/node-api'
import type { QueryResult } from './databricks'

const TOKEN = process.env.MOTHERDUCK_TOKEN!

// Singleton connection — reused across requests in the same serverless instance.
let _connPromise: Promise<Awaited<ReturnType<DuckDBInstance['connect']>>> | null = null

async function getConnection() {
  if (!_connPromise) {
    _connPromise = (async () => {
      const instance = await DuckDBInstance.create(`md:transit`, {
        motherduck_token: TOKEN,
      })
      return instance.connect()
    })()
  }
  return _connPromise
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

  const conn = await getConnection()
  const reader = await conn.runAndReadAll(resolvedSql)

  // getRowObjectsJson() returns JSON-serializable values — BigInt columns
  // (COUNT, SUM) are converted to strings rather than crashing the serializer.
  const rows = reader.getRowObjectsJson() as T[]
  const schema =
    rows.length > 0
      ? Object.keys(rows[0] as object).map((name) => ({ name, type_text: 'STRING' }))
      : []

  return { rows, schema }
}
