import duckdb from 'duckdb'
import type { QueryResult } from './databricks'

const TOKEN = process.env.MOTHERDUCK_TOKEN!

let _db: duckdb.Database | null = null
let _conn: duckdb.Connection | null = null

function getConnection(): duckdb.Connection {
  if (!_conn) {
    _db = new duckdb.Database(`md:transit?motherduck_token=${TOKEN}`)
    _conn = _db.connect()
  }
  return _conn
}

function execAll(conn: duckdb.Connection, sql: string): Promise<Record<string, unknown>[]> {
  return new Promise((resolve, reject) => {
    conn.all(sql, (err: Error | null, rows: Record<string, unknown>[]) => {
      if (err) reject(err)
      else resolve(rows)
    })
  })
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

  const conn = getConnection()

  // Wrap in to_json() so that DuckDB serializes BigInt columns (COUNT, SUM) to
  // JSON strings in C++ before values hit the Node.js NAPI layer.  Without this
  // the duckdb native addon crashes with "Do not know how to serialize a BigInt".
  const wrappedSql = `SELECT to_json(t)::VARCHAR AS _row FROM (${resolvedSql}) t`
  const raw = await execAll(conn, wrappedSql)

  const rows = raw.map((r) => JSON.parse(r._row as string) as T)
  const schema =
    rows.length > 0
      ? Object.keys(rows[0] as object).map((name) => ({ name, type_text: 'STRING' }))
      : []

  return { rows, schema }
}
