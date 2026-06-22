import type { QueryResult } from './databricks'

const TOKEN = process.env.MOTHERDUCK_TOKEN!

// duckdb (MotherDuck's fork) statically compiles DuckDB into the .node addon —
// no libduckdb.so dependency, works in Vercel serverless. The package.json is
// patched by scripts/patch-duckdb.js (postinstall) to add napi_versions so
// Turbopack can parse the node-pre-gyp binary manifest without crashing.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const duckdb = require('duckdb') as typeof import('duckdb')

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type DuckConn = any

let _db: InstanceType<typeof duckdb.Database> | null = null
let _conn: DuckConn = null

function openConnection(): DuckConn {
  _db = new duckdb.Database(`md:transit?motherduck_token=${TOKEN}`)
  _conn = _db.connect()
  return _conn
}

function getConnection(): DuckConn {
  if (!_conn) openConnection()
  return _conn
}

function resetConnection() {
  try { _conn?.close?.() } catch { /* ignore */ }
  try { _db?.close?.() }  catch { /* ignore */ }
  _db = null
  _conn = null
}

function execAll(conn: DuckConn, sql: string): Promise<Record<string, unknown>[]> {
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

  // Wrap in to_json() so DuckDB serializes BigInt columns (COUNT, SUM) to
  // JSON strings in C++ before they hit the Node.js NAPI layer.
  const wrappedSql = `SELECT to_json(t)::VARCHAR AS _row FROM (${resolvedSql}) t`

  async function run(): Promise<Record<string, unknown>[]> {
    return execAll(getConnection(), wrappedSql)
  }

  let raw: Record<string, unknown>[]
  try {
    raw = await run()
  } catch (err) {
    // Vercel freezes containers between requests; the DuckDB connection can
    // become stale after thaw. Reset and retry once on connection errors.
    const msg = err instanceof Error ? err.message : ''
    if (msg.includes('Connection') || msg.includes('connection')) {
      resetConnection()
      raw = await run()
    } else {
      throw err
    }
  }

  const rows = raw.map((r) => JSON.parse(r._row as string) as T)
  const schema =
    rows.length > 0
      ? Object.keys(rows[0] as object).map((name) => ({ name, type_text: 'STRING' }))
      : []

  return { rows, schema }
}
