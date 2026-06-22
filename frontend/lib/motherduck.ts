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
// Promise that resolves when the MotherDuck async handshake completes.
// Using a promise prevents concurrent requests from each opening a connection.
let _ready: Promise<DuckConn> | null = null

function openConnection(): Promise<DuckConn> {
  _ready = new Promise<DuckConn>((resolve, reject) => {
    // The callback form of Database() fires only after MotherDuck's network
    // handshake succeeds. Without it, connect()/all() run before the session
    // is live and produce "Connection was never established."
    // Vercel Lambda has no HOME directory; DuckDB needs one to store the
    // MotherDuck extension. /tmp is the only writable path in Lambda.
    if (!process.env.HOME) process.env.HOME = '/tmp'
    _db = new duckdb.Database(
      `md:transit?motherduck_token=${TOKEN}`,
      (err: Error | null) => {
        if (err) {
          _db = null
          _ready = null
          reject(err)
          return
        }
        _conn = _db!.connect()
        resolve(_conn)
      }
    )
  })
  return _ready
}

function getConnection(): Promise<DuckConn> {
  if (!_ready) openConnection()
  return _ready!
}

function resetConnection() {
  try { _conn?.close?.() } catch { /* ignore */ }
  try { (_db as InstanceType<typeof duckdb.Database> | null)?.close?.() } catch { /* ignore */ }
  _db = null
  _conn = null
  _ready = null
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
    const conn = await getConnection()
    return execAll(conn, wrappedSql)
  }

  let raw: Record<string, unknown>[]
  try {
    raw = await run()
  } catch (err) {
    // After Vercel container thaw, the established connection may go stale.
    // Reset and retry once on any connection-related error.
    const msg = err instanceof Error ? err.message : ''
    if (msg.toLowerCase().includes('connection')) {
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
