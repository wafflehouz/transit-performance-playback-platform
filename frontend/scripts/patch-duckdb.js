// Two-part patch for duckdb's Turbopack/Vercel incompatibilities:
//
// 1. Remove the `binary` section from duckdb/package.json.
//    Turbopack reads this section to trace native binaries and crashes because
//    duckdb's node-pre-gyp config omits the required `napi_versions` field.
//    Removing it prevents the crash.
//
// 2. Replace duckdb-binding.js with a static require.
//    The original uses node_pre_gyp.find() (dynamic path) which Vercel's nft
//    file tracer can't follow, so the .node binary would be missing from the
//    deployment bundle. A static require('./binding/duckdb.node') lets nft
//    trace and include the binary, while also bypassing node-pre-gyp's
//    napi_versions validation that the patch in step 1 would otherwise break.

const fs   = require('fs')
const path = require('path')
const pkg  = path.join(__dirname, '..', 'node_modules', 'duckdb', 'package.json')
const binding = path.join(__dirname, '..', 'node_modules', 'duckdb', 'lib', 'duckdb-binding.js')

// Step 1: strip the binary section
try {
  const p = JSON.parse(fs.readFileSync(pkg, 'utf8'))
  if (p.binary) {
    delete p.binary
    fs.writeFileSync(pkg, JSON.stringify(p, null, 2))
    console.log('✓ removed duckdb/package.json binary section')
  }
} catch (e) {
  console.warn('duckdb package.json patch skipped:', e.message)
}

// Step 2: replace duckdb-binding.js with a static require
try {
  const current = fs.readFileSync(binding, 'utf8')
  if (current.includes('node-pre-gyp') || current.includes('node_pre_gyp')) {
    fs.writeFileSync(binding, [
      "'use strict';",
      "// Patched by scripts/patch-duckdb.js — static path lets Vercel nft trace binary.",
      "module.exports = require('./binding/duckdb.node');",
    ].join('\n') + '\n')
    console.log('✓ patched duckdb/lib/duckdb-binding.js (static require)')
  }
} catch (e) {
  console.warn('duckdb-binding.js patch skipped:', e.message)
}
