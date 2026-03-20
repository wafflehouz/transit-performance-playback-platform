import 'dotenv/config'
import express from 'express'
import cors from 'cors'
import { startPoller } from './cache'
import vehiclesRouter from './routes/vehicles'

const app = express()
const PORT = parseInt(process.env.PORT ?? '3001', 10)

// ── CORS ─────────────────────────────────────────────────────────────────────
// Public read-only feed — allow all origins so Vercel preview URLs and local
// dev all work without reconfiguring Render env vars.
app.use(cors({ methods: ['GET'] }))

app.use(express.json())

// ── Routes ────────────────────────────────────────────────────────────────────
app.use('/vehicles', vehiclesRouter)

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', ts: new Date().toISOString() })
})

// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`Transit Live API listening on port ${PORT}`)
  startPoller()
})
