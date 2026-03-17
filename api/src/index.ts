import express from 'express'
import cors from 'cors'
import { startPoller } from './cache'
import vehiclesRouter from './routes/vehicles'

const app = express()
const PORT = parseInt(process.env.PORT ?? '3001', 10)

// ── CORS ─────────────────────────────────────────────────────────────────────
const allowedOrigins = [
  'http://localhost:3000',
  'http://localhost:3001',
  process.env.ALLOWED_ORIGIN,
].filter(Boolean) as string[]

app.use(
  cors({
    origin: (origin, callback) => {
      // Allow requests with no origin (health checks, curl, Render pings)
      if (!origin) return callback(null, true)
      if (allowedOrigins.includes(origin)) return callback(null, true)
      callback(new Error(`CORS: origin ${origin} not allowed`))
    },
    methods: ['GET'],
  })
)

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
