import { Router, Request, Response } from 'express'
import { getVehiclesForRoute, getCacheStatus, getActiveRouteIds } from '../cache'

const router = Router()

/**
 * GET /vehicles?route_id=40
 * Returns latest cached positions for a single route.
 */
router.get('/', (req: Request, res: Response) => {
  const routeId = req.query.route_id as string | undefined

  if (!routeId || routeId.trim() === '') {
    res.status(400).json({ error: 'route_id query param is required' })
    return
  }

  const status = getCacheStatus()

  if (status.error && status.vehicle_count === 0) {
    res.status(503).json({ error: `Feed unavailable: ${status.error}` })
    return
  }

  const vehicles = getVehiclesForRoute(routeId.trim())

  res.json({
    route_id: routeId,
    vehicle_count: vehicles.length,
    fetched_at: status.fetched_at,
    fetched_at_ms: status.fetched_at_ms,
    feed_ts: status.feed_ts,
    vehicles,
  })
})

/**
 * GET /vehicles/routes
 * Returns list of route IDs currently active in the feed.
 * Used to populate the route selector in the frontend.
 */
router.get('/routes', (_req: Request, res: Response) => {
  res.json({
    route_ids: getActiveRouteIds(),
    fetched_at: getCacheStatus().fetched_at,
  })
})

/**
 * GET /vehicles/status
 * Health + cache status — used by Render health checks.
 */
router.get('/status', (_req: Request, res: Response) => {
  const status = getCacheStatus()
  const healthy = status.vehicle_count > 0 && !status.error
  res.status(healthy ? 200 : 503).json({ ...status, healthy })
})

export default router
