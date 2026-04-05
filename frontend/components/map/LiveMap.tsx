'use client'

import { useEffect, useRef, useState } from 'react'
import type { Map as MaplibreMap, Popup } from 'maplibre-gl'
import type { RouteStop } from '@/types'

export type OtpStatus = 'early' | 'on_time' | 'late' | 'very_late' | 'unknown'

export interface LiveVehicle {
  vehicle_id: string
  headsign: string | null
  trip_id: string | null
  route_id: string | null
  direction_id: number | null
  lat: number
  lon: number
  bearing: number | null
  speed_mps: number | null
  timestamp: number | null
  fetched_at: string
  delay_seconds: number | null
  otp_status: OtpStatus
}

interface Props {
  vehicles: LiveVehicle[]
  fetchedAtMs: number | null  // epoch ms when vehicles were fetched — for dead reckoning
  routeStops: Array<RouteStop & { point_type?: string }>
  routeColors: Map<string, string>  // route_id → hex; each line feature reads its own color
  selectedVehicleId?: string | null
  onVehicleSelect?: (v: LiveVehicle | null) => void
}

const MAPTILER_KEY = process.env.NEXT_PUBLIC_MAPTILER_KEY!

// OTP → color matching Swiftly's palette
const OTP_COLOR: Record<OtpStatus, string> = {
  early:     '#ec4899', // pink
  on_time:   '#22c55e', // green
  late:      '#f59e0b', // amber
  very_late: '#ef4444', // red
  unknown:   '#6b7280', // gray
}

function otpColor(status: OtpStatus): string {
  return OTP_COLOR[status]
}

function formatDelay(s: number | null): string {
  if (s === null) return 'No schedule data'
  if (s < -60) return `${Math.abs(Math.round(s / 60))}m early`
  if (s <= 60)  return 'On time'
  const m = Math.floor(s / 60), sec = s % 60
  return sec > 0 ? `+${m}m ${sec}s late` : `+${m}m late`
}

// ── Dead reckoning ─────────────────────────────────────────────────────────────
// Interpolates vehicle position forward based on speed + bearing.
// bearing: degrees clockwise from north (standard GTFS-RT bearing)
function deadReckon(
  lat: number, lon: number,
  bearing: number | null, speed_mps: number | null,
  elapsedSeconds: number
): [number, number] {
  if (!bearing || !speed_mps || speed_mps < 1.5) return [lat, lon]
  // Cap interpolation at 15s to avoid drift on curves
  const dt = Math.min(elapsedSeconds, 15)
  const bearingRad = (bearing * Math.PI) / 180
  const distM = speed_mps * dt
  const deltaLat = (distM * Math.cos(bearingRad)) / 111_111
  const deltaLon = (distM * Math.sin(bearingRad)) / (111_111 * Math.cos((lat * Math.PI) / 180))
  return [lat + deltaLat, lon + deltaLon]
}

// ── Map component ──────────────────────────────────────────────────────────────

const DEFAULT_ROUTE_COLOR = '#38bdf8'

export default function LiveMap({ vehicles, fetchedAtMs, routeStops, routeColors, selectedVehicleId, onVehicleSelect }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef = useRef<MaplibreMap | null>(null)
  const popupRef = useRef<Popup | null>(null)
  const vehiclesRef = useRef<LiveVehicle[]>(vehicles)
  const fetchedAtMsRef = useRef<number | null>(fetchedAtMs)
  const selectedVehicleIdRef = useRef<string | null>(selectedVehicleId ?? null)
  const onVehicleSelectRef = useRef(onVehicleSelect)
  const animFrameRef = useRef<number | null>(null)
  const [mapReady, setMapReady] = useState(false)

  // Keep refs current for the dead reckoning interval and event handlers
  useEffect(() => { vehiclesRef.current = vehicles }, [vehicles])
  useEffect(() => { fetchedAtMsRef.current = fetchedAtMs }, [fetchedAtMs])
  useEffect(() => { selectedVehicleIdRef.current = selectedVehicleId ?? null }, [selectedVehicleId])
  useEffect(() => { onVehicleSelectRef.current = onVehicleSelect }, [onVehicleSelect])

  // Init map once
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return
    let map: MaplibreMap

    import('maplibre-gl').then((ml) => {
      map = new ml.Map({
        container: containerRef.current!,
        style: `https://api.maptiler.com/maps/dataviz-dark/style.json?key=${MAPTILER_KEY}`,
        center: [-112.074, 33.449],
        zoom: 11,
        attributionControl: false,
      })

      map.addControl(new ml.NavigationControl({ showCompass: false }), 'bottom-right')
      map.addControl(new ml.AttributionControl({ compact: true }), 'bottom-left')

      popupRef.current = new ml.Popup({
        closeButton: false,
        closeOnClick: false,
        className: 'vp-popup',
        offset: 12,
      })

      map.on('load', () => {
        // Vehicle source
        map.addSource('vehicles', {
          type: 'geojson',
          data: { type: 'FeatureCollection', features: [] },
        })

        // Outer glow — subtle halo behind vehicle
        map.addLayer({
          id: 'veh-glow',
          type: 'circle',
          source: 'vehicles',
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 9, 14, 13, 22],
            'circle-color': ['get', 'color'],
            'circle-opacity': 0.18,
            'circle-blur': 1,
          },
        })

        // Main circle — OTP color, no border, slight transparency like Swiftly
        map.addLayer({
          id: 'veh-circle',
          type: 'circle',
          source: 'vehicles',
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 9, 8, 12, 12, 15, 16],
            'circle-color': ['get', 'color'],
            'circle-opacity': 0.88,
          },
        })

        // Directional arrow — drawn inside the circle, scaled to ~45% of circle diameter
        map.addLayer({
          id: 'veh-bearing',
          type: 'symbol',
          source: 'vehicles',
          layout: {
            'icon-image': 'bearing-arrow',
            // icon-size maps the 64px canvas to ~45% of circle diameter at each zoom
            'icon-size': ['interpolate', ['linear'], ['zoom'], 9, 0.11, 12, 0.17, 15, 0.22],
            'icon-rotate': ['coalesce', ['get', 'bearing'], 0],
            'icon-rotation-alignment': 'map',
            'icon-allow-overlap': true,
            'icon-ignore-placement': true,
          },
          paint: {
            'icon-opacity': ['case', ['==', ['get', 'bearing'], null], 0, 1],
          },
        })

        // Bearing arrow image — solid upward triangle fills ~60% of the canvas
        // so that when icon-size scales it to fit inside the circle it reads cleanly
        const size = 64
        const cx = size / 2
        const canvas = document.createElement('canvas')
        canvas.width = size; canvas.height = size
        const ctx = canvas.getContext('2d')!
        ctx.fillStyle = 'rgba(255,255,255,0.95)'
        ctx.beginPath()
        ctx.moveTo(cx,          8)           // apex — top center
        ctx.lineTo(cx + 20,     size - 12)   // bottom-right
        ctx.lineTo(cx,          size - 22)   // inner notch (arrow tail indent)
        ctx.lineTo(cx - 20,     size - 12)   // bottom-left
        ctx.closePath()
        ctx.fill()
        map.addImage('bearing-arrow', { width: size, height: size, data: new Uint8Array(ctx.getImageData(0, 0, size, size).data) })

        // Route shape + stops
        map.addSource('route-line', {
          type: 'geojson',
          data: { type: 'FeatureCollection', features: [] },
        })
        map.addSource('route-stops', {
          type: 'geojson',
          data: { type: 'FeatureCollection', features: [] },
        })

        // Route line — white casing + colored fill so it's distinct from stop dots
        map.addLayer({
          id: 'route-line-casing',
          type: 'line',
          source: 'route-line',
          paint: {
            'line-color': '#1e293b',
            'line-width': ['interpolate', ['linear'], ['zoom'], 9, 5, 13, 8],
            'line-opacity': 0.8,
          },
        }, 'veh-glow')

        map.addLayer({
          id: 'route-line-fill',
          type: 'line',
          source: 'route-line',
          paint: {
            'line-color': ['coalesce', ['get', 'color'], '#38bdf8'],
            'line-width': ['interpolate', ['linear'], ['zoom'], 9, 3, 13, 5],
            'line-opacity': 0.75,
          },
        }, 'veh-glow')

        // Stop circles — rendered above the line
        map.addLayer({
          id: 'stop-circles',
          type: 'circle',
          source: 'route-stops',
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 10, 3, 14, 6],
            'circle-color': '#0f172a',
            'circle-stroke-width': 2,
            'circle-stroke-color': '#38bdf8',
            'circle-opacity': 1,
          },
        }, 'veh-glow')

        // Stop name labels at zoom 14+
        map.addLayer({
          id: 'stop-labels',
          type: 'symbol',
          source: 'route-stops',
          minzoom: 13.5,
          layout: {
            'text-field': ['coalesce', ['get', 'stop_name'], ['get', 'stop_id']],
            'text-size': 11,
            'text-font': ['Open Sans Semibold', 'Open Sans Bold', 'Arial Unicode MS Bold'],
            'text-offset': [0, 1.4],
            'text-anchor': 'top',
            'text-allow-overlap': false,
            'text-max-width': 10,
          },
          paint: {
            'text-color': '#e2e8f0',
            'text-halo-color': '#0f172a',
            'text-halo-width': 2,
          },
        }, 'veh-glow')

        // ── Selected vehicle layers ────────────────────────────────────────────
        // Single-feature source updated in the dead reckoning loop
        map.addSource('selected-vehicle', {
          type: 'geojson',
          data: { type: 'FeatureCollection', features: [] },
        })

        // Expanding pulse ring — rendered behind the main circle, driven by rAF
        map.addLayer({
          id: 'veh-selected-pulse',
          type: 'circle',
          source: 'selected-vehicle',
          paint: {
            'circle-radius': 12,
            'circle-color': 'transparent',
            'circle-opacity': 0,
            'circle-stroke-width': 2,
            'circle-stroke-color': ['get', 'color'],
            'circle-stroke-opacity': 0,
          },
        }, 'veh-circle')  // insert below main circle so it expands outward from behind

        // Brightened circle overlay — redraws the selected vehicle at full opacity
        // on top of the base veh-circle layer (which renders at 0.88)
        map.addLayer({
          id: 'veh-selected-circle',
          type: 'circle',
          source: 'selected-vehicle',
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 9, 8, 12, 12, 15, 16],
            'circle-color': ['get', 'color'],
            'circle-opacity': 1.0,
          },
        }, 'veh-bearing')  // insert below bearing arrows

        // Inset white ring — stroke radius = circle-radius minus stroke-width
        // so the stroke falls entirely inside the vehicle circle (no gap, no overlap)
        map.addLayer({
          id: 'veh-selected-ring',
          type: 'circle',
          source: 'selected-vehicle',
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 9, 6, 12, 10, 15, 14],
            'circle-color': 'transparent',
            'circle-opacity': 0,
            'circle-stroke-width': 2,
            'circle-stroke-color': 'rgba(255,255,255,0.9)',
            'circle-stroke-opacity': 1,
          },
        }, 'veh-bearing')  // insert below bearing arrows so arrows remain on top

        // ── Pulse animation (requestAnimationFrame) ────────────────────────────
        const PULSE_DURATION = 1800  // ms per cycle
        const pulseEpoch = performance.now()

        function animatePulse() {
          const t = ((performance.now() - pulseEpoch) % PULSE_DURATION) / PULSE_DURATION
          // Base radius tracks veh-circle: 8px at zoom 9, 16px at zoom 15
          const zoom = map.getZoom()
          const baseR = Math.max(8, Math.min(16, 8 + ((zoom - 9) / 6) * 8))
          map.setPaintProperty('veh-selected-pulse', 'circle-radius', baseR + t * 18)
          // Ease-out opacity so the ring fades as it expands
          map.setPaintProperty('veh-selected-pulse', 'circle-stroke-opacity', 0.7 * (1 - t) * (1 - t))
          animFrameRef.current = requestAnimationFrame(animatePulse)
        }
        animFrameRef.current = requestAnimationFrame(animatePulse)

        // ── Click: select / deselect vehicle ──────────────────────────────────
        map.on('click', 'veh-circle', (e) => {
          e.originalEvent.stopPropagation()
          popupRef.current?.remove()
          const feat = e.features?.[0]
          if (!feat) return
          const vehicleId = feat.properties?.vehicle_id as string
          // Toggle: clicking the already-selected vehicle deselects it
          if (vehicleId === selectedVehicleIdRef.current) {
            onVehicleSelectRef.current?.(null)
          } else {
            const vehicle = vehiclesRef.current.find((v) => v.vehicle_id === vehicleId)
            onVehicleSelectRef.current?.(vehicle ?? null)
          }
        })

        // Click map background → deselect
        map.on('click', (e) => {
          const hits = map.queryRenderedFeatures(e.point, { layers: ['veh-circle'] })
          if (hits.length === 0 && selectedVehicleIdRef.current) {
            onVehicleSelectRef.current?.(null)
          }
        })

        // Hover popup
        map.on('mouseenter', 'veh-circle', (e) => {
          map.getCanvas().style.cursor = 'pointer'
          const feat = e.features?.[0]
          if (!feat || feat.geometry.type !== 'Point') return
          const p = feat.properties as Record<string, unknown>
          const headsign = (p.headsign as string | null) ?? '—'
          const speed = p.speed_mps != null ? `${((p.speed_mps as number) * 2.237).toFixed(0)} mph` : '—'

          popupRef.current
            ?.setLngLat(feat.geometry.coordinates as [number, number])
            .setHTML(`
              <div class="vp-inner">
                <div class="vp-header">
                  <span class="vp-badge" style="background:${p.color}22;color:${p.color};border-color:${p.color}44">
                    ${(p.otp_status as string).replace('_', ' ')}
                  </span>
                  <span class="vp-vid">Vehicle ${p.vehicle_id}</span>
                </div>
                <div class="vp-delay">${formatDelay(p.delay_seconds as number | null)}</div>
                <div class="vp-rows">
                  <div class="vp-row"><span>Route</span><span>${p.route_id ?? '—'}</span></div>
                  <div class="vp-row"><span>To</span><span>${headsign}</span></div>
                  <div class="vp-row"><span>Speed</span><span>${speed}</span></div>
                </div>
              </div>
            `)
            .addTo(map)
        })

        map.on('mouseleave', 'veh-circle', () => {
          map.getCanvas().style.cursor = ''
          popupRef.current?.remove()
        })

        mapRef.current = map
        setMapReady(true)
      })
    })

    return () => {
      if (animFrameRef.current) cancelAnimationFrame(animFrameRef.current)
      map?.remove()
    }
  }, [])

  // Update route line + stop markers when routeStops or routeColors changes
  useEffect(() => {
    if (!mapReady) return
    const map = mapRef.current
    if (!map) return

    const shapeRows = routeStops.filter((s) => s.point_type === 'shape')
    const stopRows  = routeStops.filter((s) => s.point_type === 'stop')
    const lineSource = shapeRows.length > 0 ? shapeRows : routeStops

    // Group by (route_id, direction_id) — supports single or multiple routes
    const byRouteDir = new Map<string, RouteStop[]>()
    for (const s of lineSource) {
      const key = `${s.route_id ?? 'default'}|${s.direction_id}`
      const arr = byRouteDir.get(key) ?? []
      arr.push(s)
      byRouteDir.set(key, arr)
    }

    // One LineString per (route, direction) — color embedded in feature properties
    const lineFeatures = Array.from(byRouteDir.entries()).map(([key, pts]) => {
      const [routeId, dirStr] = key.split('|')
      const color = routeColors.get(routeId) ?? DEFAULT_ROUTE_COLOR
      return {
        type: 'Feature' as const,
        geometry: {
          type: 'LineString' as const,
          coordinates: pts
            .sort((a, b) => a.stop_sequence - b.stop_sequence)
            .map((s) => [s.lon, s.lat]),
        },
        properties: { route_id: routeId, direction_id: Number(dirStr), color },
      }
    })

    // Stop point source — deduplicate by stop_id (only populated in single-route mode)
    const seenStops = new Set<string>()
    const stopFeatures = stopRows.flatMap((s) => {
      if (seenStops.has(s.stop_id)) return []
      seenStops.add(s.stop_id)
      return [{
        type: 'Feature' as const,
        geometry: { type: 'Point' as const, coordinates: [s.lon, s.lat] },
        properties: { stop_id: s.stop_id, stop_name: s.stop_name },
      }]
    })

    const lineSrc = map.getSource('route-line') as { setData: (d: unknown) => void } | undefined
    const stopSrc = map.getSource('route-stops') as { setData: (d: unknown) => void } | undefined
    lineSrc?.setData({ type: 'FeatureCollection', features: lineFeatures })
    stopSrc?.setData({ type: 'FeatureCollection', features: stopFeatures })
  }, [mapReady, routeStops, routeColors])

  // Update stop circle color when route selection changes
  // Route line colors are embedded per-feature so don't need setPaintProperty
  useEffect(() => {
    if (!mapReady) return
    const map = mapRef.current
    if (!map) return
    // Stop circles only show in single-route mode — use that route's color
    const stopColor = routeColors.size === 1
      ? Array.from(routeColors.values())[0]
      : DEFAULT_ROUTE_COLOR
    map.setPaintProperty('stop-circles', 'circle-stroke-color', stopColor)
  }, [mapReady, routeColors])

  // Dead reckoning — update positions every second
  useEffect(() => {
    if (!mapReady) return

    const interval = setInterval(() => {
      const map = mapRef.current
      if (!map) return
      const src = map.getSource('vehicles') as { setData: (d: unknown) => void } | undefined
      if (!src) return

      const now = Date.now()
      const baseMs = fetchedAtMsRef.current ?? now
      const elapsed = (now - baseMs) / 1000

      src.setData({
        type: 'FeatureCollection',
        features: vehiclesRef.current.map((v) => {
          const [lat, lon] = deadReckon(v.lat, v.lon, v.bearing, v.speed_mps, elapsed)
          return {
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [lon, lat] },
            properties: {
              vehicle_id: v.vehicle_id,
              headsign: v.headsign,
              trip_id: v.trip_id,
              route_id: v.route_id,
              direction_id: v.direction_id,
              bearing: v.bearing,
              speed_mps: v.speed_mps,
              delay_seconds: v.delay_seconds,
              otp_status: v.otp_status,
              color: otpColor(v.otp_status),
            },
          }
        }),
      })

      // Keep selected-vehicle source tracking the selected vehicle's dead-reckoned position
      const selSrc = map.getSource('selected-vehicle') as { setData: (d: unknown) => void } | undefined
      if (selSrc) {
        const selV = vehiclesRef.current.find((v) => v.vehicle_id === selectedVehicleIdRef.current)
        if (selV) {
          const [slat, slon] = deadReckon(selV.lat, selV.lon, selV.bearing, selV.speed_mps, elapsed)
          selSrc.setData({
            type: 'FeatureCollection',
            features: [{
              type: 'Feature',
              geometry: { type: 'Point', coordinates: [slon, slat] },
              properties: { color: otpColor(selV.otp_status) },
            }],
          })
        } else {
          selSrc.setData({ type: 'FeatureCollection', features: [] })
        }
      }
    }, 1_000)

    return () => clearInterval(interval)
  }, [mapReady])

  return (
    <>
      <style>{`
        .vp-popup .maplibregl-popup-content {
          background: #0f172a; border: 1px solid #1e293b;
          border-radius: 12px; padding: 0;
          box-shadow: 0 20px 40px rgba(0,0,0,0.6); font-family: inherit;
          min-width: 180px;
        }
        .vp-popup .maplibregl-popup-tip { border-top-color: #1e293b; }
        .vp-inner { padding: 12px 14px; }
        .vp-header { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }
        .vp-badge { font-size: 10px; font-weight: 600; padding: 2px 7px; border-radius: 99px; border: 1px solid; text-transform: capitalize; }
        .vp-vid { color: #94a3b8; font-size: 11px; }
        .vp-delay { color: #f1f5f9; font-weight: 600; font-size: 13px; margin-bottom: 8px; }
        .vp-rows { border-top: 1px solid #1e293b; padding-top: 8px; display: flex; flex-direction: column; gap: 3px; }
        .vp-row { display: flex; justify-content: space-between; gap: 16px; font-size: 12px; }
        .vp-row span:first-child { color: #64748b; }
        .vp-row span:last-child { color: #cbd5e1; font-weight: 500; }
      `}</style>
      <div ref={containerRef} className="flex-1 h-full w-full" />
    </>
  )
}
