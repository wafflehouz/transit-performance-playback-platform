'use client'

import { useEffect, useRef, useState } from 'react'
import type { Map as MaplibreMap, Popup } from 'maplibre-gl'
import type { RouteStop } from '@/types'

export type OtpStatus = 'early' | 'on_time' | 'late' | 'very_late' | 'unknown'

export interface LiveVehicle {
  vehicle_id: string
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
  routeColor: string | null   // GTFS route_color (#RRGGBB) — null falls back to default
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

export default function LiveMap({ vehicles, fetchedAtMs, routeStops, routeColor }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef = useRef<MaplibreMap | null>(null)
  const popupRef = useRef<Popup | null>(null)
  const vehiclesRef = useRef<LiveVehicle[]>(vehicles)
  const fetchedAtMsRef = useRef<number | null>(fetchedAtMs)
  const [mapReady, setMapReady] = useState(false)

  // Keep refs current for the dead reckoning interval
  useEffect(() => { vehiclesRef.current = vehicles }, [vehicles])
  useEffect(() => { fetchedAtMsRef.current = fetchedAtMs }, [fetchedAtMs])

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

        // Outer glow
        map.addLayer({
          id: 'veh-glow',
          type: 'circle',
          source: 'vehicles',
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 9, 10, 13, 18],
            'circle-color': ['get', 'color'],
            'circle-opacity': 0.15,
            'circle-blur': 1,
          },
        })

        // Main circle
        map.addLayer({
          id: 'veh-circle',
          type: 'circle',
          source: 'vehicles',
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 9, 5, 12, 8, 15, 12],
            'circle-color': ['get', 'color'],
            'circle-stroke-width': 2,
            'circle-stroke-color': '#111827',
          },
        })

        // Directional tick (bearing indicator) — small line extending from center
        map.addLayer({
          id: 'veh-bearing',
          type: 'symbol',
          source: 'vehicles',
          layout: {
            'icon-image': 'bearing-arrow',
            'icon-size': ['interpolate', ['linear'], ['zoom'], 9, 0.3, 13, 0.5],
            'icon-rotate': ['coalesce', ['get', 'bearing'], 0],
            'icon-rotation-alignment': 'map',
            'icon-allow-overlap': true,
          },
          paint: {
            'icon-opacity': ['case', ['==', ['get', 'bearing'], null], 0, 0.9],
          },
        })

        // Route label at zoom 13+
        map.addLayer({
          id: 'veh-label',
          type: 'symbol',
          source: 'vehicles',
          minzoom: 13,
          layout: {
            'text-field': ['get', 'route_id'],
            'text-size': 10,
            'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
            'text-offset': [0, -1.8],
            'text-allow-overlap': false,
          },
          paint: {
            'text-color': '#ffffff',
            'text-halo-color': '#111827',
            'text-halo-width': 1.5,
          },
        })

        // Create bearing arrow image (small upward-pointing chevron)
        const size = 64
        const canvas = document.createElement('canvas')
        canvas.width = size; canvas.height = size
        const ctx = canvas.getContext('2d')!
        ctx.fillStyle = '#ffffff'
        ctx.beginPath()
        // Arrow pointing up (north = 0°)
        ctx.moveTo(size / 2, 4)
        ctx.lineTo(size / 2 + 8, size / 2 + 4)
        ctx.lineTo(size / 2, size / 2 - 2)
        ctx.lineTo(size / 2 - 8, size / 2 + 4)
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
            'line-color': '#38bdf8',
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
          minzoom: 14,
          layout: {
            'text-field': ['coalesce', ['get', 'stop_name'], ['get', 'stop_id']],
            'text-size': 10,
            'text-font': ['Open Sans Regular', 'Arial Unicode MS Regular'],
            'text-offset': [0, 1.4],
            'text-anchor': 'top',
            'text-allow-overlap': false,
          },
          paint: {
            'text-color': '#94a3b8',
            'text-halo-color': '#0f172a',
            'text-halo-width': 1.5,
          },
        }, 'veh-glow')

        // Hover popup
        map.on('mouseenter', 'veh-circle', (e) => {
          map.getCanvas().style.cursor = 'pointer'
          const feat = e.features?.[0]
          if (!feat || feat.geometry.type !== 'Point') return
          const p = feat.properties as Record<string, unknown>
          const dir = p.direction_id === 0 ? 'Outbound' : p.direction_id === 1 ? 'Inbound' : '—'
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
                  <div class="vp-row"><span>Direction</span><span>${dir}</span></div>
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

    return () => { map?.remove() }
  }, [])

  // Update route line + stop markers when routeStops changes
  useEffect(() => {
    if (!mapReady) return
    const map = mapRef.current
    if (!map) return

    // Split into shape points (for the line) and stop points (for the markers)
    const shapeRows = routeStops.filter((s) => s.point_type === 'shape')
    const stopRows  = routeStops.filter((s) => s.point_type === 'stop')

    // Fall back to all points as the line if no shape data returned
    const lineSource = shapeRows.length > 0 ? shapeRows : routeStops

    // Group line points by direction to build one LineString per direction
    const byDir = new Map<number, RouteStop[]>()
    for (const s of lineSource) {
      const arr = byDir.get(s.direction_id) ?? []
      arr.push(s)
      byDir.set(s.direction_id, arr)
    }

    // Route line source — one LineString per direction, ordered by stop_sequence
    const lineFeatures = Array.from(byDir.entries()).map(([dir, pts]) => ({
      type: 'Feature' as const,
      geometry: {
        type: 'LineString' as const,
        coordinates: pts
          .sort((a, b) => a.stop_sequence - b.stop_sequence)
          .map((s) => [s.lon, s.lat]),
      },
      properties: { direction_id: dir },
    }))

    // Stop point source — deduplicate by stop_id
    const seenStops = new Set<string>()
    const stopFeatures = (stopRows.length > 0 ? stopRows : routeStops).flatMap((s) => {
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
  }, [mapReady, routeStops])

  // Update route line + stop colors when route changes
  useEffect(() => {
    if (!mapReady) return
    const map = mapRef.current
    if (!map) return
    const color = routeColor ?? DEFAULT_ROUTE_COLOR
    map.setPaintProperty('route-line-fill', 'line-color', color)
    map.setPaintProperty('stop-circles', 'circle-stroke-color', color)
  }, [mapReady, routeColor])

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
