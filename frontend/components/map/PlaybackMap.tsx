'use client'

import { useEffect, useRef, useState } from 'react'
import type { Map as MaplibreMap } from 'maplibre-gl'
import type { CongestionHex } from '@/types'

const CONGESTION_COLOR: Record<string, string> = {
  free_flow: '#22c55e',
  moderate:  '#eab308',
  congested: '#f97316',
  severe:    '#ef4444',
}

export interface PlaybackPoint {
  tsMs: number
  lat: number
  lon: number
  bearing: number | null
  speed: number | null
}

export interface PlaybackStop {
  stop_id: string
  stop_name: string | null
  lat: number | null
  lon: number | null
  arrival_delay_seconds: number | null
  pickup_type: number
}

interface Props {
  points: PlaybackPoint[]
  currentIdx: number          // index into points; -1 = before start
  stops: PlaybackStop[]
  routeColor: string | null
  congestionHexes: CongestionHex[]  // hexes for current 15-min time bucket
  showTraffic: boolean
}

const MAPTILER_KEY = process.env.NEXT_PUBLIC_MAPTILER_KEY!
const DEFAULT_COLOR = '#38bdf8'

function stopOtpColor(delay: number | null, pickupType: number): string {
  if (delay === null) return '#6b7280'
  if (delay < -60 && pickupType === 0) return '#ec4899'  // early
  if (delay <= 360) return '#22c55e'                     // on-time (incl. early drop-off-only)
  if (delay <= 600) return '#f59e0b'                     // late
  return '#ef4444'                                       // very late
}

type GeoSource = { setData: (d: unknown) => void } | undefined

export default function PlaybackMap({ points, currentIdx, stops, routeColor, congestionHexes, showTraffic }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef       = useRef<MaplibreMap | null>(null)
  const [mapReady, setMapReady] = useState(false)

  // ── Init map once ─────────────────────────────────────────────────────────
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

      map.on('load', () => {
        // Sources
        for (const id of ['traffic', 'path-past', 'path-future', 'stops', 'vehicle'] as const) {
          map.addSource(id, { type: 'geojson', data: { type: 'FeatureCollection', features: [] } })
        }

        // Traffic hex fill (rendered first — everything else sits on top)
        map.addLayer({
          id: 'traffic-fill',
          type: 'fill',
          source: 'traffic',
          paint: {
            'fill-color': ['get', 'color'],
            'fill-opacity': 0.35,
          },
        })
        map.addLayer({
          id: 'traffic-border',
          type: 'line',
          source: 'traffic',
          paint: {
            'line-color': ['get', 'color'],
            'line-width': 0.8,
            'line-opacity': 0.6,
          },
        })

        // Future path — dim dashed (hidden when traffic overlay active)
        map.addLayer({
          id: 'path-future-line',
          type: 'line',
          source: 'path-future',
          paint: {
            'line-color': '#475569',
            'line-width': ['interpolate', ['linear'], ['zoom'], 9, 2, 13, 3],
            'line-dasharray': [3, 2],
            'line-opacity': 0.45,
          },
        })

        // Past path — casing + colored fill
        map.addLayer({
          id: 'path-past-casing',
          type: 'line',
          source: 'path-past',
          paint: {
            'line-color': '#1e293b',
            'line-width': ['interpolate', ['linear'], ['zoom'], 9, 5, 13, 8],
            'line-opacity': 0.8,
          },
        })
        map.addLayer({
          id: 'path-past-line',
          type: 'line',
          source: 'path-past',
          paint: {
            'line-color': DEFAULT_COLOR,
            'line-width': ['interpolate', ['linear'], ['zoom'], 9, 3, 13, 5],
            'line-opacity': 0.9,
          },
        })

        // Stop circles (OTP colored stroke)
        map.addLayer({
          id: 'stop-circles',
          type: 'circle',
          source: 'stops',
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 10, 4, 14, 7],
            'circle-color': '#0f172a',
            'circle-stroke-width': 2,
            'circle-stroke-color': ['get', 'color'],
          },
        })

        // Stop name labels at zoom 13+
        map.addLayer({
          id: 'stop-labels',
          type: 'symbol',
          source: 'stops',
          minzoom: 13,
          layout: {
            'text-field': ['coalesce', ['get', 'stop_name'], ['get', 'stop_id']],
            'text-size': 11,
            'text-font': ['Open Sans Semibold', 'Open Sans Bold', 'Arial Unicode MS Bold'],
            'text-offset': [0, 1.4],
            'text-anchor': 'top',
            'text-allow-overlap': false,
          },
          paint: {
            'text-color': '#e2e8f0',
            'text-halo-color': '#0f172a',
            'text-halo-width': 2,
          },
        })

        // Vehicle glow
        map.addLayer({
          id: 'veh-glow',
          type: 'circle',
          source: 'vehicle',
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 9, 16, 13, 24],
            'circle-color': ['get', 'color'],
            'circle-opacity': 0.18,
            'circle-blur': 1,
          },
        })

        // Vehicle circle
        map.addLayer({
          id: 'veh-circle',
          type: 'circle',
          source: 'vehicle',
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 9, 9, 12, 13, 15, 17],
            'circle-color': ['get', 'color'],
            'circle-opacity': 0.92,
          },
        })

        // Bearing arrow (same canvas approach as LiveMap)
        const size = 64, cx = size / 2
        const canvas = document.createElement('canvas')
        canvas.width = size; canvas.height = size
        const ctx = canvas.getContext('2d')!
        ctx.fillStyle = 'rgba(255,255,255,0.95)'
        ctx.beginPath()
        ctx.moveTo(cx, 8)
        ctx.lineTo(cx + 20, size - 12)
        ctx.lineTo(cx, size - 22)
        ctx.lineTo(cx - 20, size - 12)
        ctx.closePath()
        ctx.fill()
        map.addImage('bearing-arrow', {
          width: size, height: size,
          data: new Uint8Array(ctx.getImageData(0, 0, size, size).data),
        })

        map.addLayer({
          id: 'veh-bearing',
          type: 'symbol',
          source: 'vehicle',
          layout: {
            'icon-image': 'bearing-arrow',
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

        mapRef.current = map
        setMapReady(true)
      })
    })

    return () => { map?.remove() }
  }, [])

  // ── Route color ────────────────────────────────────────────────────────────
  useEffect(() => {
    if (!mapReady || !mapRef.current) return
    const color = routeColor ?? DEFAULT_COLOR
    mapRef.current.setPaintProperty('path-past-line', 'line-color', color)
  }, [mapReady, routeColor])

  // ── Fit bounds when path first loads ──────────────────────────────────────
  useEffect(() => {
    if (!mapReady || !mapRef.current || points.length === 0) return
    import('maplibre-gl').then((ml) => {
      const lngs = points.map((p) => p.lon)
      const lats  = points.map((p) => p.lat)
      const bounds = new ml.LngLatBounds(
        [Math.min(...lngs), Math.min(...lats)],
        [Math.max(...lngs), Math.max(...lats)],
      )
      mapRef.current?.fitBounds(bounds, { padding: 72, duration: 800 })
    })
  }, [mapReady, points])  // intentionally only fires when points identity changes

  // ── Path split + vehicle position ─────────────────────────────────────────
  useEffect(() => {
    if (!mapReady || !mapRef.current) return
    const map = mapRef.current

    if (points.length === 0) {
      for (const id of ['path-past', 'path-future', 'vehicle']) {
        ;(map.getSource(id) as GeoSource)?.setData({ type: 'FeatureCollection', features: [] })
      }
      return
    }

    const idx = currentIdx < 0 ? 0 : currentIdx
    const pastCoords   = points.slice(0, idx + 1).map((p) => [p.lon, p.lat])
    const futureCoords = points.slice(idx).map((p) => [p.lon, p.lat])

    ;(map.getSource('path-past') as GeoSource)?.setData({
      type: 'FeatureCollection',
      features: pastCoords.length >= 2
        ? [{ type: 'Feature', geometry: { type: 'LineString', coordinates: pastCoords }, properties: {} }]
        : [],
    })
    ;(map.getSource('path-future') as GeoSource)?.setData({
      type: 'FeatureCollection',
      features: futureCoords.length >= 2
        ? [{ type: 'Feature', geometry: { type: 'LineString', coordinates: futureCoords }, properties: {} }]
        : [],
    })

    if (currentIdx >= 0 && currentIdx < points.length) {
      const pt = points[currentIdx]
      const color = routeColor ?? DEFAULT_COLOR
      ;(map.getSource('vehicle') as GeoSource)?.setData({
        type: 'FeatureCollection',
        features: [{
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [pt.lon, pt.lat] },
          properties: { bearing: pt.bearing, color },
        }],
      })
    } else {
      ;(map.getSource('vehicle') as GeoSource)?.setData({ type: 'FeatureCollection', features: [] })
    }
  }, [mapReady, points, currentIdx, routeColor])

  // ── Stop markers ──────────────────────────────────────────────────────────
  useEffect(() => {
    if (!mapReady || !mapRef.current) return
    const features = stops
      .filter((s) => s.lat != null && s.lon != null)
      .map((s) => ({
        type: 'Feature' as const,
        geometry: { type: 'Point' as const, coordinates: [s.lon!, s.lat!] },
        properties: {
          stop_id: s.stop_id,
          stop_name: s.stop_name,
          color: stopOtpColor(s.arrival_delay_seconds, s.pickup_type),
        },
      }))
    ;(mapRef.current.getSource('stops') as GeoSource)?.setData({ type: 'FeatureCollection', features })
  }, [mapReady, stops])

  // ── Traffic visibility toggle ──────────────────────────────────────────────
  useEffect(() => {
    if (!mapReady || !mapRef.current) return
    const vis = (v: boolean) => v ? 'visible' : 'none'
    mapRef.current.setLayoutProperty('traffic-fill',     'visibility', vis(showTraffic))
    mapRef.current.setLayoutProperty('traffic-border',   'visibility', vis(showTraffic))
    mapRef.current.setLayoutProperty('path-future-line', 'visibility', vis(!showTraffic))
  }, [mapReady, showTraffic])

  // ── Congestion hex polygons ────────────────────────────────────────────────
  useEffect(() => {
    if (!mapReady || !mapRef.current) return

    if (congestionHexes.length === 0) {
      ;(mapRef.current.getSource('traffic') as GeoSource)?.setData({ type: 'FeatureCollection', features: [] })
      return
    }

    // Lazy-load h3-js (pure JS, client-only) to convert indices → GeoJSON polygons.
    // cellToBoundary returns [[lat, lon], ...]; GeoJSON needs [lon, lat].
    import('h3-js').then(({ cellToBoundary }) => {
      if (!mapRef.current) return
      // h3_index is stored in Databricks as a decimal string (e.g., "617726976648282111").
      // h3-js expects a hex string (e.g., "8929b6d02c7ffff"). Convert before calling cellToBoundary.
      const features = congestionHexes.flatMap((hex) => {
        try {
          const hexIndex = BigInt(hex.h3_index).toString(16)
          const boundary = cellToBoundary(hexIndex) // [[lat, lon], ...]
          const ring = boundary.map(([lat, lon]) => [lon, lat])
          ring.push(ring[0]) // close the polygon ring
          return [{
            type: 'Feature' as const,
            geometry: { type: 'Polygon' as const, coordinates: [ring] },
            properties: {
              congestion_level: hex.congestion_level,
              color: CONGESTION_COLOR[hex.congestion_level] ?? '#6b7280',
              avg_speed_mps: hex.avg_speed_mps,
            },
          }]
        } catch {
          return []
        }
      })
      ;(mapRef.current.getSource('traffic') as GeoSource)?.setData({ type: 'FeatureCollection', features })
    })
  }, [mapReady, congestionHexes])

  return <div ref={containerRef} className="flex-1 h-full w-full" />
}
