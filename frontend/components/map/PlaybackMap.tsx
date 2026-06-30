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

export interface TrackData {
  points: PlaybackPoint[]
  currentIdx: number   // index into points; -1 = before start
  stops: PlaybackStop[]
  color: string
}

interface Props {
  tracks: TrackData[]          // up to 3
  congestionHexes: CongestionHex[]
  showTraffic: boolean
}

const MAPTILER_KEY = process.env.NEXT_PUBLIC_MAPTILER_KEY!
const MAX_TRACKS = 3
const EMPTY_FC = { type: 'FeatureCollection' as const, features: [] }

function stopOtpColor(delay: number | null, pickupType: number): string {
  if (delay === null) return '#6b7280'
  if (delay < -60 && pickupType === 0) return '#ec4899'
  if (delay <= 360) return '#22c55e'
  if (delay <= 600) return '#f59e0b'
  return '#ef4444'
}

type GeoSource = { setData: (d: unknown) => void } | undefined

export default function PlaybackMap({ tracks, congestionHexes, showTraffic }: Props) {
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
        // Shared sources (traffic, stops, vehicle)
        for (const id of ['traffic', 'stops', 'vehicle'] as const) {
          map.addSource(id, { type: 'geojson', data: EMPTY_FC })
        }

        // Per-slot path sources — always register all 3 slots
        for (let n = 0; n < MAX_TRACKS; n++) {
          map.addSource(`path-past-${n}`,   { type: 'geojson', data: EMPTY_FC })
          map.addSource(`path-future-${n}`, { type: 'geojson', data: EMPTY_FC })
        }

        // Traffic hex fill (rendered first — everything else sits on top)
        map.addLayer({
          id: 'traffic-fill',
          type: 'fill',
          source: 'traffic',
          paint: { 'fill-color': ['get', 'color'], 'fill-opacity': 0.35 },
        })
        map.addLayer({
          id: 'traffic-border',
          type: 'line',
          source: 'traffic',
          paint: { 'line-color': ['get', 'color'], 'line-width': 0.8, 'line-opacity': 0.6 },
        })

        // Per-slot path layers
        for (let n = 0; n < MAX_TRACKS; n++) {
          map.addLayer({
            id: `path-future-line-${n}`,
            type: 'line',
            source: `path-future-${n}`,
            paint: {
              'line-color': '#475569',
              'line-width': ['interpolate', ['linear'], ['zoom'], 9, 2, 13, 3],
              'line-dasharray': [3, 2],
              'line-opacity': 0.45,
            },
          })
          map.addLayer({
            id: `path-past-casing-${n}`,
            type: 'line',
            source: `path-past-${n}`,
            paint: {
              'line-color': '#1e293b',
              'line-width': ['interpolate', ['linear'], ['zoom'], 9, 5, 13, 8],
              'line-opacity': 0.8,
            },
          })
          map.addLayer({
            id: `path-past-line-${n}`,
            type: 'line',
            source: `path-past-${n}`,
            paint: {
              'line-color': '#38bdf8',
              'line-width': ['interpolate', ['linear'], ['zoom'], 9, 3, 13, 5],
              'line-opacity': 0.9,
            },
          })
        }

        // Stop circles (OTP colored stroke, shared across all tracks)
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

        // Vehicle layers — single source, multiple features with data-driven color
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

  // ── Route colors per slot ─────────────────────────────────────────────────
  useEffect(() => {
    if (!mapReady || !mapRef.current) return
    for (let n = 0; n < MAX_TRACKS; n++) {
      const color = tracks[n]?.color ?? '#38bdf8'
      mapRef.current.setPaintProperty(`path-past-line-${n}`, 'line-color', color)
    }
  }, [mapReady, tracks])

  // ── Fit bounds when path data loads or changes ───────────────────────────
  // Keyed on point counts per slot (stable string) — avoids re-fitting on
  // every 60fps render caused by the tracks prop being recreated each frame.
  const pointCountKey = tracks.map((t) => t.points.length).join(',')
  useEffect(() => {
    if (!mapReady || !mapRef.current) return
    const allPoints = tracks.flatMap((t) => t.points)
    if (allPoints.length === 0) return
    import('maplibre-gl').then((ml) => {
      const lngs = allPoints.map((p) => p.lon)
      const lats  = allPoints.map((p) => p.lat)
      const bounds = new ml.LngLatBounds(
        [Math.min(...lngs), Math.min(...lats)],
        [Math.max(...lngs), Math.max(...lats)],
      )
      mapRef.current?.fitBounds(bounds, { padding: 72, duration: 800 })
    })
  }, [mapReady, pointCountKey]) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Path split + vehicle positions ────────────────────────────────────────
  useEffect(() => {
    if (!mapReady || !mapRef.current) return
    const map = mapRef.current

    const vehicleFeatures: unknown[] = []

    for (let n = 0; n < MAX_TRACKS; n++) {
      const track = tracks[n]

      if (!track || track.points.length === 0) {
        ;(map.getSource(`path-past-${n}`)   as GeoSource)?.setData(EMPTY_FC)
        ;(map.getSource(`path-future-${n}`) as GeoSource)?.setData(EMPTY_FC)
        continue
      }

      const { points, currentIdx, color } = track
      const idx         = currentIdx < 0 ? 0 : currentIdx
      const pastCoords   = points.slice(0, idx + 1).map((p) => [p.lon, p.lat])
      const futureCoords = points.slice(idx).map((p) => [p.lon, p.lat])

      ;(map.getSource(`path-past-${n}`) as GeoSource)?.setData({
        type: 'FeatureCollection',
        features: pastCoords.length >= 2
          ? [{ type: 'Feature', geometry: { type: 'LineString', coordinates: pastCoords }, properties: {} }]
          : [],
      })
      ;(map.getSource(`path-future-${n}`) as GeoSource)?.setData({
        type: 'FeatureCollection',
        features: futureCoords.length >= 2
          ? [{ type: 'Feature', geometry: { type: 'LineString', coordinates: futureCoords }, properties: {} }]
          : [],
      })

      if (currentIdx >= 0 && currentIdx < points.length) {
        const pt = points[currentIdx]
        vehicleFeatures.push({
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [pt.lon, pt.lat] },
          properties: { bearing: pt.bearing, color },
        })
      }
    }

    ;(map.getSource('vehicle') as GeoSource)?.setData({
      type: 'FeatureCollection',
      features: vehicleFeatures,
    })
  }, [mapReady, tracks])

  // ── Stop markers — flatten all tracks ────────────────────────────────────
  useEffect(() => {
    if (!mapReady || !mapRef.current) return
    const features = tracks.flatMap(({ stops, color }) =>
      stops
        .filter((s) => s.lat != null && s.lon != null)
        .map((s) => ({
          type: 'Feature' as const,
          geometry: { type: 'Point' as const, coordinates: [s.lon!, s.lat!] },
          properties: {
            stop_id:   s.stop_id,
            stop_name: s.stop_name,
            color:     stopOtpColor(s.arrival_delay_seconds, s.pickup_type),
          },
        }))
    )
    ;(mapRef.current.getSource('stops') as GeoSource)?.setData({ type: 'FeatureCollection', features })
  }, [mapReady, tracks])

  // ── Traffic visibility toggle ──────────────────────────────────────────────
  useEffect(() => {
    if (!mapReady || !mapRef.current) return
    const vis = (v: boolean) => v ? 'visible' : 'none'
    mapRef.current.setLayoutProperty('traffic-fill',   'visibility', vis(showTraffic))
    mapRef.current.setLayoutProperty('traffic-border', 'visibility', vis(showTraffic))
    for (let n = 0; n < MAX_TRACKS; n++) {
      mapRef.current.setLayoutProperty(`path-future-line-${n}`, 'visibility', vis(!showTraffic))
    }
  }, [mapReady, showTraffic])

  // ── Congestion hex polygons ────────────────────────────────────────────────
  useEffect(() => {
    if (!mapReady || !mapRef.current) return

    if (congestionHexes.length === 0) {
      ;(mapRef.current.getSource('traffic') as GeoSource)?.setData(EMPTY_FC)
      return
    }

    import('h3-js').then(({ cellToBoundary }) => {
      if (!mapRef.current) return
      const features = congestionHexes.flatMap((hex) => {
        try {
          const hexIndex = BigInt(hex.h3_index).toString(16)
          const boundary = cellToBoundary(hexIndex)
          const ring = boundary.map(([lat, lon]) => [lon, lat])
          ring.push(ring[0])
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
