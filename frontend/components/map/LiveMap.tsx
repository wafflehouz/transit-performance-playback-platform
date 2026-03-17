'use client'

import { useEffect, useRef, useState } from 'react'
import type { Map as MaplibreMap, Popup } from 'maplibre-gl'
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
}

interface Props {
  vehicles: LiveVehicle[]
  loading?: boolean
}

const MAPTILER_KEY = process.env.NEXT_PUBLIC_MAPTILER_KEY!

function speedColor(speed_mps: number | null): string {
  if (speed_mps === null) return '#6b7280'
  const mph = speed_mps * 2.237
  if (mph > 18) return '#10b981'
  if (mph > 8)  return '#f59e0b'
  if (mph > 1)  return '#f97316'
  return '#ef4444'
}

export default function LiveMap({ vehicles, loading = false }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef = useRef<MaplibreMap | null>(null)
  const popupRef = useRef<Popup | null>(null)
  const [mapReady, setMapReady] = useState(false)

  // Init map once
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return
    let map: MaplibreMap

    import('maplibre-gl').then((ml) => {
      map = new ml.Map({
        container: containerRef.current!,
        style: `https://api.maptiler.com/maps/dataviz-dark/style.json?key=${MAPTILER_KEY}`,
        center: [-112.074, 33.449],
        zoom: 10.5,
        attributionControl: false,
      })

      map.addControl(new ml.NavigationControl({ showCompass: false }), 'bottom-right')
      map.addControl(new ml.AttributionControl({ compact: true }), 'bottom-left')

      popupRef.current = new ml.Popup({
        closeButton: false,
        closeOnClick: false,
        className: 'vehicle-popup',
      })

      map.on('load', () => {
        map.addSource('vehicles', {
          type: 'geojson',
          data: { type: 'FeatureCollection', features: [] },
        })

        // Glow ring
        map.addLayer({
          id: 'vehicle-glow',
          type: 'circle',
          source: 'vehicles',
          paint: {
            'circle-radius': 14,
            'circle-color': ['get', 'color'],
            'circle-opacity': 0.12,
            'circle-blur': 1,
          },
        })

        // Main dot
        map.addLayer({
          id: 'vehicle-dot',
          type: 'circle',
          source: 'vehicles',
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 9, 5, 12, 8, 15, 11],
            'circle-color': ['get', 'color'],
            'circle-stroke-width': 1.5,
            'circle-stroke-color': '#111827',
          },
        })

        // Route label at zoom 12+
        map.addLayer({
          id: 'vehicle-label',
          type: 'symbol',
          source: 'vehicles',
          minzoom: 12,
          layout: {
            'text-field': ['get', 'label'],
            'text-size': 10,
            'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
            'text-offset': [0, -1.6],
            'text-allow-overlap': false,
          },
          paint: {
            'text-color': '#ffffff',
            'text-halo-color': '#111827',
            'text-halo-width': 1,
          },
        })

        map.on('mouseenter', 'vehicle-dot', (e) => {
          map.getCanvas().style.cursor = 'pointer'
          const feat = e.features?.[0]
          if (!feat || feat.geometry.type !== 'Point') return
          const p = feat.properties as Record<string, unknown>
          const speedMph = p.speed_mps != null ? ((p.speed_mps as number) * 2.237).toFixed(0) + ' mph' : 'Unknown'
          const ts = p.timestamp
            ? new Date((p.timestamp as number) * 1000).toLocaleTimeString('en-US', {
                hour: '2-digit', minute: '2-digit', timeZone: 'America/Phoenix',
              })
            : '—'

          popupRef.current
            ?.setLngLat(feat.geometry.coordinates as [number, number])
            .setHTML(`
              <div class="vp-inner">
                <div class="vp-route">Vehicle ${p.vehicle_id}</div>
                <div class="vp-row"><span>Trip</span><span>${p.trip_id ?? '—'}</span></div>
                <div class="vp-row"><span>Speed</span><span>${speedMph}</span></div>
                <div class="vp-row"><span>Dir</span><span>${p.direction_id === 0 ? 'Outbound →' : p.direction_id === 1 ? 'Inbound ←' : '—'}</span></div>
                <div class="vp-row"><span>As of</span><span>${ts}</span></div>
              </div>
            `)
            .addTo(map)
        })

        map.on('mouseleave', 'vehicle-dot', () => {
          map.getCanvas().style.cursor = ''
          popupRef.current?.remove()
        })

        mapRef.current = map
        setMapReady(true)
      })
    })

    return () => { map?.remove() }
  }, [])

  // Update data
  useEffect(() => {
    if (!mapReady || !mapRef.current) return
    const src = mapRef.current.getSource('vehicles') as { setData: (d: unknown) => void } | undefined
    if (!src) return

    src.setData({
      type: 'FeatureCollection',
      features: vehicles.map((v) => ({
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [v.lon, v.lat] },
        properties: {
          vehicle_id: v.vehicle_id,
          trip_id: v.trip_id,
          route_id: v.route_id,
          direction_id: v.direction_id,
          speed_mps: v.speed_mps,
          timestamp: v.timestamp,
          label: v.route_id ?? '',
          color: speedColor(v.speed_mps),
        },
      })),
    })
  }, [vehicles, mapReady])

  return (
    <>
      <style>{`
        .vehicle-popup .maplibregl-popup-content {
          background: #111827; border: 1px solid #374151;
          border-radius: 10px; padding: 0;
          box-shadow: 0 10px 25px rgba(0,0,0,0.5); font-family: inherit;
        }
        .vehicle-popup .maplibregl-popup-tip { border-top-color: #374151; }
        .vp-inner { padding: 10px 12px; }
        .vp-route { color: #f9fafb; font-weight: 600; font-size: 13px; margin-bottom: 6px; }
        .vp-row { display: flex; justify-content: space-between; gap: 16px; font-size: 12px; color: #9ca3af; margin-top: 3px; }
        .vp-row span:last-child { color: #e5e7eb; font-weight: 500; }
      `}</style>

      {/* Loading shimmer over map */}
      {loading && (
        <div className="absolute top-4 left-1/2 -translate-x-1/2 z-20 bg-gray-900/90 border border-gray-700 rounded-full px-4 py-1.5 text-xs text-gray-300 backdrop-blur-sm">
          Loading vehicles…
        </div>
      )}

      <div ref={containerRef} className="flex-1 h-full w-full" />
    </>
  )
}
