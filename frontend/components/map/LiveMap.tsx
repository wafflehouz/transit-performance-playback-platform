'use client'

import { useEffect, useRef, useState } from 'react'
import type { Map as MaplibreMap, Popup } from 'maplibre-gl'
import type { LiveVehicle } from '@/lib/queries/live-ops'

interface Props {
  vehicles: LiveVehicle[]
}

const MAPTILER_KEY = process.env.NEXT_PUBLIC_MAPTILER_KEY!

// Speed → color (matches Swiftly green/yellow/orange/red)
function speedColor(speed_mps: number | null): string {
  if (speed_mps === null) return '#6b7280'   // gray — unknown
  const mph = speed_mps * 2.237
  if (mph > 18) return '#10b981'              // emerald — moving fast
  if (mph > 8) return '#f59e0b'               // amber — slow
  if (mph > 1) return '#f97316'               // orange — very slow
  return '#ef4444'                             // red — stopped/delayed
}

export default function LiveMap({ vehicles }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const mapRef = useRef<MaplibreMap | null>(null)
  const popupRef = useRef<Popup | null>(null)
  const [mapReady, setMapReady] = useState(false)

  // Init map once
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return

    let map: MaplibreMap

    import('maplibre-gl').then((maplibre) => {
      map = new maplibre.Map({
        container: containerRef.current!,
        style: `https://api.maptiler.com/maps/dataviz-dark/style.json?key=${MAPTILER_KEY}`,
        center: [-112.074, 33.449], // Phoenix downtown
        zoom: 10.5,
        attributionControl: false,
      })

      map.addControl(new maplibre.NavigationControl({ showCompass: false }), 'bottom-right')
      map.addControl(new maplibre.AttributionControl({ compact: true }), 'bottom-left')

      popupRef.current = new maplibre.Popup({
        closeButton: false,
        closeOnClick: false,
        className: 'vehicle-popup',
      })

      map.on('load', () => {
        // Vehicle positions source
        map.addSource('vehicles', {
          type: 'geojson',
          data: { type: 'FeatureCollection', features: [] },
        })

        // Outer glow for stopped vehicles
        map.addLayer({
          id: 'vehicle-glow',
          type: 'circle',
          source: 'vehicles',
          paint: {
            'circle-radius': 12,
            'circle-color': ['get', 'color'],
            'circle-opacity': 0.15,
            'circle-blur': 1,
          },
        })

        // Main vehicle dot
        map.addLayer({
          id: 'vehicle-dot',
          type: 'circle',
          source: 'vehicles',
          paint: {
            'circle-radius': [
              'interpolate', ['linear'], ['zoom'],
              9, 4,
              12, 7,
              15, 10,
            ],
            'circle-color': ['get', 'color'],
            'circle-stroke-width': 1.5,
            'circle-stroke-color': '#1f2937',
          },
        })

        // Route label at higher zoom
        map.addLayer({
          id: 'vehicle-label',
          type: 'symbol',
          source: 'vehicles',
          minzoom: 12,
          layout: {
            'text-field': ['get', 'route_short_name'],
            'text-size': 10,
            'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
            'text-offset': [0, -1.5],
            'text-allow-overlap': false,
          },
          paint: {
            'text-color': '#ffffff',
            'text-halo-color': '#111827',
            'text-halo-width': 1,
          },
        })

        // Hover popup
        map.on('mouseenter', 'vehicle-dot', (e) => {
          map.getCanvas().style.cursor = 'pointer'
          const feat = e.features?.[0]
          if (!feat || !feat.geometry || feat.geometry.type !== 'Point') return
          const props = feat.properties as Record<string, unknown>
          const speedMph = props.speed_mps != null ? ((props.speed_mps as number) * 2.237).toFixed(0) : 'N/A'
          const time = props.event_ts
            ? new Date(props.event_ts as string).toLocaleTimeString('en-US', {
                hour: '2-digit', minute: '2-digit', timeZone: 'America/Phoenix',
              })
            : '—'

          popupRef.current
            ?.setLngLat(feat.geometry.coordinates as [number, number])
            .setHTML(`
              <div class="vehicle-popup-inner">
                <div class="vp-route">Route ${props.route_short_name ?? '—'}</div>
                <div class="vp-row"><span>Vehicle</span><span>${props.vehicle_id}</span></div>
                <div class="vp-row"><span>Speed</span><span>${speedMph} mph</span></div>
                <div class="vp-row"><span>As of</span><span>${time}</span></div>
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

  // Update vehicle positions whenever data changes
  useEffect(() => {
    if (!mapReady || !mapRef.current) return
    const source = mapRef.current.getSource('vehicles') as { setData: (d: unknown) => void } | undefined
    if (!source) return

    source.setData({
      type: 'FeatureCollection',
      features: vehicles.map((v) => ({
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [v.lon, v.lat] },
        properties: {
          vehicle_id: v.vehicle_id,
          trip_id: v.trip_id,
          route_id: v.route_id,
          route_short_name: v.route_short_name ?? v.route_id ?? '',
          direction_id: v.direction_id,
          speed_mps: v.speed_mps,
          bearing: v.bearing,
          event_ts: v.event_ts,
          color: speedColor(v.speed_mps),
        },
      })),
    })
  }, [vehicles, mapReady])

  return (
    <>
      <style>{`
        .vehicle-popup .maplibregl-popup-content {
          background: #111827;
          border: 1px solid #374151;
          border-radius: 10px;
          padding: 0;
          box-shadow: 0 10px 25px rgba(0,0,0,0.5);
          font-family: inherit;
        }
        .vehicle-popup .maplibregl-popup-tip { border-top-color: #374151; }
        .vehicle-popup-inner { padding: 10px 12px; }
        .vp-route { color: #f9fafb; font-weight: 600; font-size: 13px; margin-bottom: 6px; }
        .vp-row { display: flex; justify-content: space-between; gap: 16px; font-size: 12px; color: #9ca3af; margin-top: 2px; }
        .vp-row span:last-child { color: #e5e7eb; font-weight: 500; }
      `}</style>
      <div ref={containerRef} className="flex-1 h-full w-full" />
    </>
  )
}
