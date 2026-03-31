import type { Metadata } from 'next'
import { Geist, Geist_Mono } from 'next/font/google'
import './globals.css'

const geistSans = Geist({ variable: '--font-geist-sans', subsets: ['latin'] })
const geistMono = Geist_Mono({ variable: '--font-geist-mono', subsets: ['latin'] })

export const metadata: Metadata = {
  title: 'Transit Performance Platform',
  description: 'Valley Metro planner performance intelligence',
  openGraph: {
    title: 'Transit Performance Platform',
    description: 'Real-time and historical transit performance analytics for the Phoenix metro network.',
    url: 'https://www.phx-transit-analytics.com',
    siteName: 'PHX Transit Analytics',
    images: [
      {
        url: 'https://www.phx-transit-analytics.com/og-image.jpg',
        width: 1200,
        height: 630,
        alt: 'Downtown Phoenix light rail',
      },
    ],
    type: 'website',
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Transit Performance Platform',
    description: 'Real-time and historical transit performance analytics for the Phoenix metro network.',
    images: ['https://www.phx-transit-analytics.com/og-image.jpg'],
  },
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className={`${geistSans.variable} ${geistMono.variable} antialiased bg-gray-950 text-white`}>
        {children}
      </body>
    </html>
  )
}
