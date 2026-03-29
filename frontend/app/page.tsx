import { redirect } from 'next/navigation'

export default function RootPage({
  searchParams,
}: {
  searchParams: { code?: string; type?: string }
}) {
  // Supabase password recovery links redirect to the site root with ?code=
  if (searchParams.code && searchParams.type === 'recovery') {
    redirect(`/reset-password?code=${searchParams.code}`)
  }
  if (searchParams.code) {
    redirect(`/reset-password?code=${searchParams.code}`)
  }
  redirect('/dashboard')
}
