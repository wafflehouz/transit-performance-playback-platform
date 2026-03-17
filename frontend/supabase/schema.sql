-- ── Profiles (extends auth.users) ────────────────────────────────────────────
create table if not exists public.profiles (
  id          uuid primary key references auth.users(id) on delete cascade,
  email       text not null,
  full_name   text,
  role        text not null default 'planner' check (role in ('planner', 'admin')),
  created_at  timestamptz not null default now()
);

alter table public.profiles enable row level security;

create policy "Users can read own profile"
  on public.profiles for select
  using (auth.uid() = id);

create policy "Users can update own profile"
  on public.profiles for update
  using (auth.uid() = id);

-- Auto-create profile on signup
create or replace function public.handle_new_user()
returns trigger language plpgsql security definer set search_path = public as $$
begin
  insert into public.profiles (id, email)
  values (new.id, new.email);
  return new;
end;
$$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
  after insert on auth.users
  for each row execute procedure public.handle_new_user();


-- ── Route subscriptions ───────────────────────────────────────────────────────
create table if not exists public.route_subscriptions (
  id          uuid primary key default gen_random_uuid(),
  user_id     uuid not null references auth.users(id) on delete cascade,
  route_id    text,               -- null if subscribing to a whole group
  group_name  text,               -- null if subscribing to a single route
  frequency   text not null default 'weekly' check (frequency in ('weekly')),
  created_at  timestamptz not null default now(),
  constraint route_or_group check (
    (route_id is not null and group_name is null) or
    (route_id is null and group_name is not null)
  ),
  unique (user_id, route_id),
  unique (user_id, group_name)
);

alter table public.route_subscriptions enable row level security;

create policy "Users manage own subscriptions"
  on public.route_subscriptions for all
  using (auth.uid() = user_id);


-- ── Saved views (pinned routes / dates) ──────────────────────────────────────
create table if not exists public.saved_views (
  id          uuid primary key default gen_random_uuid(),
  user_id     uuid not null references auth.users(id) on delete cascade,
  label       text not null,
  route_id    text,
  group_name  text,
  direction_id int,
  created_at  timestamptz not null default now()
);

alter table public.saved_views enable row level security;

create policy "Users manage own saved views"
  on public.saved_views for all
  using (auth.uid() = user_id);
