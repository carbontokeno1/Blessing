-- Adaptive 14-day Paper Trading experiment and auditable equity marks.
create table if not exists public.blessing_dual_experiments (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null,
  started_at timestamptz not null default now(),
  ends_at timestamptz not null,
  status text not null default 'active' check (status in ('active','completed','stopped','failed')),
  markets text[] not null default array['crypto','stocks']::text[],
  config jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint blessing_dual_experiments_valid_window check (ends_at > started_at)
);
create unique index if not exists blessing_dual_one_active_experiment_per_user
  on public.blessing_dual_experiments(user_id) where status = 'active';
create index if not exists blessing_dual_experiments_user_status_idx
  on public.blessing_dual_experiments(user_id,status,ends_at);

create table if not exists public.blessing_dual_equity_marks (
  id bigint generated always as identity primary key,
  experiment_id uuid not null references public.blessing_dual_experiments(id) on delete cascade,
  user_id uuid not null,
  market text not null check (market in ('crypto','stocks')),
  marked_at timestamptz not null default now(),
  cash_balance numeric not null,
  market_value numeric not null,
  equity numeric not null,
  realized_pnl numeric not null default 0,
  unrealized_pnl numeric not null default 0,
  fees_paid numeric not null default 0,
  estimated_slippage numeric not null default 0,
  net_pnl_after_costs numeric not null default 0,
  net_return_pct numeric not null default 0,
  peak_equity numeric not null,
  drawdown_pct numeric not null default 0,
  unique(experiment_id,market,marked_at)
);
create index if not exists blessing_dual_equity_marks_exp_market_time_idx
  on public.blessing_dual_equity_marks(experiment_id,market,marked_at desc);

alter table public.blessing_dual_experiments enable row level security;
alter table public.blessing_dual_equity_marks enable row level security;
revoke all on public.blessing_dual_experiments from anon, authenticated;
revoke all on public.blessing_dual_equity_marks from anon, authenticated;
grant all on public.blessing_dual_experiments to service_role;
grant all on public.blessing_dual_equity_marks to service_role;
