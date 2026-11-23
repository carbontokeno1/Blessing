-- Remove the reserved-cash deduction from both existing and future Paper portfolios.
-- Available cash can be deployed subject to the existing per-trade, daily, weekly,
-- drawdown, open-position, and aggregate-risk limits.
alter table public.blessing_dual_portfolios
  alter column reserve_cash_pct set default 0;

update public.blessing_dual_portfolios
set reserve_cash_pct = 0,
    updated_at = now()
where reserve_cash_pct <> 0;
