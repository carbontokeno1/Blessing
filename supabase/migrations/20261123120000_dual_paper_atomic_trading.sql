-- Isolated, paper-only execution for BLESSING's dual portfolios.
-- Additive and safe to replay against a database that already has these tables.
alter table public.blessing_dual_orders
  add column if not exists idempotency_key text;

create unique index if not exists blessing_dual_orders_user_idempotency_uidx
  on public.blessing_dual_orders (user_id, idempotency_key)
  where idempotency_key is not null;

create unique index if not exists blessing_dual_portfolios_user_market_uidx
  on public.blessing_dual_portfolios (user_id, market);

create or replace function public.blessing_dual_execute_order(
  p_user_id uuid, p_market text, p_symbol text, p_side text,
  p_quantity numeric, p_price numeric,
  p_stop_loss_price numeric default null,
  p_take_profit_price numeric default null,
  p_position_id uuid default null,
  p_idempotency_key text default null
) returns jsonb
language plpgsql
security invoker
set search_path = ''
as $$
declare
  v_portfolio public.blessing_dual_portfolios%rowtype;
  v_position public.blessing_dual_positions%rowtype;
  v_existing public.blessing_dual_orders%rowtype;
  v_fee numeric;
  v_notional numeric;
  v_equity numeric;
  v_reserve numeric;
  v_risk numeric;
  v_open_risk numeric;
  v_open_count integer;
  v_daily_pnl numeric;
  v_weekly_pnl numeric;
  v_drawdown numeric;
  v_realized numeric;
  v_order_id uuid;
  v_now timestamptz := pg_catalog.now();
begin
  if p_user_id is null or p_market not in ('crypto','stocks')
     or p_side not in ('buy','sell') or p_symbol is null
     or p_symbol !~ '^[A-Z0-9.-]{1,20}$'
     or p_quantity is null or p_quantity <= 0
     or p_price is null or p_price <= 0
     or p_idempotency_key is null or length(p_idempotency_key) < 16
     or length(p_idempotency_key) > 100 then
    raise exception 'INVALID_ORDER_INPUT' using errcode = '22023';
  end if;

  if (p_market = 'crypto' and p_symbol !~ '^[A-Z0-9]{2,16}USDT$')
     or (p_market = 'stocks' and p_symbol !~ '^[A-Z][A-Z0-9.-]{0,9}$') then
    raise exception 'SYMBOL_NOT_ALLOWED_FOR_MARKET' using errcode = '22023';
  end if;

  select * into v_portfolio
  from public.blessing_dual_portfolios
  where user_id = p_user_id and market = p_market
  for update;

  if not found then raise exception 'PAPER_PORTFOLIO_NOT_INITIALIZED'; end if;
  if v_portfolio.mode <> 'paper' or not v_portfolio.enabled then
    raise exception 'PAPER_PORTFOLIO_NOT_ENABLED';
  end if;

  select * into v_existing from public.blessing_dual_orders
  where user_id = p_user_id and idempotency_key = p_idempotency_key;
  if found then
    return pg_catalog.jsonb_build_object(
      'ok',true,'duplicate',true,'orderId',v_existing.id,
      'side',v_existing.side,'symbol',v_existing.symbol,
      'quantity',v_existing.quantity,'price',v_existing.price,
      'fee',v_existing.fee,'status',v_existing.status
    );
  end if;

  v_notional := p_quantity * p_price;
  v_fee := v_notional * 0.001; -- 0.10% paper fee

  select coalesce(sum(quantity * coalesce(current_price,entry_price)),0)
    into v_equity
    from public.blessing_dual_positions
    where portfolio_id=v_portfolio.id and status='open';
  v_equity := v_portfolio.cash_balance + v_equity;

  select coalesce(sum((entry_price-stop_loss_price)*quantity),0), count(*)
    into v_open_risk, v_open_count
    from public.blessing_dual_positions
    where portfolio_id=v_portfolio.id and status='open';

  if p_side='buy' then
    select coalesce(sum(realized_pnl),0) into v_daily_pnl
      from public.blessing_dual_orders
      where portfolio_id=v_portfolio.id and side='sell'
        and created_at >= pg_catalog.date_trunc('day',v_now);
    select coalesce(sum(realized_pnl),0) into v_weekly_pnl
      from public.blessing_dual_orders
      where portfolio_id=v_portfolio.id and side='sell'
        and created_at >= pg_catalog.date_trunc('week',v_now);

    v_drawdown := case when v_portfolio.peak_equity>0
      then greatest(0,(v_portfolio.peak_equity-v_equity)/v_portfolio.peak_equity*100)
      else 0 end;
    if v_daily_pnl <= -(v_portfolio.initial_capital*v_portfolio.max_daily_loss_pct/100) then
      raise exception 'DAILY_LOSS_LIMIT_REACHED';
    end if;
    if v_weekly_pnl <= -(v_portfolio.initial_capital*v_portfolio.max_weekly_loss_pct/100) then
      raise exception 'WEEKLY_LOSS_LIMIT_REACHED';
    end if;
    if v_drawdown >= v_portfolio.max_drawdown_pct then
      raise exception 'MAX_DRAWDOWN_REACHED';
    end if;

    if p_position_id is not null then raise exception 'BUY_MUST_NOT_REFERENCE_POSITION'; end if;
    if v_open_count >= v_portfolio.max_open_positions then raise exception 'MAX_OPEN_POSITIONS_REACHED'; end if;
    if exists(select 1 from public.blessing_dual_positions
              where portfolio_id=v_portfolio.id and symbol=p_symbol and status='open') then
      raise exception 'SYMBOL_ALREADY_OPEN';
    end if;
    if p_stop_loss_price is null or p_take_profit_price is null
       or p_stop_loss_price<=0 or p_stop_loss_price>=p_price
       or p_take_profit_price<=p_price then
      raise exception 'INVALID_STOP_OR_TARGET';
    end if;

    v_reserve := v_equity*v_portfolio.reserve_cash_pct/100;
    if v_notional+v_fee > greatest(0,v_portfolio.cash_balance-v_reserve) then
      raise exception 'INSUFFICIENT_DEPLOYABLE_CASH';
    end if;
    v_risk := (p_price-p_stop_loss_price)*p_quantity;
    if v_risk > v_equity*v_portfolio.risk_per_trade_pct/100 then
      raise exception 'RISK_PER_TRADE_LIMIT_EXCEEDED';
    end if;
    if v_open_risk+v_risk > v_equity*2/100 then
      raise exception 'AGGREGATE_OPEN_RISK_LIMIT_EXCEEDED';
    end if;

    insert into public.blessing_dual_positions
      (portfolio_id,user_id,market,symbol,side,quantity,entry_price,current_price,
       stop_loss_price,take_profit_price,entry_fee,unrealized_pnl,status)
    values
      (v_portfolio.id,p_user_id,p_market,p_symbol,'long',p_quantity,p_price,p_price,
       p_stop_loss_price,p_take_profit_price,v_fee,0,'open');

    update public.blessing_dual_portfolios
      set cash_balance=cash_balance-v_notional-v_fee,
          fees_paid=fees_paid+v_fee,updated_at=v_now
      where id=v_portfolio.id;

    insert into public.blessing_dual_orders
      (portfolio_id,user_id,market,symbol,side,quantity,price,notional,fee,status,reason,idempotency_key)
    values
      (v_portfolio.id,p_user_id,p_market,p_symbol,'buy',p_quantity,p_price,v_notional,
       v_fee,'filled','PAPER_MANUAL',p_idempotency_key)
    returning id into v_order_id;

    return pg_catalog.jsonb_build_object(
      'ok',true,'orderId',v_order_id,'side','buy','symbol',p_symbol,
      'quantity',p_quantity,'price',p_price,'notional',v_notional,'fee',v_fee,
      'status','filled','mode','paper'
    );
  else
    if p_position_id is null then raise exception 'SELL_REQUIRES_POSITION_ID'; end if;
    select * into v_position from public.blessing_dual_positions
      where id=p_position_id and portfolio_id=v_portfolio.id
        and user_id=p_user_id and status='open'
      for update;
    if not found then raise exception 'OPEN_POSITION_NOT_FOUND'; end if;
    if p_symbol<>v_position.symbol or p_quantity<>v_position.quantity then
      raise exception 'SELL_MUST_CLOSE_FULL_POSITION';
    end if;

    v_realized := (p_price-v_position.entry_price)*v_position.quantity
      -coalesce(v_position.entry_fee,0)-v_fee;

    update public.blessing_dual_positions
      set current_price=p_price,exit_fee=v_fee,realized_pnl=v_realized,
          unrealized_pnl=0,status='closed',closed_at=v_now,close_reason='MANUAL'
      where id=v_position.id;
    update public.blessing_dual_portfolios
      set cash_balance=cash_balance+v_notional-v_fee,
          realized_pnl=realized_pnl+v_realized,fees_paid=fees_paid+v_fee,
          updated_at=v_now
      where id=v_portfolio.id;
    insert into public.blessing_dual_orders
      (portfolio_id,user_id,market,symbol,side,quantity,price,notional,fee,status,reason,realized_pnl,idempotency_key)
    values
      (v_portfolio.id,p_user_id,p_market,p_symbol,'sell',p_quantity,p_price,v_notional,
       v_fee,'filled','PAPER_MANUAL',v_realized,p_idempotency_key)
    returning id into v_order_id;

    return pg_catalog.jsonb_build_object(
      'ok',true,'orderId',v_order_id,'side','sell','symbol',p_symbol,
      'quantity',p_quantity,'price',p_price,'notional',v_notional,'fee',v_fee,
      'realizedPnl',v_realized,'status','filled','mode','paper'
    );
  end if;
end;
$$;

create or replace function public.blessing_dual_mark_position(
  p_user_id uuid,p_market text,p_position_id uuid,p_price numeric
) returns jsonb
language plpgsql
security invoker
set search_path = ''
as $$
declare
  v_portfolio public.blessing_dual_portfolios%rowtype;
  v_position public.blessing_dual_positions%rowtype;
  v_reason text;
  v_equity numeric;
  v_result jsonb;
begin
  if p_price is null or p_price<=0 or p_market not in ('crypto','stocks') then
    raise exception 'INVALID_MARK_PRICE';
  end if;
  select * into v_portfolio from public.blessing_dual_portfolios
    where user_id=p_user_id and market=p_market for update;
  if not found or v_portfolio.mode<>'paper' then raise exception 'PAPER_PORTFOLIO_NOT_FOUND'; end if;
  select * into v_position from public.blessing_dual_positions
    where id=p_position_id and portfolio_id=v_portfolio.id
      and user_id=p_user_id and status='open'
    for update;
  if not found then return pg_catalog.jsonb_build_object('ok',true,'closed',false,'missing',true); end if;

  update public.blessing_dual_positions
    set current_price=p_price,
        unrealized_pnl=(p_price-entry_price)*quantity-coalesce(entry_fee,0)
    where id=v_position.id;
  select v_portfolio.cash_balance+coalesce(sum(quantity*coalesce(current_price,entry_price)),0)
    into v_equity from public.blessing_dual_positions
    where portfolio_id=v_portfolio.id and status='open';
  update public.blessing_dual_portfolios
    set peak_equity=greatest(coalesce(peak_equity,initial_capital),v_equity),
        updated_at=pg_catalog.now()
    where id=v_portfolio.id;

  if p_price<=v_position.stop_loss_price then v_reason:='STOP_LOSS';
  elsif p_price>=v_position.take_profit_price then v_reason:='TAKE_PROFIT';
  else return pg_catalog.jsonb_build_object('ok',true,'closed',false,'price',p_price);
  end if;

  v_result := public.blessing_dual_execute_order(
    p_user_id,p_market,v_position.symbol,'sell',v_position.quantity,p_price,
    null,null,v_position.id,'AUTO-EXIT-'||v_position.id::text
  );
  update public.blessing_dual_positions set close_reason=v_reason where id=v_position.id;
  return v_result || pg_catalog.jsonb_build_object('closeReason',v_reason,'closed',true);
end;
$$;

revoke all on function public.blessing_dual_execute_order(uuid,text,text,text,numeric,numeric,numeric,numeric,uuid,text) from public, anon, authenticated;
revoke all on function public.blessing_dual_mark_position(uuid,text,uuid,numeric) from public, anon, authenticated;
grant execute on function public.blessing_dual_execute_order(uuid,text,text,text,numeric,numeric,numeric,numeric,uuid,text) to service_role;
grant execute on function public.blessing_dual_mark_position(uuid,text,uuid,numeric) to service_role;
