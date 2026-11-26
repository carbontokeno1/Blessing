create or replace function public.blessing_dual_execute_order(
  p_user_id uuid, p_market text, p_symbol text, p_side text,
  p_quantity numeric, p_price numeric,
  p_stop_loss_price numeric default null,
  p_take_profit_price numeric default null,
  p_position_id uuid default null,
  p_idempotency_key text default null
) returns jsonb
language plpgsql security invoker set search_path = ''
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
  v_entry_notional numeric;
  v_now timestamptz := pg_catalog.now();
begin
  if p_user_id is null or p_market not in ('crypto','stocks')
     or p_side not in ('buy','sell','short','cover') or p_symbol is null
     or p_symbol !~ '^[A-Z0-9.-]{1,20}$'
     or p_quantity is null or p_quantity <= 0
     or p_price is null or p_price <= 0
     or p_idempotency_key is null or length(p_idempotency_key) < 16
     or length(p_idempotency_key) > 100 then
    raise exception 'INVALID_ORDER_INPUT' using errcode='22023';
  end if;
  if (p_market='crypto' and p_symbol !~ '^[A-Z0-9]{2,16}USDT$')
     or (p_market='stocks' and p_symbol !~ '^[A-Z][A-Z0-9.-]{0,9}$') then
    raise exception 'SYMBOL_NOT_ALLOWED_FOR_MARKET' using errcode='22023';
  end if;
  select * into v_portfolio from public.blessing_dual_portfolios
  where user_id=p_user_id and market=p_market for update;
  if not found or v_portfolio.mode<>'paper' then
    raise exception 'PAPER_PORTFOLIO_NOT_INITIALIZED';
  end if;

  select * into v_existing from public.blessing_dual_orders
  where user_id=p_user_id and idempotency_key=p_idempotency_key;
  if found then
    return pg_catalog.jsonb_build_object('ok',true,'duplicate',true,
      'orderId',v_existing.id,'side',v_existing.side,'symbol',v_existing.symbol,
      'quantity',v_existing.quantity,'price',v_existing.price,'fee',v_existing.fee,
      'status',v_existing.status);
  end if;

  v_notional := p_quantity*p_price;
  v_fee := v_notional*0.001; -- 0.10% per fill

  if p_side in ('buy','short') then
    if not v_portfolio.enabled then raise exception 'PAPER_PORTFOLIO_NOT_ENABLED'; end if;

    select coalesce(sum(
      case when side='short'
        then entry_price*quantity + coalesce(unrealized_pnl,0) + coalesce(entry_fee,0)
        else quantity*coalesce(current_price,entry_price)
      end),0)
    into v_equity
    from public.blessing_dual_positions
    where portfolio_id=v_portfolio.id and status='open';
    v_equity := v_portfolio.cash_balance + v_equity;

    select coalesce(sum(abs(entry_price-stop_loss_price)*quantity),0),count(*)
      into v_open_risk,v_open_count
    from public.blessing_dual_positions
    where portfolio_id=v_portfolio.id and status='open';

    select coalesce(sum(realized_pnl),0) into v_daily_pnl
    from public.blessing_dual_orders
    where portfolio_id=v_portfolio.id and realized_pnl is not null
      and created_at >= (v_now at time zone 'Asia/Bangkok')::date::timestamp
                         at time zone 'Asia/Bangkok';
    select coalesce(sum(realized_pnl),0) into v_weekly_pnl
    from public.blessing_dual_orders
    where portfolio_id=v_portfolio.id and realized_pnl is not null
      and created_at >= date_trunc('week',v_now at time zone 'Asia/Bangkok')
                         at time zone 'Asia/Bangkok';
    v_drawdown := case when v_portfolio.peak_equity>0
      then greatest(0,(v_portfolio.peak_equity-v_equity)/v_portfolio.peak_equity*100)
      else 0 end;

    if v_daily_pnl <= -(v_portfolio.initial_capital*v_portfolio.max_daily_loss_pct/100)
      then raise exception 'DAILY_LOSS_LIMIT_REACHED'; end if;
    if v_weekly_pnl <= -(v_portfolio.initial_capital*v_portfolio.max_weekly_loss_pct/100)
      then raise exception 'WEEKLY_LOSS_LIMIT_REACHED'; end if;
    if v_drawdown >= v_portfolio.max_drawdown_pct
      then raise exception 'MAX_DRAWDOWN_REACHED'; end if;
    if p_position_id is not null then raise exception 'OPEN_MUST_NOT_REFERENCE_POSITION'; end if;
    if v_open_count >= v_portfolio.max_open_positions then raise exception 'MAX_OPEN_POSITIONS_REACHED'; end if;
    if exists(select 1 from public.blessing_dual_positions
      where portfolio_id=v_portfolio.id and symbol=p_symbol and status='open')
      then raise exception 'SYMBOL_ALREADY_OPEN'; end if;

    if p_side='buy' then
      if p_stop_loss_price is null or p_take_profit_price is null
        or p_stop_loss_price<=0 or p_stop_loss_price>=p_price
        or p_take_profit_price<=p_price then raise exception 'INVALID_STOP_OR_TARGET'; end if;
    else
      if p_market<>'crypto' then raise exception 'SHORT_CRYPTO_ONLY'; end if;
      if p_stop_loss_price is null or p_take_profit_price is null
        or p_stop_loss_price<=p_price or p_take_profit_price<=0
        or p_take_profit_price>=p_price then raise exception 'INVALID_SHORT_STOP_OR_TARGET'; end if;
    end if;

    v_reserve := v_equity*v_portfolio.reserve_cash_pct/100;
    if v_notional+v_fee > greatest(0,v_portfolio.cash_balance-v_reserve)
      then raise exception 'INSUFFICIENT_DEPLOYABLE_CASH'; end if;
    v_risk := abs(p_price-p_stop_loss_price)*p_quantity;
    if v_risk > v_equity*v_portfolio.risk_per_trade_pct/100
      then raise exception 'RISK_PER_TRADE_LIMIT_EXCEEDED'; end if;
    if v_open_risk+v_risk > v_equity*1.5/100
      then raise exception 'AGGREGATE_OPEN_RISK_LIMIT_EXCEEDED'; end if;

    insert into public.blessing_dual_positions
      (portfolio_id,user_id,market,symbol,side,quantity,entry_price,current_price,
       best_price,stop_loss_price,take_profit_price,entry_fee,unrealized_pnl,status)
    values
      (v_portfolio.id,p_user_id,p_market,p_symbol,
       case when p_side='buy' then 'long' else 'short' end,
       p_quantity,p_price,p_price,p_price,p_stop_loss_price,p_take_profit_price,
       v_fee,0,'open');

    -- Both long notional and short collateral are ring-fenced from available cash.
    update public.blessing_dual_portfolios
      set cash_balance=cash_balance-v_notional-v_fee,
          fees_paid=fees_paid+v_fee,updated_at=v_now
      where id=v_portfolio.id;

    insert into public.blessing_dual_orders
      (portfolio_id,user_id,market,symbol,side,quantity,price,notional,fee,status,reason,idempotency_key)
    values
      (v_portfolio.id,p_user_id,p_market,p_symbol,p_side,p_quantity,p_price,
       v_notional,v_fee,'filled','ADAPTIVE_PAPER',p_idempotency_key)
    returning id into v_order_id;

    return pg_catalog.jsonb_build_object('ok',true,'orderId',v_order_id,
      'side',p_side,'positionSide',case when p_side='buy' then 'long' else 'short' end,
      'symbol',p_symbol,'quantity',p_quantity,'price',p_price,'notional',v_notional,
      'fee',v_fee,'status','filled','mode','paper','liveOrdersEnabled',false);
  end if;

  -- Close an existing position. Closing remains allowed after the member presses STOP.
  if p_position_id is null then raise exception 'CLOSE_REQUIRES_POSITION_ID'; end if;
  select * into v_position from public.blessing_dual_positions
  where id=p_position_id and portfolio_id=v_portfolio.id
    and user_id=p_user_id and status='open' for update;
  if not found then raise exception 'OPEN_POSITION_NOT_FOUND'; end if;
  if p_symbol<>v_position.symbol or p_quantity<>v_position.quantity
    then raise exception 'CLOSE_MUST_CLOSE_FULL_POSITION'; end if;
  if (p_side='sell' and v_position.side<>'long')
     or (p_side='cover' and v_position.side<>'short')
     then raise exception 'CLOSE_SIDE_MISMATCH'; end if;

  v_entry_notional := v_position.entry_price*v_position.quantity;
  if v_position.side='long' then
    v_realized := (p_price-v_position.entry_price)*v_position.quantity
      -coalesce(v_position.entry_fee,0)-v_fee;
  else
    v_realized := (v_position.entry_price-p_price)*v_position.quantity
      -coalesce(v_position.entry_fee,0)-v_fee;
  end if;

  update public.blessing_dual_positions
    set current_price=p_price,exit_fee=v_fee,realized_pnl=v_realized,
        unrealized_pnl=0,status='closed',closed_at=v_now,close_reason='MANUAL'
    where id=v_position.id;
  if v_position.side='long' then
    update public.blessing_dual_portfolios
      set cash_balance=cash_balance+v_notional-v_fee,
          realized_pnl=realized_pnl+v_realized,fees_paid=fees_paid+v_fee,updated_at=v_now
      where id=v_portfolio.id;
  else
    update public.blessing_dual_portfolios
      set cash_balance=cash_balance+v_entry_notional
          +(v_position.entry_price-p_price)*v_position.quantity-v_fee,
          realized_pnl=realized_pnl+v_realized,fees_paid=fees_paid+v_fee,updated_at=v_now
      where id=v_portfolio.id;
  end if;

  insert into public.blessing_dual_orders
    (portfolio_id,user_id,market,symbol,side,quantity,price,notional,fee,status,reason,realized_pnl,idempotency_key)
  values
    (v_portfolio.id,p_user_id,p_market,p_symbol,p_side,p_quantity,p_price,
     v_notional,v_fee,'filled','ADAPTIVE_PAPER_CLOSE',v_realized,p_idempotency_key)
  returning id into v_order_id;
  return pg_catalog.jsonb_build_object('ok',true,'orderId',v_order_id,
    'side',p_side,'positionSide',v_position.side,'symbol',p_symbol,
    'quantity',p_quantity,'price',p_price,'notional',v_notional,'fee',v_fee,
    'realizedPnl',v_realized,'status','filled','mode','paper','liveOrdersEnabled',false);
end;
$$;

create or replace function public.blessing_dual_mark_position(
  p_user_id uuid,p_market text,p_position_id uuid,p_price numeric
) returns jsonb
language plpgsql security invoker set search_path = ''
as $$
declare
  v_portfolio public.blessing_dual_portfolios%rowtype;
  v_position public.blessing_dual_positions%rowtype;
  v_reason text;
  v_result jsonb;
  v_new_best numeric;
  v_new_stop numeric;
  v_risk_unit numeric;
begin
  if p_price is null or p_price<=0 or p_market not in ('crypto','stocks')
    then raise exception 'INVALID_MARK_PRICE'; end if;
  select * into v_portfolio from public.blessing_dual_portfolios
  where user_id=p_user_id and market=p_market for update;
  if not found or v_portfolio.mode<>'paper' then raise exception 'PAPER_PORTFOLIO_NOT_FOUND'; end if;
  select * into v_position from public.blessing_dual_positions
  where id=p_position_id and portfolio_id=v_portfolio.id
    and user_id=p_user_id and status='open' for update;
  if not found then return pg_catalog.jsonb_build_object('ok',true,'closed',false,'missing',true); end if;

  if v_position.side='long' then
    v_new_best := greatest(coalesce(v_position.best_price,v_position.entry_price),p_price);
    v_new_stop := v_position.stop_loss_price;
    v_risk_unit := v_position.entry_price-v_position.stop_loss_price;
    if v_new_best >= v_position.entry_price+1.5*v_risk_unit then
      v_new_stop := greatest(v_new_stop,v_new_best-v_risk_unit);
    end if;
    update public.blessing_dual_positions set current_price=p_price,best_price=v_new_best,
      stop_loss_price=v_new_stop,
      unrealized_pnl=(p_price-entry_price)*quantity-coalesce(entry_fee,0)
      where id=v_position.id;
    if p_price<=v_new_stop then v_reason:='STOP_LOSS_OR_TRAILING';
    elsif p_price>=v_position.take_profit_price then v_reason:='TAKE_PROFIT'; end if;
  else
    v_new_best := least(coalesce(v_position.best_price,v_position.entry_price),p_price);
    v_new_stop := v_position.stop_loss_price;
    v_risk_unit := v_position.stop_loss_price-v_position.entry_price;
    if v_new_best <= v_position.entry_price-1.5*v_risk_unit then
      v_new_stop := least(v_new_stop,v_new_best+v_risk_unit);
    end if;
    update public.blessing_dual_positions set current_price=p_price,best_price=v_new_best,
      stop_loss_price=v_new_stop,
      unrealized_pnl=(entry_price-p_price)*quantity-coalesce(entry_fee,0)
      where id=v_position.id;
    if p_price>=v_new_stop then v_reason:='STOP_LOSS_OR_TRAILING';
    elsif p_price<=v_position.take_profit_price then v_reason:='TAKE_PROFIT'; end if;
  end if;

  if v_reason is null then
    return pg_catalog.jsonb_build_object('ok',true,'closed',false,'price',p_price,
      'side',v_position.side,'trailingStop',v_new_stop);
  end if;

  v_result := public.blessing_dual_execute_order(
    p_user_id,p_market,v_position.symbol,
    case when v_position.side='long' then 'sell' else 'cover' end,
    v_position.quantity,p_price,null,null,v_position.id,
    'AUTO-EXIT-'||v_position.id::text||'-'||replace(v_reason,' ','-')
  );
  update public.blessing_dual_positions set close_reason=v_reason where id=v_position.id;
  return v_result || pg_catalog.jsonb_build_object('closeReason',v_reason,'closed',true);
end;
$$;

revoke all on function public.blessing_dual_execute_order(uuid,text,text,text,numeric,numeric,numeric,numeric,uuid,text) from public,anon,authenticated;
revoke all on function public.blessing_dual_mark_position(uuid,text,uuid,numeric) from public,anon,authenticated;
grant execute on function public.blessing_dual_execute_order(uuid,text,text,text,numeric,numeric,numeric,numeric,uuid,text) to service_role;
grant execute on function public.blessing_dual_mark_position(uuid,text,uuid,numeric) to service_role;
