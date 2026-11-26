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
  v_equity numeric;
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

  -- Refresh portfolio equity peak after every mark, including open Short collateral.
  select v_portfolio.cash_balance + coalesce(sum(
    case when side='short'
      then entry_price*quantity + coalesce(unrealized_pnl,0)
      else quantity*coalesce(current_price,entry_price)
    end),0)
  into v_equity
  from public.blessing_dual_positions
  where portfolio_id=v_portfolio.id and status='open';
  update public.blessing_dual_portfolios
    set peak_equity=greatest(coalesce(peak_equity,initial_capital),v_equity),
        updated_at=pg_catalog.now()
    where id=v_portfolio.id;

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
