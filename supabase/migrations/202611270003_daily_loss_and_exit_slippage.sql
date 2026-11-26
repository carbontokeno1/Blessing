-- Include open mark-to-market losses in daily/weekly circuit breakers and model exit slippage.
do $$
declare v_def text; v_old text; v_new text;
begin
  v_def := pg_get_functiondef('public.blessing_dual_execute_order(uuid,text,text,text,numeric,numeric,numeric,numeric,uuid,text)'::regprocedure);
  v_old := $old$    select coalesce(sum(realized_pnl),0) into v_daily_pnl
    from public.blessing_dual_orders
    where portfolio_id=v_portfolio.id and realized_pnl is not null
      and created_at >= (v_now at time zone 'Asia/Bangkok')::date::timestamp
                         at time zone 'Asia/Bangkok';$old$;
  v_new := v_old || E'\n    select v_daily_pnl+coalesce(sum(unrealized_pnl),0) into v_daily_pnl\n    from public.blessing_dual_positions where portfolio_id=v_portfolio.id and status=\'open\';';
  if position(v_old in v_def)=0 then raise exception 'DAILY_PNL_PATCH_TARGET_NOT_FOUND'; end if;
  v_def := replace(v_def,v_old,v_new);
  v_old := $old$    select coalesce(sum(realized_pnl),0) into v_weekly_pnl
    from public.blessing_dual_orders
    where portfolio_id=v_portfolio.id and realized_pnl is not null
      and created_at >= date_trunc('week',v_now at time zone 'Asia/Bangkok')
                         at time zone 'Asia/Bangkok';$old$;
  v_new := v_old || E'\n    select v_weekly_pnl+coalesce(sum(unrealized_pnl),0) into v_weekly_pnl\n    from public.blessing_dual_positions where portfolio_id=v_portfolio.id and status=\'open\';';
  if position(v_old in v_def)=0 then raise exception 'WEEKLY_PNL_PATCH_TARGET_NOT_FOUND'; end if;
  v_def := replace(v_def,v_old,v_new);
  execute v_def;
  v_def := pg_get_functiondef('public.blessing_dual_mark_position(uuid,text,uuid,numeric)'::regprocedure);
  v_old := 'v_position.quantity,p_price,null,null,v_position.id,';
  v_new := 'v_position.quantity,case when v_position.side=''long'' then p_price*0.9995 else p_price*1.0005 end,null,null,v_position.id,';
  if position(v_old in v_def)=0 then raise exception 'EXIT_SLIPPAGE_PATCH_TARGET_NOT_FOUND'; end if;
  v_def := replace(v_def,v_old,v_new);
  execute v_def;
end $$;
