-- BLESSING member onboarding and owner-controlled paper allocations
-- Applied to Supabase project dyzumqcqvnznlkugimxo on 2026-11-23.

create or replace function private.handle_new_blessing_user()
returns trigger language plpgsql security definer set search_path = ''
as $$
begin
  insert into public.profiles (user_id,email,display_name,role,status)
  values (new.id,new.email,nullif(new.raw_user_meta_data->>'display_name',''),'member','pending')
  on conflict (user_id) do nothing;
  insert into public.blessing_members (user_id,display_name,status)
  values (new.id,nullif(new.raw_user_meta_data->>'display_name',''),'pending')
  on conflict (user_id) do nothing;
  return new;
end;
$$;
drop trigger if exists on_auth_user_created_blessing on auth.users;
create trigger on_auth_user_created_blessing after insert on auth.users
for each row execute function private.handle_new_blessing_user();

create or replace function public.blessing_admin_set_member_status(p_user_id uuid,p_status text)
returns jsonb language plpgsql security definer set search_path = ''
as $$
declare v_role text;
begin
  if auth.uid() is null or not private.has_admin_role() then raise exception 'admin_only' using errcode='42501'; end if;
  if p_status not in ('active','suspended','pending') then raise exception 'invalid_member_status' using errcode='22023'; end if;
  select role into v_role from public.profiles where user_id=p_user_id;
  if v_role is distinct from 'member' then raise exception 'member_account_not_found' using errcode='P0002'; end if;
  update public.profiles set status=p_status,updated_at=now() where user_id=p_user_id;
  insert into public.blessing_members(user_id,status,approved_by,approved_at)
  values(p_user_id,p_status,case when p_status='active' then auth.uid() else null end,case when p_status='active' then now() else null end)
  on conflict(user_id) do update set status=excluded.status,
    approved_by=case when excluded.status='active' then auth.uid() else public.blessing_members.approved_by end,
    approved_at=case when excluded.status='active' then now() else public.blessing_members.approved_at end,updated_at=now();
  insert into public.audit_logs(actor_user_id,target_user_id,action,details)
  values(auth.uid(),p_user_id,'member_status_changed',jsonb_build_object('status',p_status));
  return jsonb_build_object('ok',true,'user_id',p_user_id,'status',p_status);
end;
$$;

create or replace function public.blessing_admin_set_allocation(p_user_id uuid,p_market text,p_amount numeric,p_enabled boolean default false)
returns jsonb language plpgsql security definer set search_path = ''
as $$
declare v_currency text; v_broker text; v_old numeric:=0; v_delta numeric:=0; v_id uuid; v_status text;
begin
  if auth.uid() is null or not private.has_admin_role() then raise exception 'admin_only' using errcode='42501'; end if;
  if p_market not in ('crypto','stocks') or p_amount is null or p_amount<0 then raise exception 'invalid_allocation' using errcode='22023'; end if;
  select status into v_status from public.profiles where user_id=p_user_id and role='member';
  if v_status is null then raise exception 'member_account_not_found' using errcode='P0002'; end if;
  if v_status<>'active' then raise exception 'member_must_be_approved_first' using errcode='22023'; end if;
  v_currency:=case when p_market='crypto' then 'USDT' else 'USD' end;
  v_broker:=case when p_market='crypto' then 'binance' else 'webull' end;
  select id,allocated_capital into v_id,v_old from public.blessing_member_allocations where user_id=p_user_id and market=p_market for update;
  v_old:=coalesce(v_old,0);v_delta:=p_amount-v_old;
  insert into public.blessing_member_allocations(user_id,market,broker,currency,allocated_capital,mode,enabled,created_by,updated_at)
  values(p_user_id,p_market,v_broker,v_currency,p_amount,'paper',p_enabled,auth.uid(),now())
  on conflict(user_id,market) do update set allocated_capital=excluded.allocated_capital,broker=excluded.broker,currency=excluded.currency,mode='paper',enabled=excluded.enabled,created_by=auth.uid(),updated_at=now()
  returning id into v_id;
  if v_delta<>0 then
    insert into public.blessing_member_ledger(user_id,allocation_id,market,currency,entry_type,amount,note,created_by)
    values(p_user_id,v_id,p_market,v_currency,case when v_delta>0 then 'allocation' else 'release' end,v_delta,'Admin allocation adjustment; paper allocation only',auth.uid());
  end if;
  insert into public.audit_logs(actor_user_id,target_user_id,action,details)
  values(auth.uid(),p_user_id,'member_allocation_updated',jsonb_build_object('market',p_market,'old_amount',v_old,'new_amount',p_amount,'mode','paper','enabled',p_enabled));
  return jsonb_build_object('ok',true,'allocation_id',v_id,'market',p_market,'currency',v_currency,'allocated_capital',p_amount,'delta',v_delta,'mode','paper','enabled',p_enabled);
end;
$$;
revoke all on function public.blessing_admin_set_member_status(uuid,text) from public,anon;
revoke all on function public.blessing_admin_set_allocation(uuid,text,numeric,boolean) from public,anon;
grant execute on function public.blessing_admin_set_member_status(uuid,text) to authenticated;
grant execute on function public.blessing_admin_set_allocation(uuid,text,numeric,boolean) to authenticated;
