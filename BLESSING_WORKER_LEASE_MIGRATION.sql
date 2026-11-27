-- BLESSING: database-backed leases for singleton background workers.
-- Applied to Supabase project dyzumqcqvnznlkugimxo on 2026-11-27.

create table if not exists public.blessing_worker_leases (
  worker_key text primary key,
  owner_id uuid not null,
  expires_at timestamptz not null,
  updated_at timestamptz not null default clock_timestamp()
);
alter table public.blessing_worker_leases enable row level security;
revoke all on table public.blessing_worker_leases from anon, authenticated;
grant select, insert, update, delete on table public.blessing_worker_leases to service_role;

create or replace function public.blessing_worker_lease_acquire(
  p_worker_key text, p_owner_id uuid, p_lease_seconds integer default 240
) returns boolean
language plpgsql security definer
set search_path = public, pg_temp
as $$
declare v_acquired boolean := false;
begin
  if p_worker_key is null or length(trim(p_worker_key)) = 0 or length(p_worker_key) > 100 then
    raise exception 'INVALID_WORKER_KEY';
  end if;
  if p_owner_id is null then raise exception 'INVALID_WORKER_OWNER'; end if;
  insert into public.blessing_worker_leases(worker_key, owner_id, expires_at, updated_at)
  values (p_worker_key, p_owner_id,
    clock_timestamp() + make_interval(secs => least(greatest(coalesce(p_lease_seconds,240),30),600)),
    clock_timestamp())
  on conflict (worker_key) do update
    set owner_id = excluded.owner_id,
        expires_at = excluded.expires_at,
        updated_at = clock_timestamp()
    where public.blessing_worker_leases.expires_at <= clock_timestamp()
       or public.blessing_worker_leases.owner_id = excluded.owner_id
  returning true into v_acquired;
  return coalesce(v_acquired,false);
end;
$$;

create or replace function public.blessing_worker_lease_renew(
  p_worker_key text, p_owner_id uuid, p_lease_seconds integer default 240
) returns boolean
language plpgsql security definer
set search_path = public, pg_temp
as $$
declare v_renewed boolean := false;
begin
  update public.blessing_worker_leases
     set expires_at = clock_timestamp() + make_interval(secs => least(greatest(coalesce(p_lease_seconds,240),30),600)),
         updated_at = clock_timestamp()
   where worker_key = p_worker_key and owner_id = p_owner_id
     and expires_at > clock_timestamp()
  returning true into v_renewed;
  return coalesce(v_renewed,false);
end;
$$;

create or replace function public.blessing_worker_lease_release(
  p_worker_key text, p_owner_id uuid
) returns boolean
language plpgsql security definer
set search_path = public, pg_temp
as $$
declare v_released boolean := false;
begin
  delete from public.blessing_worker_leases
   where worker_key = p_worker_key and owner_id = p_owner_id
  returning true into v_released;
  return coalesce(v_released,false);
end;
$$;

revoke all on function public.blessing_worker_lease_acquire(text,uuid,integer) from public, anon, authenticated;
revoke all on function public.blessing_worker_lease_renew(text,uuid,integer) from public, anon, authenticated;
revoke all on function public.blessing_worker_lease_release(text,uuid) from public, anon, authenticated;
grant execute on function public.blessing_worker_lease_acquire(text,uuid,integer) to service_role;
grant execute on function public.blessing_worker_lease_renew(text,uuid,integer) to service_role;
grant execute on function public.blessing_worker_lease_release(text,uuid) to service_role;

create index if not exists blessing_worker_leases_expires_at_idx
  on public.blessing_worker_leases(expires_at);
