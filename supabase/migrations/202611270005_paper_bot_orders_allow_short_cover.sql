alter table public.paper_bot_orders drop constraint if exists paper_bot_orders_side_check;
alter table public.paper_bot_orders add constraint paper_bot_orders_side_check check (side in ('buy','sell','short','cover'));
