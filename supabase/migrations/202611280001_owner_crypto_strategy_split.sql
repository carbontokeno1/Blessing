-- Split the BLESSING owner's existing Crypto Paper portfolio into two isolated strategy portfolios.
-- The FAST portfolio is created disabled; enable it only after the backend understands strategy_key.
ALTER TABLE public.blessing_dual_portfolios
  ADD COLUMN IF NOT EXISTS strategy_key text NOT NULL DEFAULT 'PRECISION',
  ADD COLUMN IF NOT EXISTS strategy_started_at timestamptz NOT NULL DEFAULT now();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'blessing_dual_portfolios_strategy_key_check'
      AND conrelid = 'public.blessing_dual_portfolios'::regclass
  ) THEN
    ALTER TABLE public.blessing_dual_portfolios
      ADD CONSTRAINT blessing_dual_portfolios_strategy_key_check
      CHECK (strategy_key IN ('FAST', 'PRECISION'));
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS blessing_dual_crypto_strategy_unique
  ON public.blessing_dual_portfolios (user_id, market, mode, strategy_key)
  WHERE market = 'crypto' AND mode = 'paper';

DO $$
DECLARE
  v_owner uuid := '62835b65-13eb-4d99-9647-c4b22bd61ef3';
  v_portfolio public.blessing_dual_portfolios%ROWTYPE;
BEGIN
  SELECT * INTO v_portfolio
  FROM public.blessing_dual_portfolios
  WHERE user_id = v_owner AND market = 'crypto' AND mode = 'paper'
    AND strategy_key = 'PRECISION'
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'OWNER_CRYPTO_PRECISION_PORTFOLIO_NOT_FOUND';
  END IF;

  IF EXISTS (
    SELECT 1 FROM public.blessing_dual_positions
    WHERE portfolio_id = v_portfolio.id AND status = 'open'
  ) THEN
    RAISE EXCEPTION 'OWNER_CRYPTO_OPEN_POSITIONS_MUST_BE_CLOSED_OR_MIGRATED_FIRST';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM public.blessing_dual_portfolios
    WHERE user_id = v_owner AND market = 'crypto' AND mode = 'paper'
      AND strategy_key = 'FAST'
  ) THEN
    UPDATE public.blessing_dual_portfolios
    SET initial_capital = v_portfolio.cash_balance / 2,
        cash_balance = v_portfolio.cash_balance / 2,
        realized_pnl = 0,
        fees_paid = 0,
        peak_equity = v_portfolio.cash_balance / 2,
        strategy_key = 'PRECISION',
        strategy_started_at = now(),
        updated_at = now()
    WHERE id = v_portfolio.id;

    INSERT INTO public.blessing_dual_portfolios (
      user_id, market, broker, currency, initial_capital, cash_balance,
      realized_pnl, fees_paid, mode, enabled, reserve_cash_pct,
      risk_per_trade_pct, max_daily_loss_pct, max_weekly_loss_pct,
      max_drawdown_pct, max_open_positions, peak_equity, strategy_key
    ) VALUES (
      v_owner, 'crypto', 'binance_th', 'USDT',
      v_portfolio.cash_balance / 2, v_portfolio.cash_balance / 2,
      0, 0, 'paper', false, 20,
      0.5, 2, 6, 5, 3,
      v_portfolio.cash_balance / 2,
      'FAST'
    );
  END IF;
END $$;
