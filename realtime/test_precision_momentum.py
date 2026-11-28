import unittest
from unittest.mock import patch

import app as blessing_app


class BlessingPrecisionMomentumTests(unittest.TestCase):
    def setUp(self):
        self.row = {
            "symbol": "BTCUSDT",
            "exchangeSymbol": "BTCUSDT",
            "source": "Binance TH Spot API",
            "price": 101.0,
            "ema20": 100.5,
            "ema50": 99.5,
            "m1": 0.05,
            "m5": 0.45,
            "m15": 0.80,
            "m60": 1.20,
            "rsi14": 58,
            "rvol": 1.8,
            "spreadPct": 0.08,
            "atrStopPct": 1.1,
            "candleAgeSeconds": 20,
            "trendAligned": True,
            "pullbackReclaim": True,
            "breakoutRetest": False,
            "btcMarketBias": "neutral",
        }

    def test_qualified_trend_pullback_is_paper_only(self):
        result = blessing_app._blessing_precision_momentum_decision(self.row)
        self.assertIsNotNone(result)
        self.assertEqual(result["side"], "long")
        self.assertEqual(result["strategy"], "BLESSING_PRECISION_MOMENTUM")
        self.assertEqual(result["liveEntryScore"], 0)
        self.assertEqual(result["liveEntryModel"], "PAPER_ONLY_BLESSING_PRECISION_MOMENTUM")

    def test_bearish_btc_bias_blocks_altcoin_entries(self):
        self.row["btcMarketBias"] = "bearish"
        self.assertIsNone(blessing_app._blessing_precision_momentum_decision(self.row))

    def test_stale_candle_blocks_entry(self):
        self.row["candleAgeSeconds"] = 91
        self.assertIsNone(blessing_app._blessing_precision_momentum_decision(self.row))

    def test_wide_spread_blocks_entry(self):
        self.row["spreadPct"] = 0.21
        self.assertIsNone(blessing_app._blessing_precision_momentum_decision(self.row))

    def test_non_binance_fallback_feed_blocks_entry(self):
        self.row["source"] = "OKX"
        self.assertIsNone(blessing_app._blessing_precision_momentum_decision(self.row))


    @patch.object(blessing_app, "_dual_market_price", return_value=101.0)
    @patch.object(blessing_app, "_dual_rpc_order")
    @patch.object(blessing_app, "_sb_rest")
    def test_daily_profit_target_pauses_new_crypto_entries(self, sb_rest, rpc, quote):
        portfolio = {
            "id": "22222222-2222-4222-8222-222222222222",
            "user_id": "11111111-1111-4111-8111-111111111111",
            "cash_balance": 100.0,
            "initial_capital": 100.0,
            "max_daily_loss_pct": 2.0,
            "reserve_cash_pct": 20.0,
            "risk_per_trade_pct": 0.5,
            "max_open_positions": 3,
        }
        # No open positions; today's realized PnL is already above the 0.5% target.
        sb_rest.side_effect = [
            [],
            [{"realized_pnl": 0.60}],
        ]
        result = blessing_app._dual_try_adaptive_entry(
            {"id": "AUTO-CRYPTO", "config": {"crypto_daily_profit_target_pct": 0.5}},
            portfolio, {"symbol": "ETHUSDT", "atrStopPct": 1.0}, "crypto"
        )
        self.assertIsNone(result)
        rpc.assert_not_called()
        self.assertEqual(blessing_app.DUAL_PAPER_WORKER_STATE["dailyProfitGuard"]["status"], "target_reached")


if __name__ == "__main__":
    unittest.main()
