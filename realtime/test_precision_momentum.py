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
        # No open positions; today's realized net PnL is above the 3% target.
        sb_rest.side_effect = [
            [],
            [{"realized_pnl": 3.10}],
        ]
        result = blessing_app._dual_try_adaptive_entry(
            {"id": "AUTO-CRYPTO", "config": {"crypto_daily_profit_target_pct": 3.0}},
            portfolio, {"symbol": "ETHUSDT", "atrStopPct": 1.0}, "crypto"
        )
        self.assertIsNone(result)
        rpc.assert_not_called()
        self.assertEqual(blessing_app.DUAL_PAPER_WORKER_STATE["dailyProfitGuard"]["status"], "target_reached")



class BlessingFastCryptoPaperTests(unittest.TestCase):
    def setUp(self):
        self.row = {
            "symbol": "SOLUSDT",
            "exchangeSymbol": "SOLUSDT",
            "source": "Binance TH Spot API",
            "price": 150.0,
            "quoteVolume24h": 1000000.0,
            "m1": 0.18,
            "m5": 0.72,
            "m15": 1.1,
            "m60": 2.0,
            "rvol": 2.4,
            "spreadPct": 0.25,
            "atrStopPct": 1.4,
            "candleAgeSeconds": 18,
            "score": 75,
            "breakout": True,
            "fastEntry": True,
        }

    def test_fast_breakout_is_paper_only(self):
        result = blessing_app._blessing_fast_momentum_decision(self.row)
        self.assertIsNotNone(result)
        self.assertEqual(result["strategy"], "BLESSING_FAST_MOMENTUM")
        self.assertEqual(result["strategySetup"], "VOLUME_BREAKOUT")
        self.assertEqual(result["side"], "long")
        self.assertEqual(result["liveEntryScore"], 0)
        self.assertEqual(result["liveEntryModel"], "PAPER_ONLY_BLESSING_FAST_MOMENTUM")

    def test_fast_rejects_low_liquidity_even_when_discovered(self):
        self.row["quoteVolume24h"] = 49999.0
        self.assertIsNone(blessing_app._blessing_fast_momentum_decision(self.row))

    def test_fast_requires_scanner_breakout_signal(self):
        self.row["breakout"] = False
        self.assertIsNone(blessing_app._blessing_fast_momentum_decision(self.row))

    def test_fast_requires_relative_volume_at_least_two(self):
        self.row["rvol"] = 1.99
        self.assertIsNone(blessing_app._blessing_fast_momentum_decision(self.row))

    def test_fast_rejects_stale_candles(self):
        self.row["candleAgeSeconds"] = 91
        self.assertIsNone(blessing_app._blessing_fast_momentum_decision(self.row))

    def test_fast_rejects_spread_above_half_percent(self):
        self.row["spreadPct"] = 0.51
        self.assertIsNone(blessing_app._blessing_fast_momentum_decision(self.row))

    def test_fast_rejects_non_binance_feed(self):
        self.row["source"] = "Fallback feed"
        self.assertIsNone(blessing_app._blessing_fast_momentum_decision(self.row))


class BlessingCryptoOnlyModeTests(unittest.TestCase):
    def test_dual_paper_stock_new_entries_remain_paused(self):
        self.assertFalse(blessing_app.DUAL_PAPER_STOCK_ENTRIES_ENABLED)


if __name__ == "__main__":
    unittest.main()
