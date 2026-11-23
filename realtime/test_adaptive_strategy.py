import unittest
from datetime import datetime, timezone

from adaptive_strategy import adaptive_decision, build_binance_th_candidates, classify_regime, experiment_window, performance_summary


class AdaptiveStrategyTests(unittest.TestCase):
    def test_uptrend_selects_trend_momentum(self):
        result = classify_regime("crypto", {
            "m1": 0.2, "m5": 0.8, "m15": 1.2, "m60": 2.1,
            "rvol": 2.0, "spreadPct": 0.1, "breakout": True,
        })
        self.assertEqual(result["regime"], "uptrend_breakout")
        self.assertTrue(result["allowEntry"])

    def test_downtrend_and_high_volatility_block_entries(self):
        down = classify_regime("crypto", {"m1": -0.2, "m5": -0.5, "m15": -1, "m60": -2, "rvol": 2, "spreadPct": .1})
        volatile = classify_regime("crypto", {"m1": 2, "m5": 3, "m15": 4, "m60": 5, "rvol": 3, "spreadPct": .1})
        self.assertFalse(down["allowEntry"])
        self.assertFalse(volatile["allowEntry"])

    def test_cost_filter_can_reject_otherwise_good_signal(self):
        result = adaptive_decision("stocks", {
            "symbol": "AAPL", "m1": .01, "m5": .02, "rvol": 2,
            "spreadPct": .2, "breakout": True,
        }, fee_bps=20, slippage_bps=20)
        self.assertFalse(result["costFilterPassed"])
        self.assertEqual(result["decision"], "PAPER_WATCH")
        self.assertFalse(result["entryAllowed"])


    def test_binance_th_history_requires_live_horizons_and_calculates_rvol(self):
        now = 2_000_000.0
        history = []
        for second in range(3601):
            stamp = now - 3600 + second
            price = 100 * (1 + second * 0.00001)
            # Baseline cumulative volume increases at 1.7 units/second; last minute doubles.
            volume = 300000 + second * 1.7 + max(0, second - 3540) * 1.7
            history.append((stamp, price, volume, price * 0.999, price * 1.001))
        candidates = build_binance_th_candidates({"BTCUSDT": history}, now=now)
        self.assertEqual(len(candidates), 1)
        self.assertGreater(candidates[0]["rvol"], 1.5)
        self.assertTrue(candidates[0]["breakout"])
        stale = build_binance_th_candidates({"BTCUSDT": history[:-1]}, now=now + 10)
        self.assertEqual(stale, [])

    def test_cross_market_opportunity_score_rewards_stronger_net_edge(self):
        crypto = adaptive_decision("crypto", {
            "symbol": "BTCUSDT", "m1": .25, "m5": .80, "m15": .5, "m60": .4,
            "rvol": 3.0, "spreadPct": .05, "breakout": True,
        }, fee_bps=10, slippage_bps=5)
        stock = adaptive_decision("stocks", {
            "symbol": "AAPL", "m1": .08, "m5": .15, "rvol": 2.0,
            "spreadPct": .08, "breakout": True,
        }, fee_bps=10, slippage_bps=5)
        self.assertEqual(crypto["decision"], "PAPER_CANDIDATE")
        self.assertEqual(stock["decision"], "PAPER_CANDIDATE")
        self.assertGreater(crypto["opportunityScore"], stock["opportunityScore"])
        self.assertGreater(crypto["netEdgeProxyPct"], 0)
        self.assertFalse(crypto["entryAllowed"])

    def test_experiment_window_is_fourteen_days(self):
        now = datetime(2026, 11, 23, tzinfo=timezone.utc)
        window = experiment_window(14, now)
        self.assertEqual(window["days"], 14)
        self.assertEqual((datetime.fromisoformat(window["endsAt"]) - now).days, 14)

    def test_net_profit_and_drawdown_include_costs(self):
        result = performance_summary(100, [
            {"at": "2026-11-01T00:00:00Z", "equity": 110},
            {"at": "2026-11-02T00:00:00Z", "equity": 99},
            {"at": "2026-11-03T00:00:00Z", "equity": 105},
        ], fees_paid=1, estimated_slippage=0.5)
        self.assertAlmostEqual(result["netPnlAfterCosts"], 3.5)
        self.assertAlmostEqual(result["maxDrawdownPct"], 10.0)


if __name__ == "__main__":
    unittest.main()
