import unittest
from datetime import datetime, timezone

from adaptive_strategy import adaptive_decision, classify_regime, experiment_window, performance_summary


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
