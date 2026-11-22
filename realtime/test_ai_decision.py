import unittest

from ai_decision import build_decision_preview


class DecisionPreviewTests(unittest.TestCase):
    def test_binance_high_score_is_paper_candidate_but_never_order(self):
        payload = {
            "status": "ok",
            "exchange": "Binance",
            "provider": "Binance Spot public market data",
            "updatedAt": "2026-11-22T00:00:00+00:00",
            "candidates": [{"symbol": "ETHUSDT", "score": 82, "quoteVolume": 2500000,
                            "rvol": 2.1, "spreadPct": 0.1, "breakout": True}],
        }
        result = build_decision_preview(payload)
        self.assertEqual(result["summary"]["paperCandidates"], 1)
        self.assertFalse(result["liveOrdersEnabled"])
        self.assertFalse(result["entryGateOpen"])
        self.assertFalse(result["decisions"][0]["entryAllowed"])

    def test_fallback_provider_never_eligible(self):
        payload = {
            "status": "ok",
            "exchange": "OKX",
            "updatedAt": "2026-11-22T00:00:00+00:00",
            "candidates": [{"symbol": "ETHUSDT", "score": 95}],
        }
        result = build_decision_preview(payload)
        self.assertEqual(result["summary"]["paperCandidates"], 0)
        self.assertTrue(result["haltReasons"])
        self.assertFalse(result["liveOrdersEnabled"])

    def test_daily_loss_halts_candidates(self):
        payload = {
            "status": "ok",
            "exchange": "Binance",
            "updatedAt": "2026-11-22T00:00:00+00:00",
            "candidates": [{"symbol": "BTCUSDT", "score": 99}],
        }
        result = build_decision_preview(payload, daily_loss_pct=2.1)
        self.assertEqual(result["summary"]["paperCandidates"], 0)
        self.assertIn("ขาดทุนรายวันถึงขีดจำกัด", result["haltReasons"])


if __name__ == "__main__":
    unittest.main()
