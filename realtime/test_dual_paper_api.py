import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import app as blessing_app


class DualPaperApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client_context = TestClient(blessing_app.app)
        cls.client_context.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.client_context.__exit__(None, None, None)

    @patch.object(blessing_app, "_dual_rpc_order")
    @patch.object(blessing_app, "_dual_market_price", return_value=100.0)
    @patch.object(blessing_app, "_sb_rest")
    @patch.object(blessing_app, "_paper_auth_user", return_value={"id": "11111111-1111-4111-8111-111111111111"})
    def test_buy_uses_server_price_and_paper_rpc(self, auth, sb_rest, market_price, rpc):
        sb_rest.return_value = [{
            "id": "22222222-2222-4222-8222-222222222222",
            "mode": "paper",
            "enabled": True,
        }]
        rpc.return_value = {"ok": True, "orderId": "paper-order"}
        response = self.client_context.post("/ai/dual-paper/order", json={
            "market": "crypto",
            "symbol": "BTCUSDT",
            "side": "buy",
            "notional": 20,
            "stopLossPct": 1,
            "takeProfitPct": 2,
            "price": 1,
        })
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["liveOrdersEnabled"])
        self.assertEqual(payload["marketPrice"], 100.0)
        self.assertAlmostEqual(payload["simulatedFillPrice"], 100.05, places=7)
        args = rpc.call_args.args[0]
        self.assertAlmostEqual(args["p_price"], 100.05, places=7)
        self.assertAlmostEqual(args["p_quantity"], round(20 / 100.05, 10), places=10)
        self.assertAlmostEqual(args["p_stop_loss_price"], 99.0495, places=6)
        self.assertAlmostEqual(args["p_take_profit_price"], 102.051, places=6)
        self.assertEqual(args["p_market"], "crypto")
        self.assertEqual(args["p_side"], "buy")

    @patch.object(blessing_app, "_dual_rpc_order")
    @patch.object(blessing_app, "_dual_market_price", return_value=50.0)
    @patch.object(blessing_app, "_sb_rest")
    @patch.object(blessing_app, "_paper_auth_user", return_value={"id": "11111111-1111-4111-8111-111111111111"})
    def test_sell_closes_full_database_position(self, auth, sb_rest, market_price, rpc):
        sb_rest.side_effect = [
            [{"id": "22222222-2222-4222-8222-222222222222", "mode": "paper", "enabled": True}],
            [{"id": "33333333-3333-4333-8333-333333333333", "symbol": "AAPL", "quantity": 0.5}],
        ]
        rpc.return_value = {"ok": True, "result": {"realizedPnl": 0.5}}
        response = self.client_context.post("/ai/dual-paper/order", json={
            "market": "stocks", "symbol": "AAPL", "side": "sell",
            "positionId": "33333333-3333-4333-8333-333333333333",
        })
        self.assertEqual(response.status_code, 200)
        args = rpc.call_args.args[0]
        self.assertEqual(args["p_side"], "sell")
        self.assertEqual(args["p_quantity"], 0.5)
        self.assertEqual(args["p_position_id"], "33333333-3333-4333-8333-333333333333")
        self.assertAlmostEqual(args["p_price"], 49.975, places=7)

    @patch.object(blessing_app, "_dual_market_price")
    @patch.object(blessing_app, "_sb_rest", return_value=[
        {"id": "22222222-2222-4222-8222-222222222222", "mode": "paper", "enabled": False}
    ])
    @patch.object(blessing_app, "_paper_auth_user", return_value={"id": "11111111-1111-4111-8111-111111111111"})
    def test_disabled_portfolio_rejects_order_before_price_fetch(self, auth, sb_rest, market_price):
        response = self.client_context.post("/ai/dual-paper/order", json={
            "market": "crypto", "symbol": "BTCUSDT", "side": "buy", "notional": 10
        })
        self.assertEqual(response.status_code, 409)
        market_price.assert_not_called()

    @patch.object(blessing_app, "_paper_auth_user", return_value={"id": "11111111-1111-4111-8111-111111111111"})
    def test_invalid_market_is_rejected(self, auth):
        response = self.client_context.post("/ai/dual-paper/order", json={
            "market": "combined", "symbol": "BTCUSDT", "side": "buy", "notional": 10
        })
        self.assertEqual(response.status_code, 422)


if __name__ == "__main__":
    unittest.main()
