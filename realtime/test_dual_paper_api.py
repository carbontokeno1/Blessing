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

    @patch.object(blessing_app, "_sb_rpc")
    @patch.object(blessing_app, "_paper_auth_user", return_value={"id": "11111111-1111-4111-8111-111111111111"})
    def test_buy_uses_authenticated_user_and_simulated_price_only(self, auth, rpc):
        rpc.return_value = {"ok": True, "orderId": "paper-order", "price": 60000.0}
        response = self.client_context.post("/ai/dual-paper/order", json={
            "user_id": "99999999-9999-4999-8999-999999999999",
            "market": "crypto", "symbol": "BTCUSDT", "side": "buy",
            "quantity": 0.001, "price": 60000,
            "stop_loss_price": 59400, "take_profit_price": 61200,
            "price_source": "manual_simulation",
            "idempotency_key": "test-order-key-00001",
        })
        self.assertEqual(response.status_code, 200)
        payload = rpc.call_args.args[1]
        self.assertEqual(payload["p_user_id"], "11111111-1111-4111-8111-111111111111")
        self.assertEqual(payload["p_market"], "crypto")
        self.assertEqual(payload["p_idempotency_key"], "crypto:test-order-key-00001")
        self.assertEqual(payload["p_price"], 60000.0)
        self.assertEqual(response.json()["priceSource"], "manual_simulation")

    @patch.object(blessing_app, "_sb_rpc")
    @patch.object(blessing_app, "_paper_auth_user", return_value={"id": "11111111-1111-4111-8111-111111111111"})
    def test_unlabeled_or_live_price_is_rejected(self, auth, rpc):
        response = self.client_context.post("/ai/dual-paper/order", json={
            "market": "crypto", "symbol": "BTCUSDT", "side": "buy",
            "quantity": 0.001, "price": 60000,
            "stop_loss_price": 59400, "take_profit_price": 61200,
            "idempotency_key": "test-order-key-00002",
        })
        self.assertEqual(response.status_code, 422)
        rpc.assert_not_called()

    @patch.object(blessing_app, "_sb_rpc")
    @patch.object(blessing_app, "_paper_auth_user", return_value={"id": "11111111-1111-4111-8111-111111111111"})
    def test_wrong_symbol_for_market_is_rejected_before_rpc(self, auth, rpc):
        response = self.client_context.post("/ai/dual-paper/order", json={
            "market": "crypto", "symbol": "AAPL", "side": "buy",
            "quantity": 1, "price": 100,
            "stop_loss_price": 99, "take_profit_price": 102,
            "price_source": "manual_simulation",
            "idempotency_key": "test-order-key-00003",
        })
        self.assertEqual(response.status_code, 422)
        rpc.assert_not_called()

    @patch.object(blessing_app, "_sb_rpc")
    @patch.object(blessing_app, "_paper_auth_user", return_value={"id": "11111111-1111-4111-8111-111111111111"})
    def test_sell_requires_position_and_uses_position_id(self, auth, rpc):
        rpc.return_value = {"ok": True, "realizedPnl": 1.25}
        response = self.client_context.post("/ai/dual-paper/order", json={
            "market": "stocks", "symbol": "AAPL", "side": "sell",
            "quantity": 0.5, "price": 101,
            "position_id": "33333333-3333-4333-8333-333333333333",
            "price_source": "manual_simulation",
            "idempotency_key": "test-order-key-00004",
        })
        self.assertEqual(response.status_code, 200)
        payload = rpc.call_args.args[1]
        self.assertEqual(payload["p_side"], "sell")
        self.assertEqual(payload["p_position_id"], "33333333-3333-4333-8333-333333333333")
        self.assertIsNone(payload["p_stop_loss_price"])
        self.assertIsNone(payload["p_take_profit_price"])


if __name__ == "__main__":
    unittest.main()
