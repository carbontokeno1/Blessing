import os
import unittest
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

import app as blessing_app
import broker_adapters


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



class BinanceTHReadOnlyAdapterTests(unittest.TestCase):
    @patch.object(broker_adapters.requests, "get")
    def test_uses_binance_th_account_v2_without_global_sapi_calls(self, get):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "canTrade": True, "canWithdraw": True,
            "balances": [{"asset": "USDT", "free": "25.5", "locked": "1"}],
        }
        get.return_value = response
        with patch.dict(os.environ, {
            "BINANCE_TH_API_KEY": "test-read-only-key",
            "BINANCE_TH_API_BASE_URL": "https://api.binance.th",
        }, clear=True):
            result = broker_adapters._binance_account_snapshot()

        self.assertEqual(result["status"], "connected")
        self.assertEqual(result["broker"], "binance_th")
        self.assertFalse(result["liveTradingPermissionReady"])
        self.assertFalse(result["apiKeyPermissions"]["verified"])
        self.assertEqual(result["balances"][0]["asset"], "USDT")
        self.assertEqual(result["balances"][0]["total"], 26.5)
        args, kwargs = get.call_args
        self.assertEqual(args[0], "https://api.binance.th/api/v1/accountV2")
        self.assertEqual(kwargs["headers"]["X-MBX-APIKEY"], "test-read-only-key")
        self.assertNotIn("/sapi/", args[0])

    @patch.object(broker_adapters.requests, "get")
    def test_handles_binance_th_wrapped_data_error(self, get):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {"code": -1002, "msg": "unauthorized", "data": {}}
        get.return_value = response
        with patch.dict(os.environ, {"BINANCE_TH_API_KEY": "test-key"}, clear=True):
            result = broker_adapters._binance_account_snapshot()
        self.assertEqual(result["status"], "connection_error")
        self.assertEqual(result["apiCode"], -1002)


if __name__ == "__main__":
    unittest.main()
