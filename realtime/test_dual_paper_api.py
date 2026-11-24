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

    @patch.object(blessing_app, "_dual_market_price", return_value=200.0)
    @patch.object(blessing_app, "_dual_rpc_order")
    @patch.object(blessing_app, "_sb_rest")
    @patch.object(blessing_app, "_paper_auth_user", return_value={"id": "11111111-1111-4111-8111-111111111111"})
    def test_stock_order_enforces_fixed_25_percent_stop_and_user_selected_10_percent_target(self, auth, sb_rest, rpc, quote):
        sb_rest.return_value = [{"id": "22222222-2222-4222-8222-222222222222", "mode": "paper", "enabled": True}]
        rpc.return_value = {"ok": True, "orderId": "paper-stock-order"}
        response = self.client_context.post("/ai/dual-paper/order", json={
            "market": "stocks", "symbol": "AAPL", "side": "buy", "notional": 20,
            "stopLossPct": 1, "takeProfitPct": 10, "idempotencyKey": "test-stock-risk-25-10-001"
        })
        self.assertEqual(response.status_code, 200)
        payload = rpc.call_args.args[0]
        self.assertEqual(payload["p_user_id"], "11111111-1111-4111-8111-111111111111")
        self.assertAlmostEqual(payload["p_price"], 200.1)
        self.assertAlmostEqual(payload["p_stop_loss_price"], 150.075)
        self.assertAlmostEqual(payload["p_take_profit_price"], 220.11)
        quote.assert_called_once_with("stocks", "AAPL")

    @patch.object(blessing_app, "_dual_market_price", return_value=60000.0)
    @patch.object(blessing_app, "_dual_rpc_order")
    @patch.object(blessing_app, "_sb_rest")
    @patch.object(blessing_app, "_paper_auth_user", return_value={"id": "11111111-1111-4111-8111-111111111111"})
    def test_crypto_keeps_per_order_risk_values(self, auth, sb_rest, rpc, quote):
        sb_rest.return_value = [{"id": "22222222-2222-4222-8222-222222222222", "mode": "paper", "enabled": True}]
        rpc.return_value = {"ok": True}
        response = self.client_context.post("/ai/dual-paper/order", json={
            "market": "crypto", "symbol": "BTCUSDT", "side": "buy", "notional": 20,
            "stopLossPct": 1, "takeProfitPct": 2, "idempotencyKey": "test-crypto-risk-unchanged-01"
        })
        self.assertEqual(response.status_code, 200)
        payload = rpc.call_args.args[0]
        self.assertAlmostEqual(payload["p_price"], 60030.0)
        self.assertAlmostEqual(payload["p_stop_loss_price"], 59429.7)
        self.assertAlmostEqual(payload["p_take_profit_price"], 61230.6)
        quote.assert_called_once_with("crypto", "BTCUSDT")

    @patch.object(blessing_app, "_dual_market_price", return_value=100.0)
    @patch.object(blessing_app, "_dual_rpc_order")
    @patch.object(blessing_app, "_sb_rest")
    @patch.object(blessing_app, "_paper_auth_user", return_value={"id": "11111111-1111-4111-8111-111111111111"})
    def test_sell_uses_owned_open_position_and_live_price(self, auth, sb_rest, rpc, quote):
        sb_rest.side_effect = [
            [{"id": "22222222-2222-4222-8222-222222222222", "mode": "paper", "enabled": True}],
            [{"id": "33333333-3333-4333-8333-333333333333", "symbol": "AAPL", "quantity": 0.5}]
        ]
        rpc.return_value = {"ok": True, "realizedPnl": 1.25}
        response = self.client_context.post("/ai/dual-paper/order", json={
            "market": "stocks", "symbol": "AAPL", "side": "sell",
            "positionId": "33333333-3333-4333-8333-333333333333",
            "idempotencyKey": "test-stock-sell-position-0001"
        })
        self.assertEqual(response.status_code, 200)
        payload = rpc.call_args.args[0]
        self.assertEqual(payload["p_position_id"], "33333333-3333-4333-8333-333333333333")
        self.assertEqual(payload["p_quantity"], 0.5)
        self.assertAlmostEqual(payload["p_price"], 99.95)
        quote.assert_called_once_with("stocks", "AAPL")

    @patch.object(blessing_app, "_sb_rest")
    @patch.object(blessing_app, "_paper_auth_user", return_value={"id": "11111111-1111-4111-8111-111111111111"})
    def test_wrong_symbol_is_rejected_before_database_access(self, auth, sb_rest):
        response = self.client_context.post("/ai/dual-paper/order", json={
            "market": "crypto", "symbol": "AAPL", "side": "buy", "notional": 20,
            "idempotencyKey": "test-wrong-symbol-market-001"
        })
        self.assertEqual(response.status_code, 422)
        sb_rest.assert_not_called()

    @patch.object(blessing_app, "_dual_market_price", side_effect=blessing_app.HTTPException(status_code=503, detail="NO_FRESH_LIVE_PRICE"))
    @patch.object(blessing_app, "_dual_rpc_order")
    @patch.object(blessing_app, "_sb_rest")
    @patch.object(blessing_app, "_paper_auth_user", return_value={"id": "11111111-1111-4111-8111-111111111111"})
    def test_missing_live_price_blocks_order(self, auth, sb_rest, rpc, quote):
        sb_rest.return_value = [{"id": "22222222-2222-4222-8222-222222222222", "mode": "paper", "enabled": True}]
        response = self.client_context.post("/ai/dual-paper/order", json={
            "market": "stocks", "symbol": "AAPL", "side": "buy", "notional": 20,
            "idempotencyKey": "test-no-live-price-blocks-001"
        })
        self.assertEqual(response.status_code, 503)
        rpc.assert_not_called()


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


class AutomaticStockEntryTests(unittest.TestCase):
    @patch.object(blessing_app, "_dual_market_price", return_value=200.0)
    @patch.object(blessing_app, "_dual_rpc_order")
    @patch.object(blessing_app, "_sb_rest")
    def test_stock_worker_opens_qualified_candidate_with_fixed_sl_and_ten_percent_tp(self, sb_rest, rpc, quote):
        portfolio = {
            "id": "22222222-2222-4222-8222-222222222222",
            "user_id": "11111111-1111-4111-8111-111111111111",
            "cash_balance": 100.0,
            "initial_capital": 100.0,
        }
        sb_rest.side_effect = [
            [],
            [portfolio],
            [],
        ]
        rpc.return_value = {"ok": True, "orderId": "auto-stock-paper"}
        result = blessing_app._dual_try_adaptive_entry(
            {"id": "AUTO-STOCK", "config": {"stocks_take_profit_pct": 10}},
            portfolio, {"symbol": "AAPL"}, "stocks"
        )
        self.assertEqual(result, {"ok": True, "orderId": "auto-stock-paper"})
        payload = rpc.call_args.args[0]
        self.assertAlmostEqual(payload["p_price"], 200.1)
        self.assertAlmostEqual(payload["p_stop_loss_price"], 150.075)
        self.assertAlmostEqual(payload["p_take_profit_price"], 220.11)
        quote.assert_called_once_with("stocks", "AAPL")

    @patch.object(blessing_app, "_dual_market_price", return_value=200.0)
    @patch.object(blessing_app, "_dual_rpc_order")
    @patch.object(blessing_app, "_sb_rest")
    def test_stock_worker_never_opens_sixth_position(self, sb_rest, rpc, quote):
        portfolio = {
            "id": "22222222-2222-4222-8222-222222222222",
            "user_id": "11111111-1111-4111-8111-111111111111",
            "cash_balance": 100.0,
            "initial_capital": 100.0,
        }
        sb_rest.return_value = [
            [{"id":f"p{i}","symbol":f"S{i}"} for i in range(1,6)]
        ][0]
        result = blessing_app._dual_try_adaptive_entry(
            {"id":"AUTO-STOCK","config":{"stocks_take_profit_pct":10}},
            portfolio, {"symbol":"AAPL"}, "stocks"
        )
        self.assertIsNone(result)
        rpc.assert_not_called()
        quote.assert_called_once_with("stocks", "AAPL")


if __name__ == "__main__":
    unittest.main()
