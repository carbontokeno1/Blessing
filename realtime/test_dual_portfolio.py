import unittest
from decimal import Decimal

from dual_portfolio import (
    calculate_position_size,
    portfolio_config,
    portfolio_overview,
)


class DualPortfolioTests(unittest.TestCase):
    def test_starting_capital_and_currencies_are_separate(self):
        crypto = portfolio_config("crypto")
        stocks = portfolio_config("stocks")
        self.assertEqual(crypto["starting_capital"], "100")
        self.assertEqual(crypto["currency"], "USDT")
        self.assertEqual(stocks["starting_capital"], "100")
        self.assertEqual(stocks["currency"], "USD")
        self.assertNotEqual(crypto["portfolio_id"], stocks["portfolio_id"])

    def test_no_cross_portfolio_transfers_or_live_orders(self):
        overview = portfolio_overview()
        self.assertFalse(overview["liveOrdersEnabled"])
        self.assertFalse(overview["crossPortfolioTransfersEnabled"])
        self.assertFalse(overview["portfolios"]["crypto"]["liveOrdersEnabled"])
        self.assertFalse(overview["portfolios"]["stocks"]["liveOrdersEnabled"])

    def test_position_sizing_uses_individual_portfolio_equity(self):
        crypto = calculate_position_size("crypto", "100", "1.5")
        stocks = calculate_position_size("stocks", "100", "2")
        self.assertEqual(Decimal(crypto["riskBudget"]), Decimal("1"))
        self.assertEqual(Decimal(crypto["maxNotional"]).quantize(Decimal("0.01")), Decimal("66.66"))
        self.assertEqual(Decimal(stocks["riskBudget"]), Decimal("1"))
        self.assertEqual(Decimal(stocks["maxNotional"]), Decimal("50"))

    def test_invalid_market_and_stop_are_rejected(self):
        with self.assertRaises(ValueError):
            portfolio_config("combined")
        with self.assertRaises(ValueError):
            calculate_position_size("crypto", "100", "0")


if __name__ == "__main__":
    unittest.main()
