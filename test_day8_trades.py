"""Day 8 (Teammate 3): tests for validating matched trades."""

import unittest

from orderbook import BUY, SELL, Order, OrderBook, Trade, check_trades, validate_trades


class TradeTestCase(unittest.TestCase):
    def setUp(self):
        self.book = OrderBook()
        self.book.add_order(Order("B1", BUY, 101.0, 10))
        self.book.add_order(Order("B2", BUY, 99.0, 5))
        self.book.add_order(Order("S1", SELL, 100.0, 10))
        self.book.add_order(Order("S2", SELL, 102.0, 5))

    def assertProblem(self, trades, text):
        problems = check_trades(self.book, trades)
        self.assertTrue(problems, "expected a problem but trades were valid")
        self.assertTrue(any(text in p for p in problems), problems)


class TestValidTrades(TradeTestCase):
    def test_single_valid_trade(self):
        self.assertEqual(validate_trades(self.book, [Trade("T1", "B1", "S1", 100.5, 10)]), 1)

    def test_price_at_either_limit_is_valid(self):
        trades = [Trade("T1", "B1", "S1", 100.0, 4), Trade("T2", "B1", "S1", 101.0, 6)]
        self.assertEqual(check_trades(self.book, trades), [])

    def test_partial_fills_up_to_full_quantity(self):
        trades = [Trade(f"T{i}", "B1", "S1", 100.5, 2) for i in range(5)]
        self.assertEqual(check_trades(self.book, trades), [])

    def test_empty_list(self):
        self.assertEqual(validate_trades(self.book, []), 0)

    def test_generator_input(self):
        gen = (t for t in [Trade("T1", "B1", "S1", 100.5, 1)])
        self.assertEqual(validate_trades(self.book, gen), 1)

    def test_book_is_not_modified(self):
        before = self.book.get_all_orders()
        validate_trades(self.book, [Trade("T1", "B1", "S1", 100.5, 10)])
        self.assertEqual(self.book.get_all_orders(), before)


class TestBadTradeFields(TradeTestCase):
    def test_bad_trade_ids(self):
        for tid in ("", "   ", None, 7):
            with self.subTest(trade_id=tid):
                self.assertProblem([Trade(tid, "B1", "S1", 100.5, 1)], "trade ID")

    def test_duplicate_trade_id(self):
        self.assertProblem(
            [Trade("T1", "B1", "S1", 100.5, 1), Trade("T1", "B1", "S1", 100.5, 1)],
            "duplicate trade ID",
        )

    def test_bad_prices(self):
        for price in (0, -1, float("nan"), float("inf"), "100", None, True):
            with self.subTest(price=price):
                self.assertProblem([Trade("T1", "B1", "S1", price, 1)], "price must be")

    def test_bad_quantities(self):
        for qty in (0, -1, 1.5, "1", None, True):
            with self.subTest(quantity=qty):
                self.assertProblem([Trade("T1", "B1", "S1", 100.5, qty)], "quantity must be")


class TestBadOrderReferences(TradeTestCase):
    def test_unknown_buy_order(self):
        self.assertProblem([Trade("T1", "NOPE", "S1", 100.5, 1)], "buy order 'NOPE' not found")

    def test_unknown_sell_order(self):
        self.assertProblem([Trade("T1", "B1", "NOPE", 100.5, 1)], "sell order 'NOPE' not found")

    def test_buy_slot_holds_a_sell_order(self):
        self.assertProblem([Trade("T1", "S1", "S2", 101.0, 1)], "not a buy order")

    def test_sell_slot_holds_a_buy_order(self):
        self.assertProblem([Trade("T1", "B1", "B2", 100.5, 1)], "not a sell order")

    def test_non_string_order_ids(self):
        self.assertProblem([Trade("T1", None, 5, 100.5, 1)], "not found")

    def test_cancelled_order_is_rejected(self):
        self.book.cancel_order("S1")
        self.assertProblem([Trade("T1", "B1", "S1", 100.5, 1)], "sell order 'S1' not found")


class TestPriceRules(TradeTestCase):
    def test_orders_that_do_not_cross(self):
        self.assertProblem([Trade("T1", "B2", "S1", 99.5, 1)], "do not cross")

    def test_price_below_sell_limit(self):
        self.assertProblem([Trade("T1", "B1", "S1", 99.9, 1)], "outside")

    def test_price_above_buy_limit(self):
        self.assertProblem([Trade("T1", "B1", "S1", 101.1, 1)], "outside")


class TestOverfill(TradeTestCase):
    def test_single_trade_larger_than_order(self):
        self.assertProblem([Trade("T1", "B1", "S1", 100.5, 11)], "overfilled")

    def test_two_trades_together_overfill(self):
        trades = [Trade("T1", "B1", "S1", 100.5, 6), Trade("T2", "B1", "S1", 100.5, 5)]
        problems = check_trades(self.book, trades)
        self.assertEqual(len(problems), 2)          # buy B1 and sell S1 both overfilled
        self.assertTrue(all(p.startswith("Trade 2") for p in problems))

    def test_smaller_side_limits_the_trade(self):
        self.assertProblem([Trade("T1", "B1", "S2", 101.5, 6)], "'S2' overfilled")

    def test_invalid_trade_does_not_use_up_quantity(self):
        trades = [
            Trade("T1", "B1", "S1", 50.0, 10),      # bad price, ignored for fills
            Trade("T2", "B1", "S1", 100.5, 10),     # fine
        ]
        problems = check_trades(self.book, trades)
        self.assertEqual(len(problems), 1)
        self.assertTrue(problems[0].startswith("Trade 1"))


class TestReporting(TradeTestCase):
    def test_all_problems_reported_with_trade_numbers(self):
        trades = [
            Trade("T1", "B1", "S1", 100.5, 1),      # fine
            Trade("T2", "NOPE", "S1", 100.5, 1),    # bad
            Trade("T3", "B1", "S1", -1, 1),         # bad
        ]
        with self.assertRaises(ValueError) as ctx:
            validate_trades(self.book, trades)
        message = str(ctx.exception)
        self.assertIn("Trade 2", message)
        self.assertIn("Trade 3", message)
        self.assertNotIn("Trade 1:", message)


if __name__ == "__main__":
    unittest.main(verbosity=2)