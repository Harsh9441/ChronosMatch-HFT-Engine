"""Unit tests for ChronosMatch OrderBook Day 1-4."""

import unittest

from orderbook import BUY, SELL, Order, OrderBook


class TestOrderBook(unittest.TestCase):
    def setUp(self):
        self.book = OrderBook()

    # Day 1: structure
    def test_new_book_is_empty(self):
        self.assertEqual(len(self.book), 0)
        self.assertEqual(self.book.get_orders(BUY), [])
        self.assertEqual(self.book.get_orders(SELL), [])

    # Day 2: insert and validate
    def test_add_buy_order(self):
        order = Order("B1", BUY, 100.0, 5)
        self.book.add_order(order)
        self.assertEqual(self.book.get_orders(BUY), [order])

    def test_add_sell_order(self):
        order = Order("S1", SELL, 105.0, 3)
        self.book.add_order(order)
        self.assertEqual(self.book.get_orders(SELL), [order])

    def test_reject_invalid_side(self):
        with self.assertRaises(ValueError):
            self.book.add_order(Order("X1", "X", 100.0, 1))

    def test_reject_nonpositive_price(self):
        with self.assertRaises(ValueError):
            self.book.add_order(Order("B1", BUY, 0, 1))

    def test_reject_nonpositive_quantity(self):
        with self.assertRaises(ValueError):
            self.book.add_order(Order("B1", BUY, 100.0, 0))

    def test_reject_duplicate_order_id(self):
        self.book.add_order(Order("O1", BUY, 100.0, 2))
        with self.assertRaises(ValueError):
            self.book.add_order(Order("O1", SELL, 105.0, 2))
   # Day 3: retrieval
    def test_get_order_by_id(self):
        order = Order("B1", BUY, 100.0, 5)
        self.book.add_order(order)
        self.assertEqual(self.book.get_order("B1"), order)

    def test_missing_order_returns_none(self):
        self.assertIsNone(self.book.get_order("missing"))

    def test_invalid_retrieval_side(self):
        with self.assertRaises(ValueError):
            self.book.get_orders("X")

    def test_get_all_orders(self):
        buy = Order("B1", BUY, 100.0, 5)
        sell = Order("S1", SELL, 105.0, 3)
        self.book.add_order(buy)
        self.book.add_order(sell)
        self.assertEqual(self.book.get_all_orders(), [buy, sell])
# Day 4: cancellation and best prices
    def test_cancel_buy_order(self):
        self.book.add_order(Order("B1", BUY, 100.0, 5))
        self.assertTrue(self.book.cancel_order("B1"))
        self.assertIsNone(self.book.get_order("B1"))
        self.assertEqual(len(self.book), 0)

    def test_cancel_missing_order_returns_false(self):
        self.assertFalse(self.book.cancel_order("missing"))

    def test_best_bid_is_highest_buy_price(self):
        low = Order("B1", BUY, 99.0, 2)
        high = Order("B2", BUY, 101.0, 3)
        self.book.add_order(low)
        self.book.add_order(high)
        self.assertEqual(self.book.best_bid(), high)

    def test_best_ask_is_lowest_sell_price(self):
        high = Order("S1", SELL, 105.0, 2)
        low = Order("S2", SELL, 102.0, 3)
        self.book.add_order(high)
        self.book.add_order(low)
        self.assertEqual(self.book.best_ask(), low)

    def test_best_prices_on_empty_book(self):
        self.assertIsNone(self.book.best_bid())
        self.assertIsNone(self.book.best_ask())

    def test_snapshot_returns_both_sides(self):
        buy = Order("B1", BUY, 100.0, 1)
        sell = Order("S1", SELL, 102.0, 1)
        self.book.add_order(buy)
        self.book.add_order(sell)
        self.assertEqual(self.book.snapshot(), {"buys": [buy], "sells": [sell]})

    # Day 5: firehose integration
    def test_ingest_firehose_events(self):
        events = [
            {"order_id": "B1", "side": BUY, "price": 100.0, "quantity": 5},
            {"order_id": "S1", "side": SELL, "price": 101, "quantity": 3},
        ]

        self.assertEqual(self.book.ingest_firehose(events), 2)
        self.assertEqual(self.book.get_order("B1"), Order("B1", BUY, 100.0, 5))
        self.assertEqual(self.book.get_order("S1"), Order("S1", SELL, 101.0, 3))

    def test_ingest_empty_firehose(self):
        self.assertEqual(self.book.ingest_firehose([]), 0)
        self.assertEqual(len(self.book), 0)

    def test_ingest_firehose_rejects_missing_fields(self):
        with self.assertRaisesRegex(ValueError, "missing field 'quantity'"):
            self.book.ingest_firehose(
                [{"order_id": "B1", "side": BUY, "price": 100.0}]
            )

    def test_ingest_firehose_reports_invalid_order(self):
        with self.assertRaisesRegex(ValueError, "Firehose event 1"):
            self.book.ingest_firehose(
                [{"order_id": "B1", "side": BUY, "price": 100.0, "quantity": 0}]
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
