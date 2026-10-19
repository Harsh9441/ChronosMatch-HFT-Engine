
import unittest

from order_book import Order, OrderBook, BUY, SELL


class TestOrderBook(unittest.TestCase):
    def setUp(self):
        self.book = OrderBook()

    def test_empty_book(self):
        self.assertEqual(self.book.get_orders(BUY), [])
        self.assertEqual(self.book.get_orders(SELL), [])

    def test_insert_buy_order(self):
        order = Order("B1", BUY, 100.0, 5)
        self.book.add_order(order)
        self.assertEqual(self.book.get_order("B1"), order)

    def test_insert_sell_order(self):
        order = Order("S1", SELL, 105.0, 3)
        self.book.add_order(order)
        self.assertEqual(self.book.get_orders(SELL), [order])

    def test_retrieve_orders_by_side(self):
        buy = Order("B1", BUY, 100.0, 5)
        sell = Order("S1", SELL, 105.0, 3)
        self.book.add_order(buy)
        self.book.add_order(sell)

        self.assertEqual(self.book.get_orders(BUY), [buy])
        self.assertEqual(self.book.get_orders(SELL), [sell])

    def test_invalid_order(self):
        with self.assertRaises(ValueError):
            self.book.add_order(Order("X1", "X", 100.0, 1))

    def test_duplicate_order_id(self):
        self.book.add_order(Order("B1", BUY, 100.0, 5))
        with self.assertRaises(ValueError):
            self.book.add_order(Order("B1", SELL, 105.0, 2))

    def test_missing_order(self):
        self.assertIsNone(self.book.get_order("UNKNOWN"))


if __name__ == "__main__":
    unittest.main()
