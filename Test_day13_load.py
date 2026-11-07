"""Day 13: load tests for the order book (50,000+ orders)."""

import unittest

import orderbook as ob


class TestLoad50k(unittest.TestCase):
    def test_50000_orders_every_check_passes(self):
        r = ob.run_load_test(50_000)
        failed = [name for name, ok in r["checks"].items() if not ok]
        print("\n" + ob.format_load_report(r))
        self.assertEqual(failed, [])
        self.assertEqual(r["orders"], 50_000)
        self.assertEqual(r["cancelled"], 10_000)

    def test_100000_orders(self):
        r = ob.run_load_test(100_000, cancel_fraction=0.3)
        self.assertTrue(r["ok"], [k for k, v in r["checks"].items() if not v])

    def test_half_cancelled_triggers_heap_cleanup(self):
        r = ob.run_load_test(50_000, cancel_fraction=0.9)
        self.assertTrue(r["ok"], [k for k, v in r["checks"].items() if not v])

    def test_no_cancels(self):
        r = ob.run_load_test(50_000, cancel_fraction=0)
        self.assertTrue(r["ok"])
        self.assertEqual(r["cancelled"], 0)

    def test_speed_is_reasonable(self):
        # Very loose limits: a slow PC should still pass.
        r = ob.run_load_test(50_000)
        self.assertLess(r["ingest_encoded_s"], 20)
        self.assertLess(r["cancel_s"], 20)

    def test_same_seed_gives_same_orders(self):
        self.assertEqual(ob.make_load_events(1000, 5), ob.make_load_events(1000, 5))
        self.assertNotEqual(ob.make_load_events(1000, 5), ob.make_load_events(1000, 6))

    def test_bad_arguments(self):
        with self.assertRaises(ValueError):
            ob.run_load_test(0)
        with self.assertRaises(ValueError):
            ob.run_load_test(100, cancel_fraction=1)


class TestLoadInBatches(unittest.TestCase):
    def test_50_batches_of_1000(self):
        events = ob.make_load_events(50_000)
        book = ob.OrderBook()
        for start in range(0, 50_000, 1000):
            payload = ob.encode_events(events[start:start + 1000])
            self.assertEqual(book.ingest_encoded(payload), 1000)
        self.assertEqual(len(book), 50_000)
        self.assertEqual(
            [o.order_id for o in book.get_orders_in_arrival_order()],
            [e["order_id"] for e in events],
        )

    def test_bad_order_in_a_big_batch_keeps_earlier_orders(self):
        events = ob.make_load_events(50_000)
        events[40_000]["order_id"] = events[0]["order_id"]    # duplicate
        book = ob.OrderBook()
        with self.assertRaisesRegex(ValueError, "40001"):
            book.ingest_encoded(ob.encode_events(events))
        self.assertEqual(len(book), 40_000)

    def test_damaged_big_payload_leaves_book_untouched(self):
        payload = ob.encode_events(ob.make_load_events(50_000))
        book = ob.OrderBook()
        with self.assertRaises(ValueError):
            book.ingest_encoded(payload[:-5])
        self.assertEqual(len(book), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)