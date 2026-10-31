"""Day 9 (Teammate 3): stress tests with 10,000+ orders.

These tests check CORRECTNESS at large sizes and use generous time limits, so
they only fail if something becomes dramatically slower than it should be.
For the full speed table run:  python stress_test.py
"""

import asyncio
import random
import time
import unittest

from orderbook import BUY, SELL, Order, OrderBook


def make_events(count, start=1, seed=1):
    rng = random.Random(seed)
    return [
        {
            "order_id": str(i),
            "side": rng.choice([BUY, SELL]),
            "price": rng.randint(9900, 10100),
            "quantity": rng.randint(1, 100),
        }
        for i in range(start, start + count)
    ]


def to_orders(events):
    return [Order(e["order_id"], e["side"], float(e["price"]), e["quantity"])
            for e in events]


class StressCase(unittest.TestCase):
    def assertBookMatches(self, book, expected_ids):
        orders = book.get_all_orders()
        self.assertEqual(len(book), len(expected_ids))
        self.assertEqual(len(orders), len(expected_ids))
        self.assertEqual({o.order_id for o in orders}, expected_ids)
        for order in random.Random(0).sample(orders, min(1000, len(orders))):
            self.assertIs(book.get_order(order.order_id), order)


class TestBigIngest(StressCase):
    def test_10000_orders(self):
        events = make_events(10_000)
        book = OrderBook()
        self.assertEqual(book.ingest_firehose(events), 10_000)
        self.assertBookMatches(book, {e["order_id"] for e in events})

    def test_50000_orders_fast_and_exact(self):
        events = make_events(50_000)
        book = OrderBook()
        start = time.perf_counter()
        self.assertEqual(book.ingest_firehose(events), 50_000)
        seconds = time.perf_counter() - start
        self.assertBookMatches(book, {e["order_id"] for e in events})
        self.assertLess(seconds, 5)
        print(f"\n   50,000 orders ingested in {seconds:.2f} s")

    def test_every_order_is_stored_unchanged(self):
        events = make_events(20_000)
        book = OrderBook()
        book.ingest_firehose(events)
        for e in events:
            order = book.get_order(e["order_id"])
            self.assertEqual(
                (order.side, order.price, order.quantity),
                (e["side"], float(e["price"]), e["quantity"]),
            )

    def test_buy_and_sell_counts(self):
        events = make_events(30_000)
        book = OrderBook()
        book.ingest_firehose(events)
        buys = sum(1 for e in events if e["side"] == BUY)
        self.assertEqual(len(book.get_orders(BUY)), buys)
        self.assertEqual(len(book.get_orders(SELL)), 30_000 - buys)

    def test_several_batches_add_up(self):
        book = OrderBook()
        for batch in range(10):
            self.assertEqual(
                book.ingest_firehose(make_events(5_000, start=batch * 5_000 + 1, seed=batch)),
                5_000,
            )
        self.assertEqual(len(book), 50_000)

    def test_duplicate_id_is_still_caught_in_a_big_book(self):
        book = OrderBook()
        book.ingest_firehose(make_events(20_000))
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            book.ingest_firehose([{"order_id": "12345", "side": BUY,
                                   "price": 100, "quantity": 1}])
        self.assertEqual(len(book), 20_000)


class TestBigLookups(StressCase):
    def test_best_prices_match_brute_force(self):
        book = OrderBook()
        book.ingest_firehose(make_events(50_000))
        self.assertEqual(book.best_bid().price,
                         max(o.price for o in book.get_orders(BUY)))
        self.assertEqual(book.best_ask().price,
                         min(o.price for o in book.get_orders(SELL)))

    def test_100000_lookups_are_fast(self):
        book = OrderBook()
        book.ingest_firehose(make_events(50_000))
        rng = random.Random(3)
        start = time.perf_counter()
        for _ in range(100_000):
            self.assertIsNotNone(book.get_order(str(rng.randint(1, 50_000))))
        self.assertLess(time.perf_counter() - start, 5)

    def test_missing_ids_in_a_big_book(self):
        book = OrderBook()
        book.ingest_firehose(make_events(20_000))
        for missing in ("0", "20001", "abc", ""):
            self.assertIsNone(book.get_order(missing))


class TestBigCancels(StressCase):
    def test_cancel_5000_of_50000_is_fast(self):
        events = make_events(50_000)
        ids = {e["order_id"] for e in events}
        book = OrderBook()
        book.ingest_firehose(events)

        victims = random.Random(5).sample(sorted(ids), 5_000)
        start = time.perf_counter()
        results = [book.cancel_order(v) for v in victims]
        seconds = time.perf_counter() - start

        self.assertTrue(all(results))
        self.assertBookMatches(book, ids - set(victims))
        self.assertTrue(all(book.get_order(v) is None for v in victims))
        # a slow cancel (scanning a list) takes about 10 s here; a fast one about 0.01 s
        self.assertLess(seconds, 3, f"cancelling 5,000 orders took {seconds:.1f} s")
        print(f"\n   cancelled 5,000 of 50,000 orders in {seconds:.3f} s")

    def test_cancel_everything(self):
        events = make_events(20_000)
        book = OrderBook()
        book.ingest_firehose(events)
        for e in events:
            self.assertTrue(book.cancel_order(e["order_id"]))
        self.assertEqual(len(book), 0)
        self.assertIsNone(book.best_bid())
        self.assertIsNone(book.best_ask())
        self.assertEqual(book.get_all_orders(), [])

    def test_cancel_then_reuse_ids(self):
        events = make_events(10_000)
        book = OrderBook()
        book.ingest_firehose(events)
        for e in events[:5_000]:
            book.cancel_order(e["order_id"])
        again = [{**e, "side": SELL} for e in events[:5_000]]
        self.assertEqual(book.ingest_firehose(again), 5_000)
        self.assertEqual(len(book), 10_000)

    def test_arrival_order_survives_cancels(self):
        book = OrderBook()
        for i in range(1, 2_001):
            book.add_order(Order(str(i), BUY, 100.0, 1))
        for i in range(2, 2_001, 2):                      # cancel the even ones
            book.cancel_order(str(i))
        ids = [int(o.order_id) for o in book.get_orders(BUY)]
        self.assertEqual(ids, sorted(ids))
        self.assertEqual(len(ids), 1_000)


class TestRandomOperations(StressCase):
    def test_100000_random_inserts_and_cancels(self):
        rng = random.Random(11)
        book = OrderBook()
        live = {}
        for step in range(100_000):
            oid = str(rng.randint(1, 20_000))
            if oid in live and rng.random() < 0.5:
                self.assertTrue(book.cancel_order(oid))
                del live[oid]
            elif oid not in live:
                order = Order(oid, rng.choice([BUY, SELL]),
                              float(rng.randint(9900, 10100)), rng.randint(1, 100))
                book.add_order(order)
                live[oid] = order
            else:
                with self.assertRaises(ValueError):
                    book.add_order(Order(oid, BUY, 100.0, 1))
        self.assertBookMatches(book, set(live))
        buys = [o.price for o in live.values() if o.side == BUY]
        sells = [o.price for o in live.values() if o.side == SELL]
        self.assertEqual(book.best_bid().price if buys else None, max(buys, default=None))
        self.assertEqual(book.best_ask().price if sells else None, min(sells, default=None))


class TestAsyncStress(unittest.IsolatedAsyncioTestCase):
    async def test_async_stream_of_20000(self):
        events = make_events(20_000)

        async def stream():
            for e in events:
                yield e
                await asyncio.sleep(0)

        book = OrderBook()
        self.assertEqual(await book.ingest_async(stream()), 20_000)
        self.assertEqual(len(book), 20_000)

    async def test_two_streams_one_after_another(self):
        book = OrderBook()

        async def stream(start):
            for e in make_events(10_000, start=start):
                yield e
                await asyncio.sleep(0)

        self.assertEqual(await book.ingest_async(stream(1)), 10_000)
        self.assertEqual(await book.ingest_async(stream(10_001)), 10_000)
        self.assertEqual(len(book), 20_000)


class TestTradesAtScale(unittest.TestCase):
    def test_reference_trades_validate_for_20000_orders(self):
        try:
            from trade_validator import reference_match, validate_trades
        except ImportError:
            self.skipTest("trade_validator.py is not in this folder")
        orders = to_orders(make_events(20_000))
        trades = reference_match(orders)
        self.assertGreater(len(trades), 5_000)
        self.assertEqual(validate_trades(orders, trades, price_rule="resting"), [])

    def test_damaged_trades_are_caught_at_scale(self):
        try:
            from trade_validator import QUANTITY, reference_match, validate_trades
        except ImportError:
            self.skipTest("trade_validator.py is not in this folder")
        orders = to_orders(make_events(20_000))
        trades = reference_match(orders)
        broken = [dict(t) for t in trades]
        broken[len(broken) // 2][QUANTITY] += 500
        self.assertNotEqual(validate_trades(orders, broken), [])
        self.assertNotEqual(validate_trades(orders, trades[:-1]), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)