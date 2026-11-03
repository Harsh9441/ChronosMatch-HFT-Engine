"""Day 10 (Teammate 3): integration testing / Week 2 verification.

Checks that Days 1-9 work TOGETHER: ingest (sync and async) -> look up ->
cancel -> best bid/ask -> match -> validate trades, on small and large books.

orderbook.py is not a matching engine, so this file contains a small
reference matcher (match_book) that builds trades from the book. Those
trades are then checked by the real validate_trades / check_trades.
"""

import asyncio
import random
import time
import unittest

import orderbook
from orderbook import BUY, SELL, Order, OrderBook, Trade, check_trades, validate_trades


# ------------------------------------------------------------------ helpers
def make_events(count, start=1, seed=0, low=9900, high=10100):
    rng = random.Random(seed)
    return [
        {
            "order_id": str(i),
            "side": rng.choice([BUY, SELL]),
            "price": rng.randint(low, high),
            "quantity": rng.randint(1, 100),
        }
        for i in range(start, start + count)
    ]


def match_book(book):
    """Reference matcher: pair best bids with best asks while prices cross.

    Reads the book only (never changes it). Price priority, then arrival
    order. Trade price is the midpoint of the two limits.
    """
    bids = sorted(enumerate(book.get_orders(BUY)), key=lambda t: (-t[1].price, t[0]))
    asks = sorted(enumerate(book.get_orders(SELL)), key=lambda t: (t[1].price, t[0]))
    left = {o.order_id: o.quantity for _, o in bids + asks}
    trades, bi, ai = [], 0, 0
    while bi < len(bids) and ai < len(asks):
        bid, ask = bids[bi][1], asks[ai][1]
        if bid.price < ask.price:
            break
        qty = min(left[bid.order_id], left[ask.order_id])
        trades.append(
            Trade(f"T{len(trades) + 1}", bid.order_id, ask.order_id,
                  (bid.price + ask.price) / 2, qty)
        )
        left[bid.order_id] -= qty
        left[ask.order_id] -= qty
        if left[bid.order_id] == 0:
            bi += 1
        if left[ask.order_id] == 0:
            ai += 1
    return trades


class BookCase(unittest.TestCase):
    def assertBookHealthy(self, book):
        """Invariants that must hold after ANY sequence of operations."""
        buys, sells = book.get_orders(BUY), book.get_orders(SELL)
        everything = book.get_all_orders()
        self.assertEqual(len(book), len(buys) + len(sells))
        self.assertEqual(everything, buys + sells)
        self.assertTrue(all(o.side == BUY for o in buys))
        self.assertTrue(all(o.side == SELL for o in sells))
        ids = [o.order_id for o in everything]
        self.assertEqual(len(ids), len(set(ids)), "duplicate ids in book")
        for order in everything:
            self.assertIs(book.get_order(order.order_id), order)
        if buys:
            self.assertEqual(book.best_bid().price, max(o.price for o in buys))
        else:
            self.assertIsNone(book.best_bid())
        if sells:
            self.assertEqual(book.best_ask().price, min(o.price for o in sells))
        else:
            self.assertIsNone(book.best_ask())
        snap = book.snapshot()
        self.assertEqual(snap, {"buys": buys, "sells": sells})


# ------------------------------------------------------------------ tests
class TestWeek2Checklist(unittest.TestCase):
    """Every feature promised for Days 1-8 exists in the module."""

    def test_public_names_exist(self):
        for name in ("BUY", "SELL", "Order", "OrderBook", "Trade",
                     "check_trades", "validate_trades"):
            self.assertTrue(hasattr(orderbook, name), name)

    def test_orderbook_methods_exist(self):
        for name in ("add_order", "ingest_firehose", "ingest_async", "get_orders",
                     "get_all_orders", "get_order", "cancel_order",
                     "best_bid", "best_ask", "snapshot", "__len__"):
            self.assertTrue(callable(getattr(OrderBook, name, None)), name)

    def test_new_book_is_empty(self):
        book = OrderBook()
        self.assertEqual(len(book), 0)
        self.assertEqual(book.get_all_orders(), [])
        self.assertIsNone(book.best_bid())
        self.assertIsNone(book.best_ask())
        self.assertEqual(book.snapshot(), {"buys": [], "sells": []})


class TestFullPipeline(BookCase):
    def test_ingest_cancel_match_validate(self):
        book = OrderBook()
        events = make_events(5000, seed=1)
        self.assertEqual(book.ingest_firehose(events), 5000)
        self.assertBookHealthy(book)

        ids = [e["order_id"] for e in events]
        random.Random(2).shuffle(ids)
        for oid in ids[:1500]:
            self.assertTrue(book.cancel_order(oid))
        self.assertEqual(len(book), 3500)
        self.assertBookHealthy(book)

        trades = match_book(book)
        self.assertGreater(len(trades), 0, "this book should cross")
        self.assertEqual(check_trades(book, trades), [])
        self.assertEqual(validate_trades(book, trades), len(trades))

        # matching only reads the book
        self.assertEqual(len(book), 3500)
        self.assertBookHealthy(book)

    def test_matched_quantities_balance(self):
        book = OrderBook()
        book.ingest_firehose(make_events(3000, seed=3))
        trades = match_book(book)
        self.assertEqual(check_trades(book, trades), [])
        bought, sold = {}, {}
        for t in trades:
            bought[t.buy_order_id] = bought.get(t.buy_order_id, 0) + t.quantity
            sold[t.sell_order_id] = sold.get(t.sell_order_id, 0) + t.quantity
        self.assertEqual(sum(bought.values()), sum(sold.values()))
        for oid, qty in {**bought, **sold}.items():
            self.assertLessEqual(qty, book.get_order(oid).quantity)

    def test_damaged_trades_are_caught(self):
        book = OrderBook()
        book.ingest_firehose(make_events(2000, seed=4))
        trades = match_book(book)
        self.assertGreater(len(trades), 10)

        damaged = {
            "price too high": lambda t: Trade(t.trade_id, t.buy_order_id, t.sell_order_id, t.price * 10, t.quantity),
            "overfilled": lambda t: Trade(t.trade_id, t.buy_order_id, t.sell_order_id, t.price, 10**6),
            "unknown buy": lambda t: Trade(t.trade_id, "missing", t.sell_order_id, t.price, t.quantity),
            "sides swapped": lambda t: Trade(t.trade_id, t.sell_order_id, t.buy_order_id, t.price, t.quantity),
            "zero quantity": lambda t: Trade(t.trade_id, t.buy_order_id, t.sell_order_id, t.price, 0),
        }
        for label, damage in damaged.items():
            with self.subTest(damage=label):
                bad = list(trades)
                bad[len(bad) // 2] = damage(bad[len(bad) // 2])
                with self.assertRaises(ValueError):
                    validate_trades(book, bad)

    def test_uncrossed_book_has_no_trades(self):
        book = OrderBook()
        events = [
            {"order_id": f"B{i}", "side": BUY, "price": 90 + i % 9, "quantity": 5}
            for i in range(500)
        ] + [
            {"order_id": f"S{i}", "side": SELL, "price": 100 + i % 9, "quantity": 5}
            for i in range(500)
        ]
        book.ingest_firehose(events)
        self.assertLess(book.best_bid().price, book.best_ask().price)
        self.assertEqual(match_book(book), [])
        self.assertEqual(validate_trades(book, []), 0)


class TestCancelAndTradesTogether(BookCase):
    def setUp(self):
        self.book = OrderBook()
        self.book.add_order(Order("B1", BUY, 101.0, 10))
        self.book.add_order(Order("S1", SELL, 100.0, 10))
        self.trade = Trade("T1", "B1", "S1", 100.5, 10)

    def test_trade_valid_then_invalid_after_cancel(self):
        self.assertEqual(validate_trades(self.book, [self.trade]), 1)
        self.assertTrue(self.book.cancel_order("S1"))
        with self.assertRaisesRegex(ValueError, "S1"):
            validate_trades(self.book, [self.trade])

    def test_trade_valid_again_after_reinsert(self):
        self.book.cancel_order("S1")
        self.book.add_order(Order("S1", SELL, 100.0, 10))
        self.assertEqual(validate_trades(self.book, [self.trade]), 1)

    def test_reinserted_order_with_new_terms_changes_the_verdict(self):
        self.book.cancel_order("S1")
        self.book.add_order(Order("S1", SELL, 102.0, 10))      # no longer crosses
        with self.assertRaisesRegex(ValueError, "do not cross"):
            validate_trades(self.book, [self.trade])

    def test_best_prices_follow_cancels(self):
        self.book.add_order(Order("B2", BUY, 105.0, 1))
        self.assertEqual(self.book.best_bid().order_id, "B2")
        self.book.cancel_order("B2")
        self.assertEqual(self.book.best_bid().order_id, "B1")
        self.book.cancel_order("B1")
        self.assertIsNone(self.book.best_bid())
        self.assertBookHealthy(self.book)

    def test_equal_prices_go_to_earliest_order(self):
        for i in range(5):
            self.book.add_order(Order(f"E{i}", BUY, 200.0, 1))
        self.assertEqual(self.book.best_bid().order_id, "E0")
        self.book.cancel_order("E0")
        self.assertEqual(self.book.best_bid().order_id, "E1")


class TestSyncAndAsyncTogether(BookCase):
    def test_both_ingest_paths_share_one_book(self):
        book = OrderBook()
        sync_events = make_events(1000, start=1, seed=6)
        async_events = make_events(1000, start=1001, seed=7)

        async def stream():
            for e in async_events:
                yield e
                await asyncio.sleep(0)

        self.assertEqual(book.ingest_firehose(sync_events), 1000)
        self.assertEqual(asyncio.run(book.ingest_async(stream())), 1000)
        self.assertEqual(len(book), 2000)
        self.assertBookHealthy(book)

    def test_duplicate_between_sync_and_async_is_rejected(self):
        book = OrderBook()
        book.ingest_firehose(make_events(10))

        async def stream():
            yield {"order_id": "5", "side": BUY, "price": 1, "quantity": 1}

        with self.assertRaisesRegex(ValueError, "Duplicate"):
            asyncio.run(book.ingest_async(stream()))
        self.assertEqual(len(book), 10)

    def test_bad_batch_keeps_earlier_events_and_book_stays_usable(self):
        book = OrderBook()
        events = make_events(20)
        events[12]["price"] = float("nan")
        with self.assertRaisesRegex(ValueError, "event 13"):
            book.ingest_firehose(events)
        self.assertEqual(len(book), 12)
        self.assertBookHealthy(book)

        self.assertEqual(book.ingest_firehose(make_events(5, start=100)), 5)
        self.assertEqual(len(book), 17)
        trades = match_book(book)
        self.assertEqual(check_trades(book, trades), [])
        self.assertBookHealthy(book)


class TestReturnedDataIsIsolated(BookCase):
    def test_outside_changes_do_not_reach_the_book(self):
        book = OrderBook()
        book.ingest_firehose(make_events(100))
        book.get_orders(BUY).clear()
        book.get_all_orders().clear()
        book.snapshot()["sells"].clear()
        book.buys.clear()
        book.sells.clear()
        self.assertEqual(len(book), 100)
        self.assertBookHealthy(book)


class TestDeterminism(unittest.TestCase):
    def test_same_input_gives_same_everything(self):
        results = []
        for _ in range(2):
            book = OrderBook()
            book.ingest_firehose(make_events(3000, seed=9))
            for i in range(1, 3000, 3):
                book.cancel_order(str(i))
            trades = match_book(book)
            results.append((
                book.get_all_orders(), book.best_bid(), book.best_ask(), trades,
            ))
        self.assertEqual(results[0], results[1])


class TestLargeEndToEnd(BookCase):
    def test_20k_orders_full_cycle_is_fast_and_correct(self):
        start = time.perf_counter()
        book = OrderBook()
        events = make_events(20_000, seed=10)
        book.ingest_firehose(events)

        ids = [e["order_id"] for e in events]
        random.Random(11).shuffle(ids)
        for oid in ids[:8000]:
            book.cancel_order(oid)

        trades = match_book(book)
        self.assertEqual(validate_trades(book, trades), len(trades))
        seconds = time.perf_counter() - start

        self.assertEqual(len(book), 12_000)
        self.assertGreater(len(trades), 100)
        self.assertBookHealthy(book)
        print(f"\n   20,000 orders -> 8,000 cancels -> {len(trades):,} trades "
              f"validated in {seconds:.2f} s")
        self.assertLess(seconds, 15)

    def test_random_workload_keeps_invariants(self):
        rng = random.Random(12)
        book = OrderBook()
        live = {}
        for step in range(20_000):
            oid = str(rng.randint(1, 3000))
            if oid in live and rng.random() < 0.45:
                self.assertTrue(book.cancel_order(oid))
                del live[oid]
            elif oid not in live:
                order = Order(oid, rng.choice([BUY, SELL]),
                              float(rng.randint(95, 105)), rng.randint(1, 20))
                book.add_order(order)
                live[oid] = order
            if step % 2500 == 0:
                self.assertBookHealthy(book)
        self.assertEqual({o.order_id for o in book.get_all_orders()}, set(live))
        self.assertBookHealthy(book)
        trades = match_book(book)
        self.assertEqual(check_trades(book, trades), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)