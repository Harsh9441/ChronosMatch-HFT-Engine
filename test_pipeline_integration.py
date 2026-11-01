"""Day 10 (Teammate 3): integration tests.

Checks the WHOLE chain working together:

    firehose  ->  order book  ->  matching engine  ->  trade validator

Until the teammates' real matching engine is ready, ENGINE below is the simple
reference matcher from trade_validator.py. To test the real engine, change the
ENGINE line. An engine is any function that takes a list of Order objects (in
arrival order) and returns a list of trade dictionaries.
"""

import asyncio
import random
import time
import unittest

from orderbook import BUY, SELL, Order, OrderBook
from trade_validator import (
    BUY_ID, PRICE, QUANTITY, SELL_ID, TRADE_ID,
    reference_match, summarize, validate_trades,
)

ENGINE = reference_match          # <-- replace with the teammates' engine


# ---------------------------------------------------------------------------
# Stand-in firehose and the pipeline that connects everything
# ---------------------------------------------------------------------------
async def firehose(count, seed=1, start=1, bad_every=0):
    """Async stream of order dictionaries. bad_every=10 corrupts every 10th one."""
    rng = random.Random(seed)
    for i in range(start, start + count):
        event = {
            "order_id": str(i),
            "side": rng.choice([BUY, SELL]),
            "price": rng.randint(9990, 10010),     # narrow range: many trades
            "quantity": rng.randint(1, 50),
        }
        if bad_every and i % bad_every == 0:
            event = {**event, "side": "Z"}         # invalid side
        yield event
        await asyncio.sleep(0)


async def replay(events):
    for event in events:
        yield event


def to_orders(events, keep_ids=None):
    return [
        Order(e["order_id"], e["side"], float(e["price"]), e["quantity"])
        for e in events
        if keep_ids is None or e["order_id"] in keep_ids
    ]


async def run_pipeline(count=1000, seed=1, engine=None, events=None,
                       cancel_fraction=0.0, tolerant=False, bad_every=0):
    """firehose -> order book -> (cancels) -> engine -> validator.

    tolerant=True inserts one event at a time and records bad ones instead of
    stopping. Returns a dictionary with everything the tests need.
    """
    engine = engine or ENGINE
    if events is None:
        events = [e async for e in firehose(count, seed, bad_every=bad_every)]

    book = OrderBook()
    rejected = []

    start = time.perf_counter()
    if tolerant:
        for number, event in enumerate(events, start=1):
            try:
                book.ingest_firehose([event])
            except (ValueError, TypeError) as error:
                rejected.append((number, str(error)))
    else:
        await book.ingest_async(replay(events))
    ingest_s = time.perf_counter() - start

    cancelled = set()
    if cancel_fraction:
        rng = random.Random(seed + 1)
        live = [o.order_id for o in book.get_all_orders()]
        for oid in rng.sample(live, int(len(live) * cancel_fraction)):
            assert book.cancel_order(oid)
            cancelled.add(oid)

    live_ids = {o.order_id for o in book.get_all_orders()}
    orders = to_orders(events, live_ids)               # arrival order, live only

    start = time.perf_counter()
    trades = engine(orders)
    match_s = time.perf_counter() - start

    start = time.perf_counter()
    problems = validate_trades(orders, trades, mode="batch", price_rule="resting")
    validate_s = time.perf_counter() - start

    return {
        "events": events, "book": book, "orders": orders, "trades": trades,
        "problems": problems, "rejected": rejected, "cancelled": cancelled,
        "ingest_s": ingest_s, "match_s": match_s, "validate_s": validate_s,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class QuietAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    """Async tests without asyncio's "Executing ... took 0.2 seconds" warnings.
    Big pipelines do long stretches of work without waiting, which is fine."""

    async def asyncSetUp(self):
        asyncio.get_running_loop().slow_callback_duration = 60


class TestHappyPath(QuietAsyncTestCase):
    async def test_small_run_is_fully_valid(self):
        r = await run_pipeline(1000)
        self.assertEqual(r["problems"], [])
        self.assertEqual(len(r["book"]), 1000)
        self.assertGreater(len(r["trades"]), 100)

    async def test_20000_orders_end_to_end(self):
        r = await run_pipeline(20_000)
        self.assertEqual(r["problems"], [])
        self.assertEqual(len(r["book"]), 20_000)
        self.assertGreater(len(r["trades"]), 2_000)

    async def test_every_order_reaches_the_book_unchanged(self):
        r = await run_pipeline(5_000)
        for e in r["events"]:
            order = r["book"].get_order(e["order_id"])
            self.assertEqual(
                (order.side, order.price, order.quantity),
                (e["side"], float(e["price"]), e["quantity"]),
            )

    async def test_matching_does_not_change_the_book(self):
        r = await run_pipeline(3_000)
        self.assertEqual(len(r["book"]), 3_000)            # still all there
        self.assertEqual(r["book"].get_all_orders().__len__(), 3_000)

    async def test_traded_quantity_is_possible(self):
        r = await run_pipeline(5_000)
        total_buy = sum(o.quantity for o in r["orders"] if o.side == BUY)
        total_sell = sum(o.quantity for o in r["orders"] if o.side == SELL)
        traded = sum(t[QUANTITY] for t in r["trades"])
        self.assertLessEqual(traded, min(total_buy, total_sell))
        self.assertGreater(traded, 0)

    async def test_order_counts_add_up(self):
        r = await run_pipeline(5_000)
        s = summarize(r["orders"], r["trades"])
        self.assertEqual(
            s["fully_filled_orders"] + s["partly_filled_orders"] + s["unfilled_orders"],
            5_000,
        )
        self.assertLessEqual(s["partly_filled_orders"], len(r["trades"]))

    async def test_trade_ids_are_unique(self):
        r = await run_pipeline(5_000)
        ids = [t[TRADE_ID] for t in r["trades"]]
        self.assertEqual(len(ids), len(set(ids)))

    async def test_same_seed_gives_same_trades(self):
        a = await run_pipeline(2_000, seed=5)
        b = await run_pipeline(2_000, seed=5)
        c = await run_pipeline(2_000, seed=6)
        self.assertEqual(a["trades"], b["trades"])
        self.assertNotEqual(a["trades"], c["trades"])


class TestEdgeCases(QuietAsyncTestCase):
    async def test_empty_firehose(self):
        r = await run_pipeline(0)
        self.assertEqual(len(r["book"]), 0)
        self.assertEqual(r["trades"], [])
        self.assertEqual(r["problems"], [])

    async def test_only_buyers_means_no_trades(self):
        events = [{"order_id": str(i), "side": BUY, "price": 100 + i, "quantity": 5}
                  for i in range(1, 501)]
        r = await run_pipeline(events=events)
        self.assertEqual(r["trades"], [])
        self.assertEqual(r["problems"], [])

    async def test_prices_that_never_cross(self):
        events = (
            [{"order_id": f"B{i}", "side": BUY, "price": 90, "quantity": 5} for i in range(200)]
            + [{"order_id": f"S{i}", "side": SELL, "price": 110, "quantity": 5} for i in range(200)]
        )
        r = await run_pipeline(events=events)
        self.assertEqual(r["trades"], [])
        self.assertEqual(r["problems"], [])

    async def test_one_buy_sweeps_many_sells(self):
        events = [{"order_id": f"S{i}", "side": SELL, "price": 100 + i, "quantity": 1}
                  for i in range(50)]
        events.append({"order_id": "BIG", "side": BUY, "price": 200, "quantity": 30})
        r = await run_pipeline(events=events)
        self.assertEqual(r["problems"], [])
        self.assertEqual(len(r["trades"]), 30)
        self.assertTrue(all(t[BUY_ID] == "BIG" for t in r["trades"]))
        self.assertEqual([t[SELL_ID] for t in r["trades"]], [f"S{i}" for i in range(30)])


class TestBadInput(QuietAsyncTestCase):
    async def test_tolerant_policy_skips_bad_events_and_keeps_going(self):
        r = await run_pipeline(1000, tolerant=True, bad_every=10)
        self.assertEqual(len(r["rejected"]), 100)
        self.assertEqual(len(r["book"]), 900)
        self.assertEqual([n for n, _ in r["rejected"]], list(range(10, 1001, 10)))
        self.assertTrue(all("Side must be" in message for _, message in r["rejected"]))
        self.assertEqual(r["problems"], [])                # trades from good orders are valid

    async def test_strict_policy_stops_at_the_first_bad_event(self):
        book = OrderBook()
        events = [e async for e in firehose(100, bad_every=10)]
        with self.assertRaisesRegex(ValueError, "event 10"):
            await book.ingest_async(replay(events))
        self.assertEqual(len(book), 9)                     # events 1-9 stayed in

    async def test_nan_price_is_kept_out_of_the_whole_chain(self):
        events = [
            {"order_id": "1", "side": BUY, "price": 100, "quantity": 5},
            {"order_id": "2", "side": SELL, "price": float("nan"), "quantity": 5},
            {"order_id": "3", "side": SELL, "price": 100, "quantity": 5},
        ]
        r = await run_pipeline(events=events, tolerant=True)
        self.assertEqual(len(r["rejected"]), 1)
        self.assertEqual(len(r["trades"]), 1)
        self.assertEqual(r["trades"][0][SELL_ID], "3")
        self.assertEqual(r["problems"], [])


class TestCancelsInTheMiddle(QuietAsyncTestCase):
    async def test_cancelled_orders_never_trade(self):
        r = await run_pipeline(5_000, cancel_fraction=0.2)
        self.assertEqual(len(r["cancelled"]), 1_000)
        self.assertEqual(len(r["book"]), 4_000)
        traded_ids = {t[BUY_ID] for t in r["trades"]} | {t[SELL_ID] for t in r["trades"]}
        self.assertTrue(traded_ids.isdisjoint(r["cancelled"]))
        self.assertEqual(r["problems"], [])

    async def test_cancel_everything_leaves_nothing_to_trade(self):
        r = await run_pipeline(1_000, cancel_fraction=1.0)
        self.assertEqual(len(r["book"]), 0)
        self.assertEqual(r["trades"], [])


class TestValidatorCatchesBrokenEngines(QuietAsyncTestCase):
    """The pipeline must FAIL when an engine makes mistakes."""

    @staticmethod
    def broken(change):
        def engine(orders):
            trades = [dict(t) for t in reference_match(orders)]
            return change(trades)
        return engine

    async def assertPipelineRejects(self, change, text):
        r = await run_pipeline(2_000, engine=self.broken(change))
        self.assertTrue(r["problems"], "validator accepted a broken engine")
        self.assertTrue(any(text in p for p in r["problems"]),
                        f"expected {text!r} in {r['problems'][:3]}")

    async def test_engine_that_overfills(self):
        def change(trades):
            trades[10][QUANTITY] += 1_000
            return trades
        await self.assertPipelineRejects(change, "overfill")

    async def test_engine_that_forgets_a_trade(self):
        def change(trades):
            del trades[len(trades) // 2]
            return trades
        await self.assertPipelineRejects(change, "")

    async def test_engine_that_stops_early(self):
        def change(trades):
            return trades[: len(trades) // 2]
        await self.assertPipelineRejects(change, "Missed match")

    async def test_engine_that_swaps_buyer_and_seller(self):
        def change(trades):
            trades[3][BUY_ID], trades[3][SELL_ID] = trades[3][SELL_ID], trades[3][BUY_ID]
            return trades
        await self.assertPipelineRejects(change, "is not a buy order")

    async def test_engine_with_prices_outside_the_limits(self):
        def change(trades):
            trades[5][PRICE] = 1.0
            return trades
        await self.assertPipelineRejects(change, "outside limit prices")

    async def test_engine_that_repeats_a_trade(self):
        def change(trades):
            trades.insert(4, dict(trades[3]))
            return trades
        await self.assertPipelineRejects(change, "duplicate trade id")

    async def test_engine_that_ignores_priority(self):
        def change(trades):
            trades[0], trades[1] = trades[1], trades[0]
            return trades
        r = await run_pipeline(2_000, engine=self.broken(change))
        self.assertTrue(r["problems"])

    async def test_engine_that_invents_an_order(self):
        def change(trades):
            trades[2][SELL_ID] = "GHOST"
            return trades
        await self.assertPipelineRejects(change, "unknown sell order")


class TestStreaming(QuietAsyncTestCase):
    async def test_producer_and_consumer_with_a_small_queue(self):
        """The firehose and the book run at the same time, linked by a queue."""
        queue = asyncio.Queue(maxsize=50)
        book = OrderBook()
        sent = []

        async def producer():
            async for event in firehose(5_000, seed=3):
                sent.append(event)
                await queue.put(event)
            await queue.put(None)

        async def consumer():
            while True:
                event = await queue.get()
                if event is None:
                    return
                book.ingest_firehose([event])

        await asyncio.gather(producer(), consumer())
        self.assertEqual(len(book), 5_000)
        orders = to_orders(sent)
        self.assertEqual(validate_trades(orders, ENGINE(orders), price_rule="resting"), [])

    async def test_two_firehoses_at_the_same_time(self):
        book = OrderBook()

        async def feed(start, seed):
            async for event in firehose(2_000, seed=seed, start=start):
                book.ingest_firehose([event])

        await asyncio.gather(feed(1, 1), feed(10_001, 2))
        self.assertEqual(len(book), 4_000)
        self.assertEqual(len({o.order_id for o in book.get_all_orders()}), 4_000)


class TestScale(QuietAsyncTestCase):
    async def test_50000_orders_end_to_end(self):
        start = time.perf_counter()
        r = await run_pipeline(50_000)
        seconds = time.perf_counter() - start
        self.assertEqual(r["problems"], [])
        self.assertEqual(len(r["book"]), 50_000)
        self.assertLess(seconds, 30)
        print(f"\n   50,000 orders through the whole chain in {seconds:.2f} s "
              f"({len(r['trades']):,} trades)")


if __name__ == "__main__":
    unittest.main(verbosity=2)