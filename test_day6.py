import asyncio
import random
import time
import unittest

from orderbook import BUY, SELL, OrderBook


async def fake_firehose(count, start=1):
    """Stand-in async firehose: yields dictionaries with string ids."""
    for i in range(start, start + count):
        yield {
            "order_id": str(i),
            "side": random.choice([BUY, SELL]),
            "price": random.randint(9900, 10100),
            "quantity": random.randint(1, 100),
        }
        await asyncio.sleep(0)


class TestIngestAsync(unittest.IsolatedAsyncioTestCase):
    async def test_small_stream(self):
        async def stream():
            yield {"order_id": "1", "side": BUY, "price": 100, "quantity": 10}
            yield {"order_id": "2", "side": SELL, "price": 101, "quantity": 5}
            yield {"order_id": "3", "side": BUY, "price": 99, "quantity": 7}

        book = OrderBook()
        self.assertEqual(await book.ingest_async(stream()), 3)
        self.assertEqual(len(book), 3)
        self.assertEqual(book.best_bid().order_id, "1")
        self.assertEqual(book.best_ask().order_id, "2")

    async def test_empty_stream(self):
        async def stream():
            return
            yield  # makes this an async generator

        book = OrderBook()
        self.assertEqual(await book.ingest_async(stream()), 0)
        self.assertEqual(len(book), 0)

    async def test_every_order_arrives_unchanged(self):
        events = [e async for e in fake_firehose(1000)]

        async def replay():
            for e in events:
                yield e

        book = OrderBook()
        self.assertEqual(await book.ingest_async(replay()), 1000)
        self.assertEqual(len(book), 1000)
        for e in events:
            order = book.get_order(e["order_id"])
            self.assertIsNotNone(order)
            self.assertEqual(
                (order.side, order.price, order.quantity),
                (e["side"], float(e["price"]), e["quantity"]),
            )

    async def test_bad_event_reports_position(self):
        async def stream():
            yield {"order_id": "1", "side": BUY, "price": 100, "quantity": 10}
            yield {"order_id": "2", "side": "X", "price": 100, "quantity": 10}

        with self.assertRaisesRegex(ValueError, "event 2"):
            await OrderBook().ingest_async(stream())

    async def test_integer_order_id_is_rejected(self):
        async def stream():
            yield {"order_id": 1, "side": BUY, "price": 100, "quantity": 10}

        with self.assertRaises(ValueError):
            await OrderBook().ingest_async(stream())

    async def test_duplicate_id_is_rejected(self):
        async def stream():
            yield {"order_id": "1", "side": BUY, "price": 100, "quantity": 10}
            yield {"order_id": "1", "side": SELL, "price": 101, "quantity": 5}

        with self.assertRaisesRegex(ValueError, "Duplicate"):
            await OrderBook().ingest_async(stream())

    async def test_cancel_after_async_ingest(self):
        book = OrderBook()
        await book.ingest_async(fake_firehose(50))
        self.assertTrue(book.cancel_order("10"))
        self.assertIsNone(book.get_order("10"))
        self.assertEqual(len(book), 49)
        self.assertFalse(book.cancel_order("10"))
        self.assertFalse(book.cancel_order("nope"))

    async def test_stress_10000_orders(self):
        book = OrderBook()
        start = time.perf_counter()
        count = await book.ingest_async(fake_firehose(10000))
        seconds = time.perf_counter() - start
        self.assertEqual(count, 10000)
        self.assertEqual(len(book), 10000)
        self.assertEqual(len(book.get_all_orders()), 10000)
        print(f"\n   10,000 orders ingested in {seconds:.2f} s")


if __name__ == "__main__":
    unittest.main()
    