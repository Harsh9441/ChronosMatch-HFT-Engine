"""Day 11 (Teammate 3): tests for the bytes helpers in orderbook.py
(encode_events, decode_events, send_events, OrderBook.ingest_encoded)."""

import multiprocessing as mp
import random
import unittest

from orderbook import (
    BUY, SELL, OrderBook, decode_events, encode_events, send_events,
)


def make_events(count, start=1, seed=1):
    rng = random.Random(seed)
    return [
        {"order_id": str(i), "side": rng.choice([BUY, SELL]),
         "price": float(rng.randint(9900, 10100)), "quantity": rng.randint(1, 100)}
        for i in range(start, start + count)
    ]


def receive_and_count(connection, results):
    """Runs in another process: receive bytes, put them in a book, report."""
    book = OrderBook()
    data = connection.recv_bytes()
    count = book.ingest_encoded(data)
    results.put((count, len(book), book.best_bid().price, book.best_ask().price))


class TestRoundTrip(unittest.TestCase):
    def test_events_survive_encode_and_decode(self):
        events = make_events(500)
        self.assertEqual(decode_events(encode_events(events)), events)

    def test_empty_batch(self):
        data = encode_events([])
        self.assertEqual(len(data), 4)
        self.assertEqual(decode_events(data), [])

    def test_integer_price_comes_back_as_float(self):
        out = decode_events(encode_events([{"order_id": "a", "side": "B", "price": 100, "quantity": 1}]))
        self.assertEqual(out[0]["price"], 100.0)
        self.assertIsInstance(out[0]["price"], float)

    def test_unusual_ids(self):
        ids = ["a", "id with spaces", "日本語", "é-2", "x" * 1000, "0007", " lead"]
        events = [{"order_id": i, "side": BUY, "price": 1.5, "quantity": 2} for i in ids]
        self.assertEqual([e["order_id"] for e in decode_events(encode_events(events))], ids)

    def test_both_sides(self):
        events = [{"order_id": "b", "side": BUY, "price": 1, "quantity": 1},
                  {"order_id": "s", "side": SELL, "price": 2, "quantity": 1}]
        self.assertEqual([e["side"] for e in decode_events(encode_events(events))], [BUY, SELL])

    def test_large_quantities(self):
        events = [{"order_id": "a", "side": BUY, "price": 1, "quantity": 2**64 - 1}]
        self.assertEqual(decode_events(encode_events(events))[0]["quantity"], 2**64 - 1)

    def test_accepts_a_generator(self):
        self.assertEqual(len(decode_events(encode_events(e for e in make_events(10)))), 10)

    def test_decode_accepts_bytearray_and_memoryview(self):
        data = encode_events(make_events(20))
        self.assertEqual(decode_events(bytearray(data)), decode_events(memoryview(data)))

    def test_10000_orders(self):
        events = make_events(10_000)
        self.assertEqual(decode_events(encode_events(events)), events)

    def test_size_is_small_and_predictable(self):
        events = make_events(100)
        expected = 4 + sum(19 + len(e["order_id"]) for e in events)
        self.assertEqual(len(encode_events(events)), expected)


class TestBrokenBytes(unittest.TestCase):
    def setUp(self):
        self.data = encode_events(make_events(5))

    def test_every_shorter_prefix_is_refused(self):
        for cut in range(len(self.data)):
            with self.subTest(cut=cut):
                with self.assertRaises(ValueError):
                    decode_events(self.data[:cut])

    def test_extra_bytes_are_refused(self):
        with self.assertRaisesRegex(ValueError, "extra bytes"):
            decode_events(self.data + b"\x00")

    def test_bad_side_code_is_refused(self):
        broken = bytearray(self.data)
        broken[4] = 7                                   # side byte of the first order
        with self.assertRaisesRegex(ValueError, "side code 7"):
            decode_events(broken)

    def test_id_that_is_not_text_is_refused(self):
        broken = bytearray(encode_events([{"order_id": "a", "side": "B", "price": 1, "quantity": 1}]))
        broken[-1] = 0xFF
        with self.assertRaisesRegex(ValueError, "not valid text"):
            decode_events(broken)

    def test_count_larger_than_content_is_refused(self):
        broken = bytearray(self.data)
        broken[0] = 200
        with self.assertRaises(ValueError):
            decode_events(broken)


class TestEncodeRefusesBadShapes(unittest.TestCase):
    good = {"order_id": "a", "side": "B", "price": 1, "quantity": 1}

    def refused(self, **change):
        event = {**self.good, **change}
        with self.assertRaises(ValueError):
            encode_events([event])

    def test_missing_field(self):
        for field in self.good:
            event = {k: v for k, v in self.good.items() if k != field}
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, f"missing field '{field}'"):
                    encode_events([event])

    def test_not_a_mapping(self):
        with self.assertRaises(TypeError):
            encode_events(["text"])

    def test_bad_values(self):
        self.refused(order_id=5)
        self.refused(side="X")
        self.refused(price="1")
        self.refused(price=True)
        self.refused(quantity=1.5)
        self.refused(quantity=True)
        self.refused(quantity=-1)
        self.refused(quantity=2**64)
        self.refused(order_id="x" * 70_000)

    def test_error_names_the_event(self):
        events = make_events(3)
        events[1]["side"] = "Q"
        with self.assertRaisesRegex(ValueError, "Event 2"):
            encode_events(events)


class TestIngestEncoded(unittest.TestCase):
    def test_same_book_as_ingest_firehose(self):
        events = make_events(2_000)
        a, b = OrderBook(), OrderBook()
        a.ingest_firehose(events)
        self.assertEqual(b.ingest_encoded(encode_events(events)), 2_000)
        self.assertEqual(a.get_orders_in_arrival_order(), b.get_orders_in_arrival_order())
        self.assertEqual(a.best_bid(), b.best_bid())
        self.assertEqual(a.best_ask(), b.best_ask())

    def test_empty_batch_adds_nothing(self):
        book = OrderBook()
        self.assertEqual(book.ingest_encoded(encode_events([])), 0)
        self.assertEqual(len(book), 0)

    def test_duplicate_id_names_the_position_and_keeps_earlier_orders(self):
        events = make_events(5)
        events.append(dict(events[1]))
        book = OrderBook()
        with self.assertRaisesRegex(ValueError, "event 6.*Duplicate"):
            book.ingest_encoded(encode_events(events))
        self.assertEqual(len(book), 5)

    def test_rules_of_the_book_still_apply(self):
        for bad, text in (({"price": 0}, "positive"), ({"price": float("nan")}, "finite"),
                          ({"quantity": 0}, "positive"), ({"order_id": "  "}, "empty")):
            with self.subTest(bad=bad):
                event = {"order_id": "a", "side": "B", "price": 1, "quantity": 1, **bad}
                with self.assertRaisesRegex(ValueError, text):
                    OrderBook().ingest_encoded(encode_events([event]))

    def test_two_batches_in_a_row(self):
        book = OrderBook()
        book.ingest_encoded(encode_events(make_events(100)))
        book.ingest_encoded(encode_events(make_events(100, start=101)))
        self.assertEqual(len(book), 200)

    def test_broken_bytes_leave_the_book_untouched(self):
        book = OrderBook()
        with self.assertRaises(ValueError):
            book.ingest_encoded(encode_events(make_events(5))[:-3])
        self.assertEqual(len(book), 0)


class TestSendingBetweenProcesses(unittest.TestCase):
    def test_send_events_over_a_pipe_in_one_process(self):
        receiver, sender = mp.Pipe(duplex=False)
        events = make_events(50)
        self.assertEqual(send_events(sender, events), 50)
        self.assertEqual(decode_events(receiver.recv_bytes()), events)

    def test_send_accepts_a_generator(self):
        receiver, sender = mp.Pipe(duplex=False)
        self.assertEqual(send_events(sender, (e for e in make_events(7))), 7)
        self.assertEqual(len(decode_events(receiver.recv_bytes())), 7)

    def test_send_refuses_bad_events_before_sending_anything(self):
        receiver, sender = mp.Pipe(duplex=False)
        with self.assertRaises(ValueError):
            send_events(sender, [{"order_id": "a"}])
        self.assertFalse(receiver.poll(0.05))

    def test_another_process_builds_the_same_book(self):
        ctx = mp.get_context("spawn")
        receiver, sender = ctx.Pipe(duplex=False)
        results = ctx.Queue()
        child = ctx.Process(target=receive_and_count, args=(receiver, results))
        child.start()
        events = make_events(1_000)
        send_events(sender, events)
        count, size, best_bid, best_ask = results.get(timeout=60)
        child.join(30)
        mine = OrderBook()
        mine.ingest_firehose(events)
        self.assertEqual((count, size), (1_000, 1_000))
        self.assertEqual(best_bid, mine.best_bid().price)
        self.assertEqual(best_ask, mine.best_ask().price)


if __name__ == "__main__":
    unittest.main(verbosity=2) 