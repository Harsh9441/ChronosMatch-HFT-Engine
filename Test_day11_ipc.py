"""Day 11 (Teammate 3): tests for IPC - binary batches and cross-process ingest."""

import multiprocessing as mp
import random
import threading
import time
import unittest

from orderbook import (
    BUY, SELL, OrderBook, decode_events, encode_events, send_events,
)


def make_events(count, start=1, seed=0):
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


def digest(book):
    return [(o.order_id, o.side, o.price, o.quantity) for o in book.get_all_orders()]


def child_ingest(conn, out):
    """Runs in a separate process: build a book from whatever arrives."""
    book = OrderBook()
    try:
        total = book.ingest_from_connection(conn)
        out.put(("ok", total, len(book), digest(book)))
    except Exception as exc:                       # report, do not crash silently
        out.put(("error", type(exc).__name__, str(exc), len(book)))


class TestCodecRoundTrip(unittest.TestCase):
    def test_events_survive_the_round_trip(self):
        events = make_events(200)
        back = decode_events(encode_events(events))
        self.assertEqual(len(back), 200)
        for before, after in zip(events, back):
            self.assertEqual(after["order_id"], before["order_id"])
            self.assertEqual(after["side"], before["side"])
            self.assertEqual(after["price"], before["price"])        # 101 == 101.0
            self.assertEqual(after["quantity"], before["quantity"])
            self.assertIsInstance(after["price"], float)

    def test_empty_batch(self):
        data = encode_events([])
        self.assertTrue(data)                      # never empty: empty means "stop"
        self.assertEqual(decode_events(data), [])

    def test_generator_input(self):
        self.assertEqual(len(decode_events(encode_events(e for e in make_events(7)))), 7)

    def test_any_string_id_works(self):
        ids = ["A", "ORD_001", "id with spaces", "日本語", "ünï", "x" * 60_000, "007", " lead"]
        events = [{"order_id": i, "side": BUY, "price": 1.5, "quantity": 1} for i in ids]
        self.assertEqual([e["order_id"] for e in decode_events(encode_events(events))], ids)

    def test_large_quantities_and_prices(self):
        events = [
            {"order_id": "a", "side": BUY, "price": 10**12, "quantity": 10**12},
            {"order_id": "b", "side": BUY, "price": 0.0001, "quantity": 2**63 - 1},
            {"order_id": "c", "side": BUY, "price": 1, "quantity": -(2**63)},
        ]
        back = decode_events(encode_events(events))
        self.assertEqual([e["quantity"] for e in back], [10**12, 2**63 - 1, -(2**63)])
        self.assertEqual(back[1]["price"], 0.0001)

    def test_values_the_book_rejects_still_travel(self):
        events = [
            {"order_id": "1", "side": "X", "price": 5, "quantity": 1},
            {"order_id": "2", "side": BUY, "price": 0, "quantity": 1},
            {"order_id": "3", "side": BUY, "price": float("nan"), "quantity": 1},
            {"order_id": "4", "side": BUY, "price": float("inf"), "quantity": 1},
            {"order_id": "5", "side": BUY, "price": 5, "quantity": 0},
            {"order_id": "6", "side": "BUY", "price": 5, "quantity": 1},
        ]
        back = decode_events(encode_events(events))
        self.assertEqual([e["side"] for e in back], ["X", BUY, BUY, BUY, BUY, "BUY"])
        self.assertTrue(back[2]["price"] != back[2]["price"])        # still NaN
        self.assertEqual(back[3]["price"], float("inf"))


class TestEncodeIsStrict(unittest.TestCase):
    good = {"order_id": "1", "side": BUY, "price": 1, "quantity": 1}

    def reject(self, **changes):
        event = {**self.good, **changes}
        with self.assertRaises(ValueError):
            encode_events([event])

    def test_bad_types_are_refused(self):
        self.reject(order_id=7)
        self.reject(order_id=None)
        self.reject(side=1)
        self.reject(side=None)
        self.reject(price=True)             # would silently become 1.0
        self.reject(price="1")
        self.reject(price=None)
        self.reject(quantity=True)
        self.reject(quantity=1.5)
        self.reject(quantity=2.0)
        self.reject(quantity="1")

    def test_out_of_range_values_are_refused(self):
        self.reject(quantity=2**63)
        self.reject(quantity=-(2**63) - 1)
        self.reject(price=10**400)
        self.reject(order_id="x" * 70_000)
        self.reject(side="s" * 300)

    def test_missing_field(self):
        for field in self.good:
            event = dict(self.good)
            del event[field]
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, f"missing field '{field}'"):
                    encode_events([event])

    def test_non_mapping_event(self):
        for bad in ("text", 5, None, ["1", BUY, 1, 1]):
            with self.subTest(event=bad):
                with self.assertRaises(TypeError):
                    encode_events([bad])

    def test_error_names_the_event_number(self):
        events = make_events(5)
        events[3]["price"] = True
        with self.assertRaisesRegex(ValueError, "Event 4"):
            encode_events(events)


class TestDecodeCatchesDamage(unittest.TestCase):
    def setUp(self):
        self.data = encode_events(make_events(6) + [
            {"order_id": "日本語", "side": SELL, "price": 3.5, "quantity": 9}
        ])

    def test_every_shorter_prefix_is_rejected(self):
        for cut in range(len(self.data)):
            with self.subTest(cut=cut):
                with self.assertRaises(ValueError):
                    decode_events(self.data[:cut])
        self.assertEqual(len(decode_events(self.data)), 7)

    def test_extra_bytes_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "extra"):
            decode_events(self.data + b"\x00")

    def test_count_larger_than_content(self):
        bad = (100).to_bytes(4, "little") + self.data[4:]
        with self.assertRaisesRegex(ValueError, "Truncated"):
            decode_events(bad)

    def test_invalid_utf8_id(self):
        data = bytearray(encode_events([{"order_id": "ab", "side": BUY, "price": 1, "quantity": 1}]))
        data[4 + 19] = 0xFF            # 4-byte count + 19-byte record header, then the id text
        with self.assertRaises(ValueError):
            decode_events(bytes(data))

    def test_accepts_bytearray_and_memoryview(self):
        self.assertEqual(len(decode_events(bytearray(self.data))), 7)
        self.assertEqual(len(decode_events(memoryview(self.data))), 7)


class TestIngestBinary(unittest.TestCase):
    def test_same_book_as_firehose(self):
        events = make_events(2000)
        a, b = OrderBook(), OrderBook()
        self.assertEqual(a.ingest_firehose(events), 2000)
        self.assertEqual(b.ingest_binary(encode_events(events)), 2000)
        self.assertEqual(digest(a), digest(b))
        self.assertEqual(a.best_bid(), b.best_bid())
        self.assertEqual(a.best_ask(), b.best_ask())

    def test_book_rejects_bad_orders_with_the_same_message(self):
        events = make_events(5)
        events[2]["side"] = "X"
        with self.assertRaises(ValueError) as local:
            OrderBook().ingest_firehose(events)
        with self.assertRaises(ValueError) as remote:
            OrderBook().ingest_binary(encode_events(events))
        self.assertEqual(str(local.exception), str(remote.exception))

    def test_orders_before_a_bad_event_stay(self):
        events = make_events(6)
        events[4]["price"] = float("nan")
        book = OrderBook()
        with self.assertRaisesRegex(ValueError, "event 5"):
            book.ingest_binary(encode_events(events))
        self.assertEqual(len(book), 4)

    def test_duplicate_across_batches(self):
        book = OrderBook()
        book.ingest_binary(encode_events(make_events(5)))
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            book.ingest_binary(encode_events(make_events(1)))

    def test_damaged_batch_does_not_touch_the_book(self):
        data = encode_events(make_events(10))
        book = OrderBook()
        with self.assertRaises(ValueError):
            book.ingest_binary(data[:-3])
        self.assertEqual(len(book), 0)             # decoding fails before any insert


class TestConnectionInSameProcess(unittest.TestCase):
    def run_pair(self, events, batch_size=500):
        recv_end, send_end = mp.Pipe(duplex=False)
        book, result = OrderBook(), {}

        def reader():
            try:
                result["total"] = book.ingest_from_connection(recv_end)
            except Exception as exc:
                result["error"] = exc

        t = threading.Thread(target=reader)
        t.start()
        sent = send_events(send_end, events, batch_size)
        t.join(timeout=30)
        self.assertFalse(t.is_alive(), "reader did not stop")
        return book, result, sent

    def test_all_events_arrive(self):
        events = make_events(3000)
        book, result, sent = self.run_pair(events, batch_size=250)
        self.assertEqual((sent, result["total"], len(book)), (3000, 3000, 3000))

    def test_batch_sizes_do_not_change_the_result(self):
        events = make_events(1001)
        reference = None
        for size in (1, 7, 500, 1001, 5000):
            with self.subTest(batch_size=size):
                book, result, _ = self.run_pair(events, size)
                self.assertEqual(result["total"], 1001)
                reference = reference or digest(book)
                self.assertEqual(digest(book), reference)

    def test_empty_stream_stops_cleanly(self):
        book, result, sent = self.run_pair([])
        self.assertEqual((sent, result["total"], len(book)), (0, 0, 0))

    def test_generator_source(self):
        book, result, sent = self.run_pair((e for e in make_events(40)), batch_size=9)
        self.assertEqual((sent, result["total"]), (40, 40))

    def test_bad_batch_size(self):
        recv_end, send_end = mp.Pipe(duplex=False)
        with self.assertRaises(ValueError):
            send_events(send_end, [], batch_size=0)


class TestAcrossRealProcesses(unittest.TestCase):
    """A separate process (same start method as Windows) builds the book."""

    ctx = mp.get_context("spawn")

    def run_child(self, events, batch_size=500):
        recv_end, send_end = self.ctx.Pipe(duplex=False)
        out = self.ctx.Queue()
        proc = self.ctx.Process(target=child_ingest, args=(recv_end, out))
        proc.start()
        send_events(send_end, events, batch_size)
        result = out.get(timeout=60)
        proc.join(timeout=30)
        return result

    def test_book_built_in_another_process_is_identical(self):
        events = make_events(5000)
        local = OrderBook()
        local.ingest_firehose(events)
        status, total, size, remote_digest = self.run_child(events, batch_size=333)
        self.assertEqual((status, total, size), ("ok", 5000, 5000))
        self.assertEqual(remote_digest, digest(local))

    def test_empty_stream(self):
        self.assertEqual(self.run_child([])[:3], ("ok", 0, 0))

    def test_bad_event_is_reported_by_the_other_process(self):
        events = make_events(10)
        events[6]["quantity"] = 0
        status, name, message, size = self.run_child(events)
        self.assertEqual(status, "error")
        self.assertEqual(name, "ValueError")
        self.assertIn("event 7", message)
        self.assertEqual(size, 6)


class TestSpeed(unittest.TestCase):
    def test_encode_decode_50k(self):
        events = make_events(50_000)
        start = time.perf_counter()
        data = b"".join(encode_events(events[i:i + 500]) for i in range(0, 50_000, 500))
        enc = time.perf_counter() - start
        batches = [encode_events(events[i:i + 500]) for i in range(0, 50_000, 500)]
        start = time.perf_counter()
        total = sum(len(decode_events(b)) for b in batches)
        dec = time.perf_counter() - start
        self.assertEqual(total, 50_000)
        print(f"\n   50,000 events: encode {enc:.2f} s, decode {dec:.2f} s, "
              f"{len(data) / 50_000:.1f} bytes/event")
        self.assertLess(enc + dec, 10)


if __name__ == "__main__":
    mp.freeze_support()
    unittest.main(verbosity=2)