"""Day 14 (Teammate 3): verify the zero-copy architecture (zero_copy.py, Day 13).

What "zero-copy" must mean here, and how each claim is checked:

  1. The reader looks at the SAME memory the writer wrote (identity of the
     underlying buffer, write-through visible to the reader, bytes in the
     segment equal the encoded batch).
  2. Reading a batch allocates (almost) nothing, while a real copy would
     allocate the whole batch (tracemalloc, with a control that proves the
     measurement can see copies).
  3. Nothing is lost or corrupted: wrap-around, back-pressure, bad events,
     damaged slots, end marker, clean-up, and real separate processes.
  4. The results are identical to the normal path (ingest_firehose).
"""

import multiprocessing as mp
import random
import struct
import sys
import threading
import tracemalloc
import unittest
from multiprocessing import shared_memory

import orderbook
import zero_copy as zc
from orderbook import BUY, SELL, Order, OrderBook


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


def as_tuples(orders):
    return [(o.order_id, o.side, o.price, o.quantity) for o in orders]


def expected_tuples(events):
    return [(e["order_id"], e["side"], float(e["price"]), e["quantity"]) for e in events]


# ---- run in separate processes (top level so "spawn" can import them)
def child_ring(handle, out):
    ring = zc.ShmRing.attach(handle)
    book = OrderBook()
    try:
        total = zc.ingest_ring(book, ring)
        out.put(("ok", total, len(book), digest(book)))
    except Exception as exc:
        out.put(("error", type(exc).__name__, str(exc), len(book)))
    finally:
        ring.close()


class RingCase(unittest.TestCase):
    def make_ring(self, slots=4, slot_bytes=64 * 1024, timeout=5.0):
        ring = zc.ShmRing.create(slots=slots, slot_bytes=slot_bytes, timeout=timeout)
        self.addCleanup(self._destroy, ring)
        return ring

    @staticmethod
    def _destroy(ring):
        try:
            ring.close()
        except Exception:
            pass
        try:
            ring.unlink()
        except Exception:
            pass

    @staticmethod
    def raw_slot_bytes(ring, slot, length):
        """Read bytes straight out of the segment through an independent mapping."""
        other = zc._attach(ring._shm.name)
        try:
            start = slot * ring.slot_bytes + 4
            chunk = other.buf[start:start + length]
            try:
                return bytes(chunk)
            finally:
                chunk.release()
        finally:
            other.close()


# =========================================================== 1. encode into a buffer
class TestEncodeInto(unittest.TestCase):
    def test_writes_at_offset_and_leaves_other_bytes_alone(self):
        buf = bytearray(b"\xAA" * 600)
        events = make_events(5)
        n = zc.encode_events_into(buf, events, offset=10)
        self.assertEqual(bytes(buf[:10]), b"\xAA" * 10)
        self.assertEqual(bytes(buf[10 + n:]), b"\xAA" * (600 - 10 - n))
        self.assertEqual(as_tuples(zc.decode_orders(memoryview(buf)[10:10 + n])),
                         expected_tuples(events))

    def test_same_bytes_as_encode_events(self):
        events = make_events(200)
        buf = bytearray(100_000)
        n = zc.encode_events_into(buf, events)
        self.assertEqual(bytes(buf[:n]), zc.encode_events(events))

    @unittest.skipUnless(hasattr(orderbook, "encode_events"), "orderbook.py has no encode_events")
    def test_same_bytes_as_the_order_book_codec(self):
        events = make_events(200)
        self.assertEqual(zc.encode_events(events), orderbook.encode_events(events))

    def test_buffer_too_small_is_untouched(self):
        buf = bytearray(b"\xAA" * 20)
        with self.assertRaisesRegex(ValueError, "needs"):
            zc.encode_events_into(buf, make_events(5))
        self.assertEqual(bytes(buf), b"\xAA" * 20)

    def test_bad_event_leaves_buffer_untouched(self):
        buf = bytearray(b"\xAA" * 1000)
        events = make_events(5)
        events[3]["price"] = True
        with self.assertRaisesRegex(ValueError, "Event 4"):
            zc.encode_events_into(buf, events)
        self.assertEqual(bytes(buf), b"\xAA" * 1000)

    def test_read_only_buffer_is_refused(self):
        with self.assertRaisesRegex(ValueError, "read-only"):
            zc.encode_events_into(bytes(1000), make_events(2))

    def test_bad_offsets(self):
        buf = bytearray(1000)
        for offset in (-1, 999, 5000):
            with self.subTest(offset=offset):
                with self.assertRaises(ValueError):
                    zc.encode_events_into(buf, make_events(2), offset=offset)

    def test_strict_types(self):
        good = {"order_id": "1", "side": BUY, "price": 1, "quantity": 1}
        for change in ({"order_id": 7}, {"side": None}, {"price": True}, {"price": "1"},
                       {"quantity": True}, {"quantity": 1.5}, {"quantity": 2**63},
                       {"price": 10**400}, {"order_id": "x" * 70_000}):
            with self.subTest(change=change):
                with self.assertRaises(ValueError):
                    zc.encode_events([{**good, **change}])
        for bad in ("text", 5, None):
            with self.assertRaises(TypeError):
                zc.encode_events([bad])

    def test_values_the_book_rejects_still_travel(self):
        events = [
            {"order_id": "1", "side": "X", "price": 5, "quantity": 1},
            {"order_id": "2", "side": BUY, "price": 0, "quantity": 1},
            {"order_id": "3", "side": BUY, "price": float("nan"), "quantity": 0},
        ]
        orders = zc.decode_orders(zc.encode_events(events))
        self.assertEqual([o.side for o in orders], ["X", BUY, BUY])
        self.assertTrue(orders[2].price != orders[2].price)

    def test_any_text_in_ids_and_sides(self):
        events = [
            {"order_id": "日本語", "side": "ß", "price": 1.5, "quantity": 1},
            {"order_id": "ORD_001", "side": BUY, "price": 2.5, "quantity": 2},
            {"order_id": "x" * 60_000, "side": SELL, "price": 3.5, "quantity": 3},
            {"order_id": "", "side": "", "price": 4.5, "quantity": 4},
        ]
        orders = zc.decode_orders(zc.encode_events(events))
        self.assertEqual([(o.order_id, o.side) for o in orders],
                         [(e["order_id"], e["side"]) for e in events])


# =========================================================== 2. decode from any buffer
class TestDecode(unittest.TestCase):
    def setUp(self):
        self.events = make_events(50) + [
            {"order_id": "日本語", "side": SELL, "price": 3.5, "quantity": 9}]
        self.data = zc.encode_events(self.events)

    def test_every_buffer_type_gives_the_same_orders(self):
        expect = expected_tuples(self.events)
        for label, buf in (("bytes", self.data), ("bytearray", bytearray(self.data)),
                           ("memoryview", memoryview(self.data))):
            with self.subTest(buffer=label):
                self.assertEqual(as_tuples(zc.decode_orders(buf)), expect)

    def test_decodes_a_window_inside_a_bigger_buffer(self):
        big = bytearray(b"\x00" * 100 + self.data + b"\xFF" * 100)
        window = memoryview(big)[100:100 + len(self.data)]
        try:
            self.assertEqual(as_tuples(zc.decode_orders(window)), expected_tuples(self.events))
        finally:
            window.release()

    def test_every_shorter_prefix_is_refused(self):
        for cut in range(len(self.data)):
            with self.subTest(cut=cut):
                with self.assertRaises(ValueError):
                    zc.decode_orders(self.data[:cut])

    def test_extra_bytes_refused(self):
        with self.assertRaisesRegex(ValueError, "extra"):
            zc.decode_orders(self.data + b"\x00")

    def test_count_larger_than_content(self):
        bad = (10_000).to_bytes(4, "little") + self.data[4:]
        with self.assertRaisesRegex(ValueError, "Truncated"):
            zc.decode_orders(bad)

    def test_id_lengths_that_do_not_match_the_text(self):
        data = bytearray(zc.encode_events([
            {"order_id": "ab", "side": BUY, "price": 1, "quantity": 1},
            {"order_id": "cd", "side": BUY, "price": 1, "quantity": 1}]))
        longer = bytearray(data)
        longer[4] = 3
        with self.assertRaisesRegex(ValueError, "Truncated"):
            zc.decode_orders(longer)
        shorter = bytearray(data)
        shorter[4] = 1
        with self.assertRaisesRegex(ValueError, "extra"):
            zc.decode_orders(shorter)

    def test_invalid_utf8_is_refused(self):
        data = bytearray(zc.encode_events([{"order_id": "é", "side": BUY, "price": 1, "quantity": 1}]))
        data[4 + 19] = 0xFF
        with self.assertRaises(ValueError):
            zc.decode_orders(data)

    def test_no_view_is_left_behind_so_the_source_can_still_be_resized(self):
        """A bytearray cannot grow while any memoryview of it is alive."""
        ok = bytearray(self.data)
        zc.decode_orders(ok)
        ok.append(0)                                         # would raise BufferError
        for label, damaged in (
            ("truncated", bytearray(self.data[:-1])),
            ("extra", bytearray(self.data + b"x")),
            ("not utf-8", bytearray(zc.encode_events(
                [{"order_id": "é", "side": BUY, "price": 1, "quantity": 1}]))),
        ):
            with self.subTest(damage=label):
                if label == "not utf-8":
                    damaged[4 + 19] = 0xFF
                with self.assertRaises(ValueError):
                    zc.decode_orders(damaged)
                damaged.append(0)                            # still resizable

    def test_encode_into_also_leaves_no_view_behind(self):
        buf = bytearray(5000)
        zc.encode_events_into(buf, make_events(10))
        buf.append(0)
        with self.assertRaises(ValueError):
            zc.encode_events_into(buf, [{"order_id": 1}])
        buf.append(0)


# =========================================================== 3. the ring, in one process
class TestRing(RingCase):
    def test_batch_comes_back_identical(self):
        ring = self.make_ring()
        events = make_events(300)
        ring.write(events)
        with ring.read() as view:
            self.assertEqual(as_tuples(zc.decode_orders(view)), expected_tuples(events))

    def test_reader_is_looking_at_the_shared_memory_itself(self):
        """The view's underlying object is the shared segment, not a copy."""
        ring = self.make_ring()
        ring.write(make_events(10))
        with ring.read() as view:
            self.assertIsInstance(view, memoryview)
            self.assertIs(view.obj, ring._shm.buf.obj)
            self.assertFalse(view.readonly)

    def test_bytes_in_the_segment_are_exactly_the_encoded_batch(self):
        ring = self.make_ring()
        events = make_events(40)
        size = ring.write(events)
        self.assertEqual(self.raw_slot_bytes(ring, 0, size), zc.encode_events(events))
        header = self.raw_slot_bytes(ring, 0, 0)
        other = zc._attach(ring._shm.name)
        try:
            self.assertEqual(struct.unpack_from("<I", other.buf, 0)[0], size)
        finally:
            other.close()

    def test_reader_sees_changes_made_after_the_write(self):
        """If the reader had a copy it would not see this edit; it reads live bytes."""
        ring = self.make_ring()
        events = [{"order_id": str(i), "side": BUY, "price": 10, "quantity": 1} for i in range(3)]
        ring.write(events)
        other = zc._attach(ring._shm.name)
        try:
            # slot header 4 + count 4, then 3 x (2+1+8) bytes of columns -> quantities
            struct.pack_into("<q", other.buf, 4 + 4 + 3 * 11, 99)
        finally:
            other.close()
        with ring.read() as view:
            orders = zc.decode_orders(view)
        self.assertEqual([o.quantity for o in orders], [99, 1, 1])

    def test_ring_wraps_many_times_and_keeps_order(self):
        ring = self.make_ring(slots=3)
        events = make_events(600)
        book = OrderBook()
        result = {}

        def reader():
            result["n"] = zc.ingest_ring(book, ring)

        t = threading.Thread(target=reader)
        t.start()
        self.assertEqual(zc.send_events_shm(ring, events, batch_size=7), 600)   # 86 batches, 3 slots
        t.join(20)
        self.assertFalse(t.is_alive())
        self.assertEqual(result["n"], 600)
        plain = OrderBook()
        plain.ingest_firehose(events)
        self.assertEqual(digest(book), digest(plain))

    def test_full_ring_blocks_the_writer_and_loses_nothing(self):
        ring = self.make_ring(slots=2, timeout=0.3)
        first, second, third = make_events(5), make_events(5, start=6), make_events(5, start=11)
        ring.write(first)
        ring.write(second)
        with self.assertRaises(TimeoutError):
            ring.write(third)                                 # no free slot
        with ring.read() as view:
            self.assertEqual(as_tuples(zc.decode_orders(view)), expected_tuples(first))
        with ring.read() as view:
            self.assertEqual(as_tuples(zc.decode_orders(view)), expected_tuples(second))
        ring.write(third)                                     # space again
        with ring.read() as view:
            self.assertEqual(as_tuples(zc.decode_orders(view)), expected_tuples(third))

    def test_empty_ring_times_out_for_the_reader(self):
        ring = self.make_ring(timeout=0.2)
        with self.assertRaises(TimeoutError):
            with ring.read():
                pass

    def test_failed_writes_do_not_use_up_slots(self):
        ring = self.make_ring(slots=2, slot_bytes=200, timeout=0.3)
        for _ in range(6):                                    # more failures than slots
            with self.assertRaisesRegex(ValueError, "needs"):
                ring.write(make_events(50))
        bad = make_events(3)
        bad[1]["quantity"] = True
        for _ in range(6):
            with self.assertRaises(ValueError):
                ring.write(bad)
        ring.write(make_events(2))                            # both slots still free
        ring.write(make_events(2, start=3))

    def test_end_marker_and_empty_batch_are_different(self):
        ring = self.make_ring()
        ring.write([])
        ring.close_stream()
        with ring.read() as view:
            self.assertIsNotNone(view)
            self.assertEqual(zc.decode_orders(view), [])
        with ring.read() as view:
            self.assertIsNone(view)

    def test_batch_that_exactly_fits(self):
        events = make_events(20)
        size = len(zc.encode_events(events))
        fits = self.make_ring(slots=2, slot_bytes=4 + size)
        fits.write(events)
        with fits.read() as view:
            self.assertEqual(len(view), size)
        too_small = self.make_ring(slots=2, slot_bytes=4 + size - 1)
        with self.assertRaisesRegex(ValueError, "needs"):
            too_small.write(events)

    def test_damaged_slot_length_is_refused_and_the_ring_goes_on(self):
        ring = self.make_ring(slots=3)
        ring.write(make_events(5))
        ring.write(make_events(5, start=6))
        other = zc._attach(ring._shm.name)
        try:
            struct.pack_into("<I", other.buf, 0, ring.slot_bytes * 10)      # slot 0 length
        finally:
            other.close()
        with self.assertRaisesRegex(ValueError, "Corrupt"):
            with ring.read():
                pass
        with ring.read() as view:                             # slot 1 is fine
            self.assertEqual(len(zc.decode_orders(view)), 5)
        ring.write(make_events(2, start=20))                  # slots were handed back

    def test_cleanup_removes_the_segment(self):
        ring = zc.ShmRing.create(slots=2, slot_bytes=1024)
        name = ring._shm.name
        ring.close()
        ring.unlink()
        with self.assertRaises(FileNotFoundError):
            shared_memory.SharedMemory(name=name)

    def test_context_manager_cleans_up(self):
        with zc.ShmRing.create(slots=2, slot_bytes=1024) as ring:
            name = ring._shm.name
            ring.write(make_events(2))
        with self.assertRaises(FileNotFoundError):
            shared_memory.SharedMemory(name=name)

    def test_bad_arguments(self):
        with self.assertRaises(ValueError):
            zc.ShmRing.create(slots=0)
        with self.assertRaises(ValueError):
            zc.ShmRing.create(slots=2, slot_bytes=8)
        ring = self.make_ring()
        with self.assertRaises(ValueError):
            zc.send_events_shm(ring, [], batch_size=0)


# =========================================================== 4. same result as the normal path
class TestSameAsNormalPath(RingCase):
    def run_through_ring(self, events, batch_size, ring):
        book, result = OrderBook(), {}

        def reader():
            try:
                result["n"] = zc.ingest_ring(book, ring)
            except Exception as exc:
                result["error"] = exc

        t = threading.Thread(target=reader)
        t.start()
        zc.send_events_shm(ring, events, batch_size)
        t.join(20)
        self.assertFalse(t.is_alive())
        return book, result

    def test_same_book_as_ingest_firehose(self):
        events = make_events(3000)
        plain = OrderBook()
        plain.ingest_firehose(events)
        book, result = self.run_through_ring(events, 250, self.make_ring(slots=4))
        self.assertEqual(result["n"], 3000)
        self.assertEqual(digest(book), digest(plain))
        self.assertEqual(book.best_bid(), plain.best_bid())
        self.assertEqual(book.best_ask(), plain.best_ask())

    def test_same_error_message_as_ingest_firehose(self):
        events = make_events(10)
        events[4]["side"] = "X"
        with self.assertRaises(ValueError) as local:
            OrderBook().ingest_firehose(events)
        book, result = self.run_through_ring(events, 100, self.make_ring(slots=4))
        self.assertEqual(str(result["error"]), str(local.exception))
        self.assertEqual(len(book), 4)                        # orders before the bad one stay

    def test_duplicate_across_batches(self):
        events = make_events(6) + make_events(1)              # id "1" again
        book, result = self.run_through_ring(events, 3, self.make_ring(slots=4))
        self.assertRegex(str(result["error"]), "event 1: Duplicate")
        self.assertEqual(len(book), 6)


# =========================================================== 5. allocation proof
class TestNothingIsCopied(RingCase):
    def peak_extra(self, fn):
        """Largest amount of extra memory held while fn runs, in bytes."""
        tracemalloc.start()
        try:
            tracemalloc.reset_peak()
            base = tracemalloc.get_traced_memory()[0]
            fn()
            return tracemalloc.get_traced_memory()[1] - base
        finally:
            tracemalloc.stop()

    def test_reading_a_batch_from_the_ring_allocates_next_to_nothing(self):
        ring = self.make_ring(slots=2, slot_bytes=256 * 1024)
        ring.write(make_events(4000))
        ring.write(make_events(4000))
        sizes = {}

        def read_in_place():
            with ring.read() as view:
                sizes["n"] = view.nbytes
                struct.unpack_from("<I", view, 0)             # touch the data

        def read_by_copying():
            with ring.read() as view:
                sizes["copy"] = len(bytes(view))              # control: a real copy

        in_place = self.peak_extra(read_in_place)
        copied = self.peak_extra(read_by_copying)
        self.assertGreater(sizes["n"], 50_000)                # the batch is big
        self.assertLess(in_place, 4096)                       # reading it costs ~nothing
        self.assertGreaterEqual(copied, sizes["n"])           # the control SEES a copy

    @unittest.skipIf(sys.platform == "win32", "pipe buffering differs on Windows")
    def test_the_pipe_path_copies_every_batch(self):
        """Control: the normal pipe receive allocates the whole batch each time."""
        payload = zc.encode_events(make_events(4000))
        self.assertGreater(len(payload), 50_000)
        recv_end, send_end = mp.Pipe(duplex=False)
        sender = threading.Thread(target=lambda: send_end.send_bytes(payload))
        sender.start()
        copied = self.peak_extra(lambda: recv_end.recv_bytes())
        sender.join(10)
        self.assertGreaterEqual(copied, len(payload))


# =========================================================== 6. real separate processes
class TestAcrossRealProcesses(RingCase):
    ctx = mp.get_context("spawn")

    def start_child(self, target, arg):
        out = self.ctx.Queue()
        proc = self.ctx.Process(target=target, args=(arg, out))
        proc.start()
        self.addCleanup(lambda: proc.is_alive() and proc.terminate())
        return proc, out

    def test_ring_between_processes_builds_an_identical_book(self):
        events = make_events(20_000)
        plain = OrderBook()
        plain.ingest_firehose(events)

        ring = self.make_ring(slots=8, slot_bytes=64 * 1024, timeout=30)
        proc, out = self.start_child(child_ring, ring.handle())
        self.assertEqual(zc.send_events_shm(ring, events, batch_size=1000), 20_000)   # 20 batches, 8 slots
        status, total, size, remote = out.get(timeout=60)
        proc.join(30)
        self.assertEqual((status, total, size), ("ok", 20_000, 20_000))
        self.assertEqual(remote, digest(plain))

        name = ring._shm.name
        ring.close()
        ring.unlink()
        with self.assertRaises(FileNotFoundError):
            shared_memory.SharedMemory(name=name)

    def test_empty_stream(self):
        ring = self.make_ring(slots=2)
        proc, out = self.start_child(child_ring, ring.handle())
        zc.send_events_shm(ring, [])
        self.assertEqual(out.get(timeout=60)[:3], ("ok", 0, 0))
        proc.join(30)

    def test_bad_event_is_reported_by_the_other_process(self):
        events = make_events(10)
        events[6]["quantity"] = 0
        ring = self.make_ring(slots=4)
        proc, out = self.start_child(child_ring, ring.handle())
        zc.send_events_shm(ring, events, batch_size=100)
        status, name, message, size = out.get(timeout=60)
        proc.join(30)
        self.assertEqual((status, name), ("error", "ValueError"))
        self.assertIn("event 7", message)
        self.assertEqual(size, 6)


if __name__ == "__main__":
    mp.freeze_support()
    unittest.main(verbosity=2)