"""Day 11 (Teammate 3): tests for the IPC methods in ipc_benchmark.py.

Every method must deliver every message exactly once, in order, between two
real processes. (The speed numbers come from: python ipc_benchmark.py)
"""

import multiprocessing as mp
import unittest

import ipc_benchmark as ipc


class IpcCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ctx = mp.get_context("spawn")

    def run_ok(self, kind, batch, n, sink="count"):
        row = self.ctx_run(kind, batch, n, sink)
        self.assertIsNone(row["error"], row["error"])
        self.assertTrue(row["ok"], "messages were lost, repeated, reordered or changed")
        self.assertGreater(row["rate"], 0)
        return row

    def ctx_run(self, kind, batch, n, sink):
        return ipc.run_method(self.ctx, "test", kind, batch, n, sink)


class TestEveryMethodDeliversEverything(IpcCase):
    def test_queue_one_by_one(self):
        self.run_ok("queue", 1, 3_000)

    def test_queue_in_batches(self):
        self.run_ok("queue", 1000, 5_000)

    def test_pipe_one_by_one(self):
        self.run_ok("pipe", 1, 3_000)

    def test_pipe_in_batches(self):
        self.run_ok("pipe", 1000, 5_000)

    def test_shared_memory_one_by_one(self):
        self.run_ok("shm", 1, 3_000)

    def test_shared_memory_in_batches(self):
        self.run_ok("shm", 1000, 5_000)


class TestSharedMemoryRing(IpcCase):
    def test_ring_wraps_around_many_times(self):
        # 2.5 times the ring's capacity: the producer must wait for the reader
        self.run_ok("shm", 1000, int(ipc.CAPACITY * 2.5))

    def test_one_by_one_also_wraps(self):
        self.run_ok("shm", 1, ipc.CAPACITY + 5_000)

    def test_count_that_is_not_a_multiple_of_the_batch(self):
        self.run_ok("shm", 1000, 2_537)

    def test_a_single_message(self):
        self.run_ok("shm", 1000, 1)

    def test_ring_layout_is_aligned(self):
        self.assertEqual(ipc.REC.size, ipc.SLOT)
        self.assertEqual(ipc.WIRE.size, 26)
        self.assertEqual(ipc.CAPACITY & (ipc.CAPACITY - 1), 0)   # power of two
        self.assertEqual(ipc.HEAD_OFF % 8, 0)
        self.assertEqual(ipc.TAIL_OFF % 8, 0)
        self.assertGreaterEqual(ipc.TAIL_OFF - ipc.HEAD_OFF, 64)  # separate cache lines


class TestWithTheOrderBook(IpcCase):
    def test_shared_memory_into_the_order_book(self):
        self.run_ok("shm", 1000, 4_000, sink="book")

    def test_queue_into_the_order_book(self):
        self.run_ok("queue", 1000, 4_000, sink="book")


class TestHelpers(unittest.TestCase):
    def test_record_roundtrip(self):
        record = ipc.make_record(41, 123456789)
        self.assertEqual(ipc.WIRE.unpack(ipc.WIRE.pack(*record)), record)
        self.assertEqual(ipc.REC.unpack(ipc.REC.pack(*record)), record)

    def test_tally_spots_a_missing_message(self):
        tally = ipc.Tally()
        for oid in (1, 2, 4):
            tally.see(ipc.make_record(oid, 0))
        self.assertEqual(tally.bad, 1)

    def test_tally_spots_a_repeated_message(self):
        tally = ipc.Tally()
        for oid in (1, 2, 2):
            tally.see(ipc.make_record(oid, 0))
        self.assertEqual(tally.bad, 1)

    def test_expected_quantity(self):
        self.assertEqual(ipc.expected_quantity(3), 2 + 3 + 4)

    def test_events_for_the_order_book(self):
        events = ipc.to_events([ipc.make_record(1, 0), ipc.make_record(2, 0)])
        self.assertEqual(events[0]["side"], "S")
        self.assertEqual(events[1]["side"], "B")
        self.assertIsInstance(events[0]["order_id"], str)


if __name__ == "__main__":
    unittest.main(verbosity=2)