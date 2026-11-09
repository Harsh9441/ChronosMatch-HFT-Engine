"""Day 18: handling missing and failed records."""

import json
import os
import sqlite3
import tempfile
import threading
import unittest

import orderbook as ob


def ev(oid, side="B", price=100.0, qty=1):
    return {"order_id": oid, "side": side, "price": price, "quantity": qty}


def boom(*_args):
    raise sqlite3.OperationalError("disk I/O error")


class Base(unittest.TestCase):
    def setUp(self):
        self.db = ob.OrderDatabase()
        self.addCleanup(self.db.close)
        self.pb = ob.PersistentBook(self.db)


class TestTolerantSubmit(Base):
    def test_good_and_bad_events_mixed(self):
        r = self.pb.submit_tolerant([
            ev("A"), {"order_id": "X", "side": "B", "quantity": 1},
            ev("B"), ev("C", price=-1), ev("D"),
        ])
        self.assertEqual((r["accepted"], r["rejected"]), (3, 2))
        self.assertEqual([o.order_id for o in self.pb.book.get_orders_in_arrival_order()],
                         ["A", "B", "D"])
        self.assertEqual(self.db.stats()["orders"], 3)
        reasons = [row[3] for row in self.db.rejected_records()]
        self.assertTrue(reasons[0].startswith("Firehose event 2"))
        self.assertTrue(reasons[1].startswith("Firehose event 4"))
        self.assertEqual(r["accepted_positions"], [1, 3, 5])

    def test_each_kind_of_bad_event_is_explained(self):
        cases = [
            (None, "not a mapping"),
            ({"order_id": "A", "side": "B", "quantity": 1}, "missing field 'price'"),
            (ev("A", price=-5), "positive"),
            (ev("A", price=float("nan")), "finite"),
            (ev("A", qty=0), "positive"),
            (ev("A", qty=2.5), "whole number"),
            (ev("A", price=True), "number"),
            (ev("A", side="X"), "side"),
            (ev("   "), "empty"),
            (ev(5), "text"),
            (ev("A", qty=2 ** 63), "too large"),
        ]
        for event, text in cases:
            with self.subTest(text=text):
                r = self.pb.submit_tolerant([event])
                self.assertEqual((r["accepted"], r["rejected"]), (0, 1))
                self.assertIn(text, self.db.rejected_records()[-1][3])
        self.assertEqual(len(self.pb.book), 0)

    def test_duplicate_inside_one_batch(self):
        r = self.pb.submit_tolerant([ev("A"), ev("A")])
        self.assertEqual((r["accepted"], r["rejected"]), (1, 1))
        self.assertIn("duplicate", self.db.rejected_records()[0][3])

    def test_id_from_an_earlier_batch(self):
        self.pb.submit([ev("A")])
        r = self.pb.submit_tolerant([ev("A")])
        self.assertEqual(r["rejected"], 1)
        self.assertIn("used before", self.db.rejected_records()[0][3])

    def test_cancelled_id_cannot_come_back(self):
        self.pb.submit([ev("A")])
        self.pb.cancel("A")
        self.assertEqual(self.pb.submit_tolerant([ev("A")])["rejected"], 1)

    def test_all_bad_stores_nothing(self):
        r = self.pb.submit_tolerant([None, ev("A", qty=0)])
        self.assertEqual((r["accepted"], r["rejected"]), (0, 2))
        self.assertEqual(len(self.pb.book), 0)

    def test_empty_batch(self):
        self.assertEqual(self.pb.submit_tolerant([])["accepted"], 0)

    def test_quarantine_keeps_the_original_event(self):
        bad = ev("A", price=-1)
        self.pb.submit_tolerant([bad])
        _, kind, ref, _, payload = self.db.rejected_records()[0]
        self.assertEqual((kind, ref, json.loads(payload)), ("order", "A", bad))

    def test_odd_data_is_still_saved_as_text(self):
        self.pb.submit_tolerant([{"order_id": "A", "side": "B", "price": 1,
                                  "quantity": {1, 2}}])
        self.assertIsNotNone(self.db.rejected_records()[0][4])

    def test_quarantine_can_be_switched_off(self):
        self.pb.submit_tolerant([None], quarantine=False)
        self.assertEqual(self.db.count_rejected(), 0)

    def test_generator_input(self):
        r = self.pb.submit_tolerant(ev(f"G{i}") for i in range(5))
        self.assertEqual(r["accepted"], 5)


class TestTolerantTrades(Base):
    def setUp(self):
        super().setUp()
        self.pb.submit([ev("B1", "B", 101, 10), ev("S1", "S", 100, 4),
                        ev("S2", "S", 101, 8)])

    def test_bad_trades_are_quarantined_and_good_ones_saved(self):
        r = self.pb.record_trades_tolerant([
            ob.Trade("T1", "B1", "S1", 100.5, 4),
            ob.Trade("BAD", "NOPE", "S2", 101, 1),
            ob.Trade("T2", "B1", "S2", 101, 6),
        ])
        self.assertEqual((r["saved"], r["rejected"]), (2, 1))
        self.assertEqual(self.db.stats()["trades"], 2)
        self.assertIn("not found", self.db.rejected_records()[0][3])
        self.assertEqual(r["saved_positions"], [1, 3])

    def test_overfill_against_an_earlier_batch_is_quarantined(self):
        self.pb.record_trades_tolerant([ob.Trade("T1", "B1", "S2", 101, 8)])
        r = self.pb.record_trades_tolerant([ob.Trade("T2", "B1", "S1", 100.5, 3)])
        self.assertEqual((r["saved"], r["rejected"]), (0, 1))
        self.assertIn("overfilled", self.db.rejected_records()[0][3])
        self.assertEqual(self.db.get_order("B1")["filled"], 8)

    def test_duplicate_trade_id_across_batches(self):
        self.pb.record_trades_tolerant([ob.Trade("T1", "B1", "S1", 100.5, 2)])
        r = self.pb.record_trades_tolerant([ob.Trade("T1", "B1", "S1", 100.5, 1)])
        self.assertEqual(r["rejected"], 1)
        self.assertIn("duplicate trade ID", self.db.rejected_records()[0][3])

    def test_dictionary_trades_are_accepted(self):
        r = self.pb.record_trades_tolerant([
            {"trade_id": "T1", "buy_order_id": "B1", "sell_order_id": "S1",
             "price": 100.5, "quantity": 1}])
        self.assertEqual(r["saved"], 1)

    def test_garbage_items_are_quarantined(self):
        r = self.pb.record_trades_tolerant(["x", 5, None, {}, {"trade_id": "T9"}])
        self.assertEqual((r["saved"], r["rejected"]), (0, 5))
        self.assertEqual(self.db.count_rejected(), 5)

    def test_too_many_bad_trades_stops_everything(self):
        bad = [ob.Trade(f"X{i}", "NOPE", "S1", 100, 1) for i in range(5)]
        good = ob.Trade("T1", "B1", "S1", 100.5, 1)
        with self.assertRaisesRegex(RuntimeError, "nothing was stored"):
            self.pb.record_trades_tolerant([good] + bad, max_failures=2)
        self.assertEqual(self.db.stats()["trades"], 0)
        self.assertEqual(self.db.count_rejected(), 0)

    def test_fully_filled_orders_leave_the_book(self):
        self.pb.record_trades_tolerant([ob.Trade("T1", "B1", "S1", 100.5, 4)])
        self.assertIsNone(self.pb.book.get_order("S1"))
        self.assertIsNotNone(self.pb.book.get_order("B1"))

    def test_quarantine_can_be_switched_off(self):
        self.pb.record_trades_tolerant([ob.Trade("BAD", "NOPE", "S1", 100, 1)],
                                       quarantine=False)
        self.assertEqual(self.db.count_rejected(), 0)

    def test_engine_output_with_bad_trades(self):
        def engine(book, filled, first):
            return ob.reference_match(book, filled, first) + [
                ob.Trade("BADT", "NOPE", "S1", 100, 1)]
        saved = self.pb.run_engine(engine, tolerant=True)
        self.assertGreater(saved, 0)
        self.assertEqual(self.db.count_rejected(), 1)
        self.assertTrue(ob.validate_trade_persistence(self.db)["ok"])

    def test_strict_mode_still_refuses_the_whole_batch(self):
        def engine(book, filled, first):
            return ob.reference_match(book, filled, first) + [
                ob.Trade("BADT", "NOPE", "S1", 100, 1)]
        with self.assertRaises(ValueError):
            self.pb.run_engine(engine)
        self.assertEqual(self.db.stats()["trades"], 0)


class TestFailedWritesRollBack(Base):
    def test_submit_rolls_the_book_back(self):
        self.db._save_orders_once = boom
        with self.assertRaises(sqlite3.OperationalError):
            self.pb.submit([ev("A"), ev("B")])
        del self.db._save_orders_once
        self.assertEqual((len(self.pb.book), self.db.stats()["orders"]), (0, 0))
        self.assertEqual(self.pb.submit([ev("A"), ev("B")]), 2)

    def test_book_error_still_keeps_earlier_orders_everywhere(self):
        with self.assertRaisesRegex(ValueError, "event 2"):
            self.pb.submit([ev("A"), ev("B", price=-1)])
        self.assertEqual((len(self.pb.book), self.db.stats()["orders"]), (1, 1))

    def test_failed_cancel_changes_nothing(self):
        self.pb.submit([ev("A")])
        self.db._mark_cancelled_once = boom
        with self.assertRaises(sqlite3.OperationalError):
            self.pb.cancel("A")
        del self.db._mark_cancelled_once
        self.assertIsNotNone(self.pb.book.get_order("A"))
        self.assertEqual(self.db.get_order("A")["status"], "open")

    def test_failed_trade_save_changes_nothing(self):
        self.pb.submit([ev("B1", "B", 101, 5), ev("S1", "S", 100, 5)])
        self.db._save_trades_once = boom
        with self.assertRaises(sqlite3.OperationalError):
            self.pb.run_engine()
        del self.db._save_trades_once
        self.assertEqual(len(self.pb.book), 2)
        self.assertEqual(self.db.stats()["trades"], 0)
        self.assertEqual(self.pb.run_engine(), 1)                # works afterwards

    def test_errors_that_are_not_lock_errors_are_not_retried(self):
        calls = []

        def counting(*_a):
            calls.append(1)
            raise sqlite3.OperationalError("disk I/O error")

        self.db._save_orders_once = counting
        with self.assertRaises(sqlite3.OperationalError):
            self.pb.submit([ev("A")])
        self.assertEqual(len(calls), 1)


class TestBusyDatabase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.path = os.path.join(self.dir.name, "busy.db")
        self.pb = ob.PersistentBook(ob.OrderDatabase(self.path))
        self.addCleanup(self.pb.db.close)
        self.other = sqlite3.connect(self.path, check_same_thread=False)
        self.addCleanup(self.other.close)
        self.other.execute("BEGIN EXCLUSIVE")
        self.pb.db.conn.execute("PRAGMA busy_timeout = 50")

    def test_clear_error_and_nothing_changes(self):
        self.pb.db.retries = 1
        with self.assertRaises(ob.DatabaseBusyError):
            self.pb.submit([ev("A")])
        self.assertEqual(len(self.pb.book), 0)
        self.other.rollback()
        self.assertEqual(self.pb.submit([ev("A")]), 1)

    def test_write_succeeds_when_the_lock_is_released_in_time(self):
        self.pb.db.retries = 8
        timer = threading.Timer(0.3, self.other.rollback)
        timer.start()
        self.addCleanup(timer.join)
        self.assertEqual(self.pb.submit([ev("A")]), 1)
        self.assertEqual(self.pb.db.stats()["orders"], 1)


class TestReconcile(Base):
    def setUp(self):
        super().setUp()
        self.pb.submit([ev("B1", "B", 101, 10), ev("S1", "S", 100, 4),
                        ev("S2", "S", 120, 3)])
        self.pb.run_engine()

    def test_healthy_state(self):
        r = self.pb.reconcile()
        self.assertTrue(r["consistent_before"])
        self.assertEqual(r["repaired"], 0)

    def test_order_the_database_never_received(self):
        self.pb.book.add_order(ob.Order("GHOST", "B", 1.0, 1))
        r = self.pb.reconcile()
        self.assertEqual(r["unsaved_in_database"], ["GHOST"])
        self.pb.reconcile(repair=True)
        self.assertEqual(self.db.get_order("GHOST")["status"], "open")
        self.assertTrue(self.pb.reconcile()["consistent_before"])

    def test_finished_order_still_resting_in_the_book(self):
        self.pb.book.add_order(ob.Order("S1", "S", 100, 4))      # S1 is filled
        self.assertEqual(self.pb.reconcile()["stale_in_book"], ["S1"])
        self.pb.reconcile(repair=True)
        self.assertIsNone(self.pb.book.get_order("S1"))

    def test_open_order_missing_from_the_book(self):
        self.pb.book.cancel_order("S2")
        self.assertEqual(self.pb.reconcile()["missing_in_book"], ["S2"])
        self.assertEqual(self.pb.reconcile(repair=True)["repaired"], 1)
        self.assertIsNotNone(self.pb.book.get_order("S2"))

    def test_different_terms_are_reported_not_guessed(self):
        with self.db.conn:
            self.db.conn.execute("UPDATE orders SET price = 125 WHERE order_id = 'S2'")
        r = self.pb.reconcile(repair=True)
        self.assertEqual(r["mismatched"], ["S2"])
        self.assertFalse(r["consistent_after"])

    def test_repair_twice_changes_nothing_more(self):
        self.pb.book.add_order(ob.Order("GHOST", "B", 1.0, 1))
        self.pb.reconcile(repair=True)
        self.assertEqual(self.pb.reconcile(repair=True)["repaired"], 0)

    def test_reports_waiting_quarantined_records(self):
        self.pb.submit_tolerant([None, None])
        self.assertEqual(self.pb.reconcile()["unresolved_rejected"], 2)


class TestRetryRejected(Base):
    def test_trade_that_arrived_before_its_orders(self):
        late = [ob.Trade("TL", "L1", "L2", 100.0, 5)]
        self.pb.run_engine(lambda b, f, n: late, tolerant=True)
        self.assertEqual(self.db.count_rejected(), 1)
        self.pb.submit([ev("L1", "B", 101, 5), ev("L2", "S", 100, 5)])
        r = self.pb.retry_rejected()
        self.assertEqual((r["trades_resolved"], r["still_failing"]), (1, 0))
        self.assertEqual(self.db.get_order("L1")["status"], "filled")
        self.assertIsNone(self.pb.book.get_order("L2"))
        self.assertTrue(ob.validate_trade_persistence(self.db)["ok"])

    def test_failures_that_stay_bad_are_not_quarantined_twice(self):
        self.pb.submit_tolerant([ev("A", price=-1), None])
        total_before = self.db.count_rejected(unresolved_only=False)
        r = self.pb.retry_rejected()
        self.assertEqual((r["orders_resolved"], r["still_failing"]), (0, 2))
        self.assertEqual(self.db.count_rejected(unresolved_only=False), total_before)

    def test_retry_twice_is_harmless(self):
        late = [ob.Trade("TL", "L1", "L2", 100.0, 5)]
        self.pb.run_engine(lambda b, f, n: late, tolerant=True)
        self.pb.submit([ev("L1", "B", 101, 5), ev("L2", "S", 100, 5)])
        self.pb.retry_rejected()
        again = self.pb.retry_rejected()
        self.assertEqual((again["trades_resolved"], self.db.stats()["trades"]), (0, 1))

    def test_unreadable_payloads_are_skipped(self):
        self.pb.run_engine(lambda b, f, n: ["garbage"], tolerant=True)
        r = self.pb.retry_rejected()
        self.assertEqual((r["trades_resolved"], r["still_failing"]), (0, 1))


class TestDemo(unittest.TestCase):
    def test_failure_demo_passes_every_check(self):
        r = ob.run_failure_handling_demo(5000)
        failed = [k for k, v in r["checks"].items() if not v]
        print("\n" + ob.format_failure_report(r))
        self.assertEqual(failed, [])
        self.assertEqual(r["bad_events"], 100)
        self.assertIn("ALL CHECKS PASSED", ob.format_failure_report(r))


if __name__ == "__main__":
    unittest.main(verbosity=2)