"""Day 19: recovery and resiliency testing."""

import os
import sqlite3
import tempfile
import unittest

import orderbook as ob


def ev(oid, side="B", price=100.0, qty=1):
    return {"order_id": oid, "side": side, "price": price, "quantity": qty}


TRADE_SETUP = [ev("B1", "B", 101, 10), ev("S1", "S", 100, 4), ev("S2", "S", 101, 8)]
T1 = ob.Trade("T1", "B1", "S1", 100.5, 4)
T2 = ob.Trade("T2", "B1", "S2", 101.0, 6)
BAD = ob.Trade("TX", "NOPE", "S1", 100.0, 1)
THREE = [ev(f"N{i}", "B", 50.0 + i) for i in range(3)]


def fresh(setup=()):
    def make():
        pb = ob.PersistentBook(ob.OrderDatabase())
        if setup:
            pb.submit(list(setup))
        return pb
    return make


def late_state():
    pb = ob.PersistentBook(ob.OrderDatabase())
    pb.run_engine(lambda b, f, n: [ob.Trade("TL", "L1", "L2", 100.0, 5)], tolerant=True)
    pb.submit([ev("L1", "B", 101, 5), ev("L2", "S", 100, 5)])
    return pb


class TestFaultInjection(unittest.TestCase):
    def test_fails_only_on_the_chosen_statement(self):
        db = ob.OrderDatabase()
        self.addCleanup(db.close)
        db.conn = ob.FaultInjectingConnection(db.conn, fail_at=2)
        db.conn.execute("SELECT 1")
        with self.assertRaises(sqlite3.OperationalError):
            db.conn.execute("SELECT 2")
        db.conn.execute("SELECT 3")
        self.assertEqual(db.conn.statements, 3)

    def test_no_failure_when_not_asked(self):
        db = ob.OrderDatabase()
        self.addCleanup(db.close)
        db.conn = ob.FaultInjectingConnection(db.conn)
        for i in range(10):
            db.conn.execute("SELECT 1")


class TestEveryFailurePoint(unittest.TestCase):
    def sweep(self, make, operation):
        points, ok = ob.fault_sweep(make, operation)
        self.assertGreater(points, 0)
        self.assertTrue(ok)

    def test_saving_orders(self):
        self.sweep(fresh(), lambda pb: pb.submit(THREE))

    def test_saving_orders_with_quarantine(self):
        self.sweep(fresh(), lambda pb: pb.submit_tolerant(
            [THREE[0], None, THREE[1], {"order_id": "Z", "side": "B"}]))

    def test_cancelling(self):
        self.sweep(fresh(THREE), lambda pb: pb.cancel("N1"))

    def test_saving_trades(self):
        self.sweep(fresh(TRADE_SETUP), lambda pb: pb.record_trades([T1, T2]))

    def test_saving_trades_with_quarantine(self):
        self.sweep(fresh(TRADE_SETUP),
                   lambda pb: pb.record_trades_tolerant([T1, BAD, T2]))

    def test_running_the_engine(self):
        self.sweep(fresh(TRADE_SETUP), lambda pb: pb.run_engine())

    def test_retrying_quarantined_trades(self):
        self.sweep(late_state, lambda pb: pb.retry_rejected())

    def test_the_sweep_catches_an_operation_that_is_not_all_or_nothing(self):
        def two_commits(pb):
            pb.db.save_orders([ob.Order("X1", "B", 1.0, 1)])
            pb.db.save_orders([ob.Order("X2", "B", 1.0, 1)])
        points, ok = ob.fault_sweep(fresh(), two_commits)
        self.assertGreater(points, 0)
        self.assertFalse(ok)


class TestFailureAfterTheCommit(unittest.TestCase):
    def test_book_cleanup_failure_is_flagged_and_repaired(self):
        make = fresh(TRADE_SETUP)
        probe = make()
        counter = ob.FaultInjectingConnection(probe.db.conn)
        probe.db.conn = counter
        probe.record_trades([T1])
        last = counter.statements              # the last statement is the cleanup read
        probe.db.conn = counter._conn
        probe.db.close()

        pb = make()
        self.addCleanup(pb.db.close)
        pb.db.conn = ob.FaultInjectingConnection(pb.db.conn, fail_at=last)
        self.assertEqual(pb.record_trades([T1]), 1)       # the save itself succeeded
        pb.db.conn = pb.db.conn._conn
        self.assertTrue(pb.needs_reconcile)
        self.assertIsNotNone(pb.book.get_order("S1"))     # filled order still resting
        self.assertEqual(pb.db.stats()["trades"], 1)

        pb.run_engine()                                   # repairs first, then matches
        self.assertFalse(pb.needs_reconcile)
        self.assertIsNone(pb.book.get_order("S1"))
        self.assertEqual(pb.db.stats()["trades"], 2)
        self.assertTrue(ob.validate_trade_persistence(pb.db)["ok"])


class TestBackupAndRecovery(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.live = os.path.join(self.dir.name, "live.db")
        self.backup = os.path.join(self.dir.name, "backup.db")
        pb = ob.PersistentBook(ob.OrderDatabase(self.live))
        pb.submit(TRADE_SETUP)
        pb.run_engine()
        self.state = ob._persistence_snapshot(pb)
        pb.db.close()

    def damage(self):
        with open(self.live, "r+b") as handle:
            handle.write(b"\x00garbage-garbage-garbage\x00" * 8)
        for suffix in ("-wal", "-shm"):
            if os.path.exists(self.live + suffix):
                os.remove(self.live + suffix)

    def test_backup_is_a_healthy_copy(self):
        ob.backup_database(self.live, self.backup)
        self.assertEqual(ob.database_health(self.backup), (True, "ok"))
        db = ob.OrderDatabase(self.backup)
        self.addCleanup(db.close)
        self.assertEqual(db.stats()["trades"], 2)

    def test_backup_never_overwrites(self):
        ob.backup_database(self.live, self.backup)
        with self.assertRaises(FileExistsError):
            ob.backup_database(self.live, self.backup)

    def test_health_check(self):
        self.assertEqual(ob.database_health(self.live), (True, "ok"))
        self.assertFalse(ob.database_health(os.path.join(self.dir.name, "none.db"))[0])
        self.damage()
        healthy, message = ob.database_health(self.live)
        self.assertFalse(healthy)
        self.assertTrue(message)

    def test_a_healthy_database_is_left_alone(self):
        ob.backup_database(self.live, self.backup)
        self.assertEqual(ob.recover_database(self.live, self.backup)["action"], "none")

    def test_recovery_restores_the_backup_and_keeps_the_damaged_file(self):
        ob.backup_database(self.live, self.backup)
        self.damage()
        result = ob.recover_database(self.live, self.backup)
        self.assertEqual(result["action"], "restored")
        self.assertTrue(os.path.exists(result["damaged_file_kept_as"]))
        back = ob.PersistentBook.restore(ob.OrderDatabase(self.live))
        self.addCleanup(back.db.close)
        self.assertEqual(ob._persistence_snapshot(back), self.state)
        self.assertTrue(ob.validate_trade_persistence(back.db)["ok"])

    def test_recovery_needs_a_backup(self):
        self.damage()
        with self.assertRaises(FileNotFoundError):
            ob.recover_database(self.live, self.backup)

    def test_a_damaged_backup_is_refused(self):
        ob.backup_database(self.live, self.backup)
        with open(self.backup, "r+b") as handle:
            handle.write(b"\x00junk" * 40)
        self.damage()
        with self.assertRaises(ValueError):
            ob.recover_database(self.live, self.backup)
        self.assertFalse(ob.database_health(self.live)[0])      # nothing was replaced


class TestRealCrashes(unittest.TestCase):
    def test_uncommitted_rows_vanish_and_committed_rows_stay(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "t.db")
            pb = ob.PersistentBook(ob.OrderDatabase(path))
            pb.submit(TRADE_SETUP)
            pb.db.close()
            r = ob.kill_during_open_transaction(path)
        self.assertTrue(r["ready"])
        self.assertEqual(r["uncommitted_rows_visible"], 0)
        self.assertEqual(r["rows_before"], r["rows_after"])
        self.assertEqual(r["rows_after"], 3)
        self.assertTrue(r["healthy"] and r["valid"])

    def test_the_whole_suite(self):
        r = ob.run_resilience_suite(1500, rounds=5, crashes=3)
        failed = [k for k, v in r["checks"].items() if not v]
        print("\n" + ob.format_resilience_report(r))
        self.assertEqual(failed, [])
        self.assertGreaterEqual(r["interrupted"], 1)


class TestOtherFailures(unittest.TestCase):
    def test_read_only_database_refuses_writes_and_the_book_rolls_back(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "ro.db")
            ob.PersistentBook(ob.OrderDatabase(path)).db.close()
            ro = ob.PersistentBook(ob.OrderDatabase(path, read_only=True))
            try:
                with self.assertRaises(sqlite3.OperationalError):
                    ro.submit(THREE)
                self.assertEqual(len(ro.book), 0)
            finally:
                ro.db.close()

    def test_delivering_the_same_events_again_changes_nothing(self):
        pb = ob.PersistentBook(ob.OrderDatabase())
        self.addCleanup(pb.db.close)
        pb.submit_tolerant(THREE, quarantine=False)
        once = (ob._db_dump(pb.db), pb.book.get_orders_in_arrival_order())
        for _ in range(3):
            self.assertEqual(pb.submit_tolerant(THREE, quarantine=False)["accepted"], 0)
        self.assertEqual((ob._db_dump(pb.db), pb.book.get_orders_in_arrival_order()), once)


if __name__ == "__main__":
    unittest.main(verbosity=2)