
import os
import tempfile
import unittest

from orderbook import (
    OrderDatabase,
    PersistentBook,
    backup_database,
    database_health,
    recover_database,
    run_resilience_suite,
    kill_during_open_transaction,
    validate_trade_persistence,
    BUY,
    SELL,
)


class TestDay19Resilience(unittest.TestCase):

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.folder = self.temp.name

    def tearDown(self):
        self.temp.cleanup()

    # Test 1: Database health check
    def test_database_health(self):
        path = os.path.join(self.folder, "health.db")

        self.assertFalse(database_health(path)[0])

        db = OrderDatabase(path)
        db.close()

        healthy, message = database_health(path)

        self.assertTrue(healthy, message)

    # Test 2: Backup database
    def test_database_backup(self):
        source = os.path.join(self.folder, "source.db")
        backup = os.path.join(self.folder, "backup.db")

        pb = PersistentBook(OrderDatabase(source))

        pb.submit([
            {
                "order_id": "B001",
                "side": BUY,
                "price": 105.0,
                "quantity": 10
            },
            {
                "order_id": "S001",
                "side": SELL,
                "price": 100.0,
                "quantity": 5
            }
        ])

        pb.db.close()

        backup_database(source, backup)

        self.assertTrue(database_health(source)[0])
        self.assertTrue(database_health(backup)[0])

    # Test 3: Restore database from backup
    def test_restore_damaged_database(self):
        source = os.path.join(self.folder, "live.db")
        backup = os.path.join(self.folder, "backup.db")

        pb = PersistentBook(OrderDatabase(source))

        pb.submit([
            {
                "order_id": "B002",
                "side": BUY,
                "price": 110.0,
                "quantity": 20
            }
        ])

        pb.db.close()

        backup_database(source, backup)

        # Simulate database file damage.
        with open(source, "r+b") as file:
            file.write(b"\x00DAMAGED_DATABASE_HEADER")

        self.assertFalse(database_health(source)[0])

        result = recover_database(source, backup)

        self.assertEqual(result["action"], "restored")
        self.assertTrue(os.path.exists(result["damaged_file_kept_as"]))
        self.assertTrue(database_health(source)[0])

        restored_db = OrderDatabase(source)

        try:
            restored_book = PersistentBook.restore(restored_db)

            self.assertEqual(len(restored_book.book), 1)
            self.assertIsNotNone(
                restored_book.book.get_order("B002")
            )

            validation = validate_trade_persistence(
                restored_db,
                expect_matched=False
            )
            self.assertTrue(validation["ok"], validation["problems"])

        finally:
            restored_db.close()

    # Test 4: Kill process during an uncommitted transaction
    def test_crash_during_transaction(self):
        path = os.path.join(self.folder, "transaction.db")

        db = OrderDatabase(path)
        db.close()

        result = kill_during_open_transaction(path)

        self.assertTrue(result["ready"])
        self.assertEqual(
            result["rows_before"],
            result["rows_after"]
        )
        self.assertEqual(result["uncommitted_rows_visible"], 0)
        self.assertTrue(result["healthy"])
        self.assertTrue(result["valid"])

    # Test 5: Replaying the same events
    def test_duplicate_event_replay(self):
        pb = PersistentBook(OrderDatabase())

        events = [
            {
                "order_id": "REPLAY001",
                "side": BUY,
                "price": 105.0,
                "quantity": 10
            }
        ]

        try:
            first = pb.submit_tolerant(
                events,
                quarantine=False
            )

            second = pb.submit_tolerant(
                events,
                quarantine=False
            )

            self.assertEqual(first["accepted"], 1)
            self.assertEqual(second["accepted"], 0)
            self.assertEqual(len(pb.book), 1)
            self.assertEqual(pb.db.stats()["orders"], 1)

        finally:
            pb.db.close()

    # Test 6: Complete Day 19 resilience suite
    def test_complete_resilience_suite(self):
        result = run_resilience_suite(
            n_orders=100,
            rounds=4,
            crashes=2,
            seed=19
        )

        for name, passed in result["checks"].items():
            with self.subTest(check=name):
                self.assertTrue(passed, name)

        self.assertTrue(result["ok"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
