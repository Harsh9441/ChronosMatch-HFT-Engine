"""Day 17: validate trade persistence."""

import os
import tempfile
import unittest

import orderbook as ob


def ev(oid, side, price, qty):
    return {"order_id": oid, "side": side, "price": price, "quantity": qty}


class Base(unittest.TestCase):
    def setUp(self):
        self.db = ob.OrderDatabase()
        self.addCleanup(self.db.close)
        self.pb = ob.PersistentBook(self.db)

    def matched_book(self):
        self.pb.submit([ev("B1", "B", 101, 10), ev("S1", "S", 100, 4),
                        ev("S2", "S", 101, 8)])
        self.assertEqual(self.pb.run_engine(), 2)

    def tamper(self, sql, args=()):
        with self.db.conn:
            self.db.conn.execute(sql, args)

    def failed(self, **kw):
        result = ob.validate_trade_persistence(self.db, **kw)
        return [name for name, ok in result["checks"].items() if not ok]


class TestHealthyData(Base):
    def test_matched_book_is_valid(self):
        self.matched_book()
        result = ob.validate_trade_persistence(self.db)
        self.assertTrue(result["ok"], result["problems"])
        self.assertEqual((result["orders"], result["trades"]), (3, 2))

    def test_empty_database_is_valid(self):
        self.assertTrue(ob.validate_trade_persistence(self.db)["ok"])

    def test_partly_filled_then_cancelled_order_is_valid(self):
        self.pb.submit([ev("B1", "B", 101, 10), ev("S1", "S", 100, 4)])
        self.pb.run_engine()
        self.assertTrue(self.pb.cancel("B1"))                 # 4 filled, 6 cancelled
        result = ob.validate_trade_persistence(self.db)
        self.assertTrue(result["ok"], result["problems"])

    def test_validation_does_not_change_the_data(self):
        self.matched_book()
        before = self.db.stats()
        ob.validate_trade_persistence(self.db)
        self.assertEqual(self.db.stats(), before)


class TestValidatorCatchesDamage(Base):
    def test_damaged_fill(self):
        self.matched_book()
        self.tamper("UPDATE orders SET filled = filled - 1 WHERE order_id = 'B1'")
        self.assertIn("database file and order fills are consistent", self.failed())

    def test_trade_price_outside_the_limits(self):
        self.matched_book()
        self.tamper("INSERT INTO trades (trade_id, buy_order_id, sell_order_id, "
                    "price, quantity) VALUES ('X1', 'B1', 'S1', 500, 1)")
        self.assertIn("every stored trade passes the trade rules", self.failed())

    def test_trade_after_its_order_was_cancelled(self):
        self.pb.submit([ev("B1", "B", 101, 10), ev("S1", "S", 100, 4)])
        self.pb.cancel("B1")
        self.tamper("INSERT INTO trades (trade_id, buy_order_id, sell_order_id, "
                    "price, quantity) VALUES ('X1', 'B1', 'S1', 100, 1)")
        self.tamper("INSERT INTO events (kind, ref) VALUES ('trade', 'X1')")
        self.assertIn("no trade happens after its order was cancelled", self.failed())

    def test_trade_older_than_its_order(self):
        self.matched_book()
        self.tamper("UPDATE events SET seq = seq + 100000 "
                    "WHERE kind = 'order' AND ref = 'S1'")
        self.assertIn("every trade comes after both of its orders", self.failed())

    def test_missing_event_log_row(self):
        self.matched_book()
        self.tamper("DELETE FROM events WHERE seq = "
                    "(SELECT MAX(seq) FROM events WHERE kind = 'trade')")
        self.assertIn("event log matches the tables", self.failed())

    def test_unbalanced_fills(self):
        self.matched_book()
        # change both the order and a trade so the first check still passes
        self.tamper("UPDATE orders SET filled = filled + 1 WHERE order_id = 'S2'")
        self.tamper("UPDATE trades SET quantity = quantity + 1 "
                    "WHERE sell_order_id = 'S2'")
        names = self.failed()
        self.assertIn("buyer and seller fills balance", names)

    def test_resting_orders_that_should_have_matched(self):
        self.pb.submit([ev("B1", "B", 101, 10), ev("S1", "S", 100, 4)])   # no engine run
        self.assertIn("resting orders do not cross", self.failed())
        self.assertEqual(self.failed(expect_matched=False), [])


class TestRestartInvariance(unittest.TestCase):
    def test_restarts_change_nothing(self):
        r = ob.run_persistence_validation(5000, rounds=5)
        self.assertTrue(r["ok"], [k for k, v in r["checks"].items() if not v] + r["problems"])
        self.assertEqual(r["restarts"], 5)                     # 4 between rounds + 1 at the end
        self.assertGreater(r["trades"], 0)

    def test_single_round(self):
        r = ob.run_persistence_validation(2000, rounds=1)
        self.assertTrue(r["ok"])
        self.assertEqual(r["restarts"], 1)

    def test_uneven_rounds(self):
        r = ob.run_persistence_validation(1001, rounds=7)
        self.assertTrue(r["ok"], r["problems"])

    def test_same_input_gives_same_trade_count(self):
        a = ob.run_persistence_validation(3000, rounds=3)
        b = ob.run_persistence_validation(3000, rounds=3)
        self.assertEqual(a["trades"], b["trades"])

    def test_bad_arguments(self):
        with self.assertRaises(ValueError):
            ob.run_persistence_validation(0)
        with self.assertRaises(ValueError):
            ob.run_persistence_validation(100, rounds=0)

    def test_existing_file_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "x.db")
            open(path, "w").close()
            with self.assertRaises(FileExistsError):
                ob.run_persistence_validation(100, path)

    def test_database_file_validates_after_the_run(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "kept.db")
            r = ob.run_persistence_validation(3000, path, rounds=4)
            self.assertTrue(r["ok"])
            db = ob.OrderDatabase(path)
            try:
                again = ob.validate_trade_persistence(db)
                self.assertTrue(again["ok"], again["problems"])
                self.assertEqual(again["trades"], r["trades"])
            finally:
                db.close()


class TestScale(unittest.TestCase):
    def test_50000_orders_ten_rounds(self):
        r = ob.run_persistence_validation(50_000, rounds=10)
        print("\n" + ob.format_persistence_report(r))
        self.assertTrue(r["ok"], [k for k, v in r["checks"].items() if not v])
        self.assertLess(r["restart_s"], 120)


if __name__ == "__main__":
    unittest.main(verbosity=2)