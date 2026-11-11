"""Day 20: end-to-end validation."""

import os
import tempfile
import unittest

import orderbook as ob


def failed(result):
    return [name for name, ok in result["checks"].items() if not ok]


class TestEndToEnd(unittest.TestCase):
    def test_default_run(self):
        r = ob.run_end_to_end_validation(3000, batch=500)
        self.assertEqual(failed(r), [])
        self.assertEqual(r["batches"], 6)
        self.assertEqual(r["nacks"], 1)                # every 4th batch is damaged once
        self.assertEqual(r["duplicates"], 1)
        self.assertGreater(r["trades"], 0)

    def test_no_damage(self):
        r = ob.run_end_to_end_validation(2000, batch=500, corrupt_every=0)
        self.assertEqual(failed(r), [])
        self.assertEqual(r["nacks"], 0)

    def test_every_batch_damaged_once(self):
        r = ob.run_end_to_end_validation(2000, batch=500, corrupt_every=1)
        self.assertEqual(failed(r), [])
        self.assertEqual(r["nacks"], r["batches"])

    def test_no_duplicate_delivery(self):
        r = ob.run_end_to_end_validation(2000, batch=500, duplicate_seq=-1)
        self.assertEqual(failed(r), [])
        self.assertEqual(r["duplicates"], 0)

    def test_one_batch_only(self):
        r = ob.run_end_to_end_validation(500, batch=500)
        self.assertEqual(failed(r), [])
        self.assertEqual(r["batches"], 1)

    def test_last_batch_is_smaller(self):
        r = ob.run_end_to_end_validation(2750, batch=1000)
        self.assertEqual(failed(r), [])
        self.assertEqual(r["batches"], 3)

    def test_export_folder_is_kept_and_verifies(self):
        with tempfile.TemporaryDirectory() as d:
            out = os.path.join(d, "kept")
            r = ob.run_end_to_end_validation(2000, batch=500, export_dir=out)
            self.assertEqual(failed(r), [])
            self.assertTrue(os.path.exists(os.path.join(out, "manifest.json")))
            self.assertTrue(ob.verify_export(out)["ok"])
            self.assertEqual(r["export"]["dir"], out)

    def test_bad_arguments(self):
        with self.assertRaises(ValueError):
            ob.run_end_to_end_validation(0)
        with self.assertRaises(ValueError):
            ob.run_end_to_end_validation(100, batch=0)

    def test_report_text(self):
        r = ob.run_end_to_end_validation(1000, batch=500)
        text = ob.format_end_to_end_report(r)
        self.assertIn("ALL CHECKS PASSED", text)
        self.assertIn("separate producer process", text)

    def test_20000_orders(self):
        r = ob.run_end_to_end_validation(20_000)
        print("\n" + ob.format_end_to_end_report(r))
        self.assertEqual(failed(r), [])
        self.assertEqual(r["batches"], 20)


if __name__ == "__main__":
    unittest.main(verbosity=2)