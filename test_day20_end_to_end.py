
import unittest

from orderbook import (
    run_end_to_end_validation,
    format_end_to_end_report
)


class TestDay20EndToEnd(unittest.TestCase):

    def test_complete_validation_returns_result(self):
        result = run_end_to_end_validation(
            orders=100,
            crashes=1,
            include_tests=False
        )

        self.assertIsInstance(result, dict)
        self.assertIn("ok", result)
        self.assertIn("checks", result)
        self.assertIn("details", result)

    def test_all_pipeline_stages_are_checked(self):
        result = run_end_to_end_validation(
            orders=100,
            crashes=1,
            include_tests=False
        )

        expected = [
            "Order book and load validation",
            "IPC validation",
            "Zero-copy validation",
            "Database validation",
            "Persistence validation",
            "Failure handling validation",
            "Recovery and resilience validation"
        ]

        for name in expected:
            with self.subTest(stage=name):
                self.assertIn(name, result["checks"])

    def test_final_status_matches_checks(self):
        result = run_end_to_end_validation(
            orders=100,
            crashes=1,
            include_tests=False
        )

        expected = (
            all(result["checks"].values())
            and bool(result["checks"])
        )

        self.assertEqual(result["ok"], expected)

    def test_report_contains_all_results(self):
        result = run_end_to_end_validation(
            orders=100,
            crashes=1,
            include_tests=False
        )

        report = format_end_to_end_report(result)

        self.assertIn("CHRONOSMATCH", report)
        self.assertIn("DAY 20", report)
        self.assertIn("FINAL STATUS", report)

    def test_report_shows_failed_validation(self):
        report = {
            "ok": False,
            "orders": 100,
            "crashes": 1,
            "checks": {
                "Database validation": True,
                "Recovery and resilience validation": False
            },
            "details": {
                "Recovery and resilience validation": "Recovery failed"
            }
        }

        output = format_end_to_end_report(report)

        self.assertIn("FAIL", output)
        self.assertIn("Recovery and resilience validation", output)

    def test_invalid_order_count(self):
        with self.assertRaises(ValueError):
            run_end_to_end_validation(
                orders=0,
                crashes=1,
                include_tests=False
            )

    def test_negative_crash_count(self):
        with self.assertRaises(ValueError):
            run_end_to_end_validation(
                orders=100,
                crashes=-1,
                include_tests=False
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
