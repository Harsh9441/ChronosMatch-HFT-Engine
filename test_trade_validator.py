"""Day 8 (Teammate 3): tests for the trade validator."""

import random
import time
import unittest

from orderbook import BUY, SELL, Order
from trade_validator import (
    BUY_ID, PRICE, QUANTITY, SELL_ID, TRADE_ID,
    assert_valid_trades, reference_match, summarize, validate_trades,
)

# A small book with a known answer:
#   sells: S1 5@100, S2 5@101, S3 5@103      buys: B1 6@99, B2 8@102
# B2 should trade 5 with S1 @100, then 3 with S2 @101. B1 and S3 wait.
ORDERS = [
    Order("S1", SELL, 100.0, 5),
    Order("S2", SELL, 101.0, 5),
    Order("S3", SELL, 103.0, 5),
    Order("B1", BUY, 99.0, 6),
    Order("B2", BUY, 102.0, 8),
]


def good_trades():
    return [
        {TRADE_ID: "T1", BUY_ID: "B2", SELL_ID: "S1", PRICE: 100.0, QUANTITY: 5},
        {TRADE_ID: "T2", BUY_ID: "B2", SELL_ID: "S2", PRICE: 101.0, QUANTITY: 3},
    ]


def random_orders(count, seed, low=9990, high=10010):
    rng = random.Random(seed)
    return [
        Order(str(i), rng.choice([BUY, SELL]), float(rng.randint(low, high)),
              rng.randint(1, 50))
        for i in range(1, count + 1)
    ]


class TestValidTrades(unittest.TestCase):
    def test_known_example_is_valid(self):
        self.assertEqual(validate_trades(ORDERS, good_trades(), price_rule="resting"), [])

    def test_reference_matcher_agrees_with_known_example(self):
        self.assertEqual(reference_match(ORDERS), good_trades())

    def test_no_trades_on_uncrossed_book_is_valid(self):
        orders = [Order("S1", SELL, 105.0, 5), Order("B1", BUY, 100.0, 5)]
        self.assertEqual(validate_trades(orders, []), [])

    def test_empty_everything_is_valid(self):
        self.assertEqual(validate_trades([], []), [])

    def test_assert_valid_trades_passes_quietly(self):
        assert_valid_trades(ORDERS, good_trades())

    def test_random_books_with_many_crossings(self):
        for seed in range(25):
            orders = random_orders(300, seed)
            trades = reference_match(orders)
            self.assertGreater(len(trades), 0)
            self.assertEqual(
                validate_trades(orders, trades, price_rule="resting"), [],
                f"seed {seed}",
            )

    def test_summarize_numbers(self):
        s = summarize(ORDERS, good_trades())
        self.assertEqual(s["trades"], 2)
        self.assertEqual(s["quantity_traded"], 8)
        self.assertEqual(s["fully_filled_orders"], 2)    # S1 and B2
        self.assertEqual(s["partly_filled_orders"], 1)   # S2 has 2 left
        self.assertEqual(s["unfilled_orders"], 2)        # S3 and B1


class TestDetectsFaults(unittest.TestCase):
    def assertCaught(self, trades, text, mode="batch", **options):
        errors = validate_trades(ORDERS, trades, mode=mode, **options)
        self.assertTrue(
            any(text in e for e in errors),
            f"expected a problem containing {text!r}, got {errors}",
        )

    def test_unknown_buy_order(self):
        t = good_trades(); t[0][BUY_ID] = "NOPE"
        self.assertCaught(t, "unknown buy order")

    def test_unknown_sell_order(self):
        t = good_trades(); t[0][SELL_ID] = "NOPE"
        self.assertCaught(t, "unknown sell order")

    def test_buy_and_sell_swapped(self):
        t = good_trades(); t[0][BUY_ID], t[0][SELL_ID] = "S1", "B2"
        self.assertCaught(t, "is not a buy order")
        self.assertCaught(t, "is not a sell order")

    def test_prices_do_not_cross(self):
        t = good_trades(); t[0][BUY_ID] = "B1"            # 99 vs 100
        self.assertCaught(t, "does not cross")

    def test_price_above_buy_limit(self):
        t = good_trades(); t[0][PRICE] = 104.0
        self.assertCaught(t, "outside limit prices")

    def test_price_below_sell_limit(self):
        t = good_trades(); t[0][PRICE] = 99.5
        self.assertCaught(t, "outside limit prices")

    def test_overfill_single_trade(self):
        t = good_trades(); t[0][QUANTITY] = 6              # S1 only has 5
        self.assertCaught(t, "overfill")

    def test_overfill_across_trades(self):
        t = good_trades()
        t.append({TRADE_ID: "T3", BUY_ID: "B2", SELL_ID: "S3", PRICE: 102.0, QUANTITY: 1})
        self.assertCaught(t, "overfill")                   # B2 already used 8

    def test_zero_quantity(self):
        t = good_trades(); t[0][QUANTITY] = 0
        self.assertCaught(t, "quantity must be")

    def test_fractional_quantity(self):
        t = good_trades(); t[0][QUANTITY] = 2.5
        self.assertCaught(t, "quantity must be")

    def test_negative_price(self):
        t = good_trades(); t[0][PRICE] = -1
        self.assertCaught(t, "price must be")

    def test_missing_field(self):
        t = good_trades(); del t[0][QUANTITY]
        self.assertCaught(t, "missing field 'quantity'")

    def test_trade_is_not_a_dictionary(self):
        self.assertCaught(["T1"], "must be a dictionary")

    def test_duplicate_trade_id(self):
        t = good_trades(); t[1][TRADE_ID] = "T1"
        self.assertCaught(t, "duplicate trade id")

    def test_missed_match_when_a_trade_is_dropped(self):
        t = good_trades()[:1]                              # B2 still has 3 left
        self.assertCaught(t, "Missed match", mode="continuous")
        self.assertCaught(t, "Missed match", mode="batch")

    def test_no_trades_on_crossed_book(self):
        self.assertCaught([], "Missed match", mode="continuous")

    def test_priority_violation_skips_cheaper_sell(self):
        t = [
            {TRADE_ID: "T1", BUY_ID: "B2", SELL_ID: "S2", PRICE: 101.0, QUANTITY: 5},
            {TRADE_ID: "T2", BUY_ID: "B2", SELL_ID: "S1", PRICE: 100.0, QUANTITY: 3},
        ]
        self.assertCaught(t, "priority violated", mode="batch")
        # basic mode does not look at priority, so it stays quiet
        self.assertEqual(validate_trades(ORDERS, t, mode="basic"), [])

    def test_time_priority_same_price(self):
        orders = [
            Order("B1", BUY, 100.0, 5),     # arrived first
            Order("B2", BUY, 100.0, 5),
            Order("S1", SELL, 100.0, 5),
        ]
        wrong = [{BUY_ID: "B2", SELL_ID: "S1", PRICE: 100.0, QUANTITY: 5}]
        right = [{BUY_ID: "B1", SELL_ID: "S1", PRICE: 100.0, QUANTITY: 5}]
        self.assertTrue(any("priority violated" in e
                            for e in validate_trades(orders, wrong)))
        self.assertEqual(validate_trades(orders, right), [])

    def test_resting_price_rule(self):
        t = good_trades(); t[0][PRICE] = 101.0             # S1 arrived first: must be 100
        self.assertEqual(validate_trades(ORDERS, t, mode="basic"), [])
        self.assertCaught(t, "resting price", mode="basic", price_rule="resting")

    def test_assert_valid_trades_raises(self):
        t = good_trades(); t[0][QUANTITY] = 99
        with self.assertRaises(AssertionError):
            assert_valid_trades(ORDERS, t)

    def test_bad_options(self):
        with self.assertRaises(ValueError):
            validate_trades(ORDERS, [], mode="fast")
        with self.assertRaises(ValueError):
            validate_trades(ORDERS, [], price_rule="average")

    def test_duplicate_order_in_input_is_reported(self):
        orders = ORDERS + [Order("S1", SELL, 100.0, 5)]
        errors = validate_trades(orders, [])
        self.assertTrue(any("duplicate order id" in e for e in errors))


class TestRandomFaults(unittest.TestCase):
    def test_every_random_corruption_is_caught(self):
        for seed in range(25):
            orders = random_orders(300, seed)
            trades = reference_match(orders)
            rng = random.Random(seed)

            bigger = [dict(t) for t in trades]
            bigger[rng.randrange(len(bigger))][QUANTITY] += 10_000
            self.assertTrue(validate_trades(orders, bigger), f"overfill, seed {seed}")

            dropped = [dict(t) for t in trades]
            del dropped[rng.randrange(len(dropped))]
            self.assertTrue(validate_trades(orders, dropped), f"dropped, seed {seed}")

            shuffled = [dict(t) for t in trades]
            if len(shuffled) > 3:
                shuffled[0], shuffled[-1] = shuffled[-1], shuffled[0]
                self.assertTrue(validate_trades(orders, shuffled), f"order, seed {seed}")


class TestSpeed(unittest.TestCase):
    def test_validate_20000_orders(self):
        orders = random_orders(20000, seed=7)
        trades = reference_match(orders)
        start = time.perf_counter()
        errors = validate_trades(orders, trades, price_rule="resting")
        seconds = time.perf_counter() - start
        self.assertEqual(errors, [])
        self.assertLess(seconds, 5)
        print(f"\n   validated {len(trades):,} trades from 20,000 orders in {seconds:.2f} s")


if __name__ == "__main__":
    unittest.main(verbosity=2)