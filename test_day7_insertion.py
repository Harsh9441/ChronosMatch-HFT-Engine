"""Day 7 (Teammate 3): tests for order insertion."""

import asyncio
import dataclasses
import random
import time
import unittest

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


class InsertTestCase(unittest.TestCase):
    def setUp(self):
        self.book = OrderBook()

    def assertRejected(self, order):
        """The order must raise ValueError and leave the book untouched."""
        before = (len(self.book), self.book.get_all_orders())
        with self.assertRaises(ValueError):
            self.book.add_order(order)
        self.assertEqual((len(self.book), self.book.get_all_orders()), before)

    def assertConsistent(self):
        """Every stored order is found by id, and counts add up."""
        orders = self.book.get_all_orders()
        self.assertEqual(len(orders), len(self.book))
        self.assertEqual(len({o.order_id for o in orders}), len(orders))
        for order in orders:
            self.assertIs(self.book.get_order(order.order_id), order)


class TestValidInsert(InsertTestCase):
    def test_buy_goes_to_buys_only(self):
        order = Order("B1", BUY, 100.0, 5)
        self.book.add_order(order)
        self.assertEqual(self.book.get_orders(BUY), [order])
        self.assertEqual(self.book.get_orders(SELL), [])
        self.assertEqual(len(self.book), 1)

    def test_sell_goes_to_sells_only(self):
        order = Order("S1", SELL, 101.0, 3)
        self.book.add_order(order)
        self.assertEqual(self.book.get_orders(SELL), [order])
        self.assertEqual(self.book.get_orders(BUY), [])

    def test_inserted_order_can_be_found_by_id(self):
        order = Order("B1", BUY, 100.0, 5)
        self.book.add_order(order)
        self.assertIs(self.book.get_order("B1"), order)

    def test_arrival_order_is_kept(self):
        ids = [f"O{i}" for i in range(20)]
        for oid in ids:
            self.book.add_order(Order(oid, BUY, 100.0, 1))   # same price
        self.assertEqual([o.order_id for o in self.book.get_orders(BUY)], ids)

    def test_integer_price_is_accepted(self):
        self.book.add_order(Order("B1", BUY, 100, 5))
        self.assertEqual(len(self.book), 1)

    def test_smallest_valid_values(self):
        self.book.add_order(Order("B1", BUY, 0.0001, 1))
        self.assertEqual(len(self.book), 1)

    def test_very_large_values(self):
        self.book.add_order(Order("B1", BUY, 10**12, 10**12))
        self.assertEqual(self.book.get_order("B1").quantity, 10**12)

    def test_unusual_but_valid_ids(self):
        for oid in ("A", "order-1", "ORD_001", "id with spaces", "日本語", "x" * 500, "007"):
            self.book.add_order(Order(oid, SELL, 100.0, 1))
        self.assertEqual(len(self.book), 7)
        self.assertConsistent()

    def test_ids_are_case_sensitive_and_space_sensitive(self):
        self.book.add_order(Order("a1", BUY, 100.0, 1))
        self.book.add_order(Order("A1", BUY, 100.0, 1))
        self.book.add_order(Order(" A1", BUY, 100.0, 1))
        self.assertEqual(len(self.book), 3)

    def test_mixed_orders_counted_per_side(self):
        for i in range(30):
            self.book.add_order(Order(f"B{i}", BUY, 99.0, 1))
        for i in range(12):
            self.book.add_order(Order(f"S{i}", SELL, 101.0, 1))
        self.assertEqual(len(self.book.get_orders(BUY)), 30)
        self.assertEqual(len(self.book.get_orders(SELL)), 12)
        self.assertEqual(len(self.book), 42)
        self.assertConsistent()

    def test_stored_order_cannot_be_changed(self):
        order = Order("B1", BUY, 100.0, 5)
        self.book.add_order(order)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            order.quantity = 999

    def test_returned_lists_are_copies(self):
        self.book.add_order(Order("B1", BUY, 100.0, 5))
        self.book.get_orders(BUY).clear()
        self.book.get_all_orders().clear()
        self.assertEqual(len(self.book), 1)


class TestInvalidId(InsertTestCase):
    def test_empty_id(self):
        self.assertRejected(Order("", BUY, 100.0, 1))

    def test_blank_id(self):
        self.assertRejected(Order("   ", BUY, 100.0, 1))

    def test_numeric_id(self):
        self.assertRejected(Order(1, BUY, 100.0, 1))

    def test_none_id(self):
        self.assertRejected(Order(None, BUY, 100.0, 1))


class TestInvalidSide(InsertTestCase):
    def test_bad_sides(self):
        for side in ("X", "b", "s", "BUY", "B ", "", None, 0, 1, True):
            with self.subTest(side=side):
                self.book = OrderBook()
                self.assertRejected(Order("O1", side, 100.0, 1))


class TestInvalidPrice(InsertTestCase):
    def test_zero_and_negative(self):
        for price in (0, 0.0, -1, -0.01, float("-inf")):
            with self.subTest(price=price):
                self.book = OrderBook()
                self.assertRejected(Order("O1", BUY, price, 1))

    def test_not_a_number(self):
        for price in ("100", None, True, False, [100], (1,)):
            with self.subTest(price=price):
                self.book = OrderBook()
                self.assertRejected(Order("O1", BUY, price, 1))

    def test_nan_and_infinity(self):
        for price in (float("nan"), float("inf")):
            with self.subTest(price=price):
                self.book = OrderBook()
                self.assertRejected(Order("O1", BUY, price, 1))


class TestInvalidQuantity(InsertTestCase):
    def test_zero_and_negative(self):
        for qty in (0, -1, -100):
            with self.subTest(quantity=qty):
                self.book = OrderBook()
                self.assertRejected(Order("O1", BUY, 100.0, qty))

    def test_not_a_whole_number(self):
        for qty in (5.0, 2.5, "5", None, True, False, [5]):
            with self.subTest(quantity=qty):
                self.book = OrderBook()
                self.assertRejected(Order("O1", BUY, 100.0, qty))


class TestDuplicates(InsertTestCase):
    def test_same_side_duplicate(self):
        self.book.add_order(Order("O1", BUY, 100.0, 1))
        self.assertRejected(Order("O1", BUY, 101.0, 9))
        self.assertEqual(self.book.get_order("O1").price, 100.0)   # original kept

    def test_other_side_duplicate(self):
        self.book.add_order(Order("O1", BUY, 100.0, 1))
        self.assertRejected(Order("O1", SELL, 105.0, 1))

    def test_identical_order_twice(self):
        order = Order("O1", BUY, 100.0, 1)
        self.book.add_order(order)
        self.assertRejected(order)

    def test_id_can_be_reused_after_cancel(self):
        self.book.add_order(Order("O1", BUY, 100.0, 1))
        self.assertTrue(self.book.cancel_order("O1"))
        self.book.add_order(Order("O1", SELL, 105.0, 2))
        self.assertEqual(self.book.get_order("O1").side, SELL)
        self.assertConsistent()


class TestRejectedOrdersLeaveNoTrace(InsertTestCase):
    def test_rejected_order_does_not_reserve_its_id(self):
        self.assertRejected(Order("O1", BUY, -5, 1))               # bad price
        self.book.add_order(Order("O1", BUY, 100.0, 1))             # same id now fine
        self.assertEqual(len(self.book), 1)

    def test_good_orders_survive_nearby_bad_ones(self):
        self.book.add_order(Order("G1", BUY, 100.0, 1))
        self.assertRejected(Order("X1", "Q", 100.0, 1))
        self.book.add_order(Order("G2", SELL, 101.0, 1))
        self.assertRejected(Order("G1", BUY, 100.0, 1))
        self.assertEqual(len(self.book), 2)
        self.assertConsistent()

    def test_best_prices_ignore_rejected_orders(self):
        self.book.add_order(Order("B1", BUY, 100.0, 1))
        self.book.add_order(Order("S1", SELL, 105.0, 1))
        for bad in (Order("B2", BUY, float("nan"), 1), Order("S2", SELL, float("inf"), 1)):
            with self.assertRaises(ValueError):
                self.book.add_order(bad)
        self.assertEqual(self.book.best_bid().order_id, "B1")
        self.assertEqual(self.book.best_ask().order_id, "S1")


class TestFirehoseInsert(InsertTestCase):
    def test_all_valid_events(self):
        events = make_events(100)
        self.assertEqual(self.book.ingest_firehose(events), 100)
        self.assertEqual(len(self.book), 100)
        self.assertConsistent()

    def test_event_fields_are_stored_exactly(self):
        event = {"order_id": "E1", "side": SELL, "price": 101, "quantity": 7}
        self.book.ingest_firehose([event])
        self.assertEqual(self.book.get_order("E1"), Order("E1", SELL, 101.0, 7))
        self.assertIsInstance(self.book.get_order("E1").price, float)

    def test_extra_fields_are_ignored(self):
        event = {"order_id": "E1", "side": BUY, "price": 1, "quantity": 1,
                 "timestamp": 123, "order_type": 0}
        self.assertEqual(self.book.ingest_firehose([event]), 1)

    def test_generator_input(self):
        self.assertEqual(self.book.ingest_firehose(e for e in make_events(10)), 10)

    def test_missing_each_field(self):
        for field in ("order_id", "side", "price", "quantity"):
            with self.subTest(field=field):
                event = {"order_id": "E1", "side": BUY, "price": 1, "quantity": 1}
                del event[field]
                with self.assertRaisesRegex(ValueError, f"missing field '{field}'"):
                    OrderBook().ingest_firehose([event])

    def test_wrong_types(self):
        good = {"order_id": "E1", "side": BUY, "price": 1, "quantity": 1}
        bad_values = {
            "order_id": 7, "side": 1, "price": "1", "quantity": 1.5,
        }
        for field, value in bad_values.items():
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    OrderBook().ingest_firehose([{**good, field: value}])

    def test_bool_values_are_rejected(self):
        good = {"order_id": "E1", "side": BUY, "price": 1, "quantity": 1}
        for field in ("price", "quantity"):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    OrderBook().ingest_firehose([{**good, field: True}])

    def test_event_that_is_not_a_mapping(self):
        for bad in ("text", 5, None, ["E1", BUY, 1, 1], ("E1", BUY, 1, 1)):
            with self.subTest(event=bad):
                with self.assertRaises(TypeError):
                    OrderBook().ingest_firehose([bad])

    def test_error_names_the_event_number(self):
        events = make_events(5)
        events[2]["quantity"] = 0
        with self.assertRaisesRegex(ValueError, "event 3"):
            self.book.ingest_firehose(events)

    def test_events_before_a_bad_one_stay_in_the_book(self):
        events = make_events(5)
        events[3]["side"] = "Z"
        with self.assertRaises(ValueError):
            self.book.ingest_firehose(events)
        self.assertEqual(len(self.book), 3)            # events 1-3 were stored
        self.assertIsNone(self.book.get_order("4"))
        self.assertIsNone(self.book.get_order("5"))    # reading stopped at the bad one

    def test_duplicate_inside_one_batch(self):
        events = make_events(3)
        events.append(dict(events[0]))
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            self.book.ingest_firehose(events)

    def test_duplicate_across_two_batches(self):
        self.book.ingest_firehose(make_events(5))
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            self.book.ingest_firehose(make_events(1))   # id "1" again

    def test_two_batches_with_new_ids(self):
        self.assertEqual(self.book.ingest_firehose(make_events(5)), 5)
        self.assertEqual(self.book.ingest_firehose(make_events(5, start=6)), 5)
        self.assertEqual(len(self.book), 10)

    def test_empty_batch(self):
        self.assertEqual(self.book.ingest_firehose([]), 0)

    def test_nan_price_event(self):
        event = {"order_id": "E1", "side": BUY, "price": float("nan"), "quantity": 1}
        with self.assertRaises(ValueError):
            self.book.ingest_firehose([event])
        self.assertEqual(len(self.book), 0)


class TestAsyncInsert(unittest.IsolatedAsyncioTestCase):
    async def test_async_stream_inserts_everything(self):
        events = make_events(200)

        async def stream():
            for e in events:
                yield e
                await asyncio.sleep(0)

        book = OrderBook()
        self.assertEqual(await book.ingest_async(stream()), 200)
        self.assertEqual(len(book), 200)

    async def test_async_stream_with_bad_event(self):
        async def stream():
            yield {"order_id": "1", "side": BUY, "price": 1, "quantity": 1}
            yield {"order_id": "2", "side": BUY, "price": -1, "quantity": 1}

        with self.assertRaisesRegex(ValueError, "event 2"):
            await OrderBook().ingest_async(stream())


class TestManyInserts(InsertTestCase):
    def test_random_mix_of_good_and_bad_orders(self):
        rng = random.Random(42)
        expected = {}
        for i in range(2000):
            side = rng.choice([BUY, SELL, "X"])
            price = rng.choice([100.0, 101.5, 0, -3, float("nan")])
            qty = rng.choice([1, 5, 0, -2, 2.5])
            oid = str(rng.randint(1, 800))                # many duplicate ids
            order = Order(oid, side, price, qty)
            valid = (
                side in (BUY, SELL) and price in (100.0, 101.5)
                and qty in (1, 5) and oid not in expected
            )
            try:
                self.book.add_order(order)
            except ValueError:
                self.assertFalse(valid, f"valid order was rejected: {order}")
            else:
                self.assertTrue(valid, f"invalid order was accepted: {order}")
                expected[oid] = order
        self.assertEqual(len(self.book), len(expected))
        self.assertEqual({o.order_id for o in self.book.get_all_orders()}, set(expected))
        self.assertConsistent()

    def test_insert_cancel_insert_keeps_everything_consistent(self):
        rng = random.Random(7)
        live = set()
        for i in range(3000):
            oid = str(rng.randint(1, 500))
            if oid in live and rng.random() < 0.5:
                self.assertTrue(self.book.cancel_order(oid))
                live.discard(oid)
            elif oid not in live:
                self.book.add_order(Order(oid, rng.choice([BUY, SELL]), 100.0, 1))
                live.add(oid)
        self.assertEqual(len(self.book), len(live))
        self.assertConsistent()

    def test_insert_50000_orders_is_fast(self):
        events = make_events(50000)
        start = time.perf_counter()
        self.assertEqual(self.book.ingest_firehose(events), 50000)
        seconds = time.perf_counter() - start
        self.assertEqual(len(self.book), 50000)
        self.assertLess(seconds, 10)
        print(f"\n   50,000 orders inserted in {seconds:.2f} s")


if __name__ == "__main__":
    unittest.main(verbosity=2)