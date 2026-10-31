"""ChronosMatch - Stress test for the order book (Teammate 3, Day 9)

Usage:
    python stress_test.py              # runs 10,000 / 50,000 / 100,000 orders
    python stress_test.py 200000       # runs your own sizes
    python stress_test.py --save       # also writes the table to stress_results.txt

For each size it checks that the book stays CORRECT and measures how FAST it is.
"""

import asyncio
import random
import sys
import time
import tracemalloc

from orderbook import BUY, SELL, Order, OrderBook

DEFAULT_SIZES = (10_000, 50_000, 100_000)
MATCH_VALIDATION_LIMIT = 100_000          # skip trade validation above this size


def make_events(count, seed=1):
    rng = random.Random(seed)
    return [
        {
            "order_id": str(i),
            "side": rng.choice([BUY, SELL]),
            "price": rng.randint(9900, 10100),
            "quantity": rng.randint(1, 100),
        }
        for i in range(1, count + 1)
    ]


def timed(function, *args):
    start = time.perf_counter()
    result = function(*args)
    return result, time.perf_counter() - start


def check(condition, message):
    if not condition:
        raise AssertionError("STRESS TEST FAILED: " + message)


def check_book_is_consistent(book, expected_ids, label):
    """Every order is stored once, found by id, and the counts add up."""
    orders = book.get_all_orders()
    check(len(orders) == len(book) == len(expected_ids), f"{label}: wrong order count")
    check({o.order_id for o in orders} == expected_ids, f"{label}: wrong set of orders")
    for order in random.sample(orders, min(2000, len(orders))):
        check(book.get_order(order.order_id) is order, f"{label}: lookup mismatch")


def brute_force_best(book):
    buys = book.get_orders(BUY)
    sells = book.get_orders(SELL)
    return (
        max((o.price for o in buys), default=None),
        min((o.price for o in sells), default=None),
    )


def run_size(n):
    """Run every stress step for n orders. Returns a dictionary of numbers."""
    rng = random.Random(n)
    events = make_events(n)
    ids = {e["order_id"] for e in events}
    row = {"orders": n}

    # 1. ingest
    book = OrderBook()
    count, seconds = timed(book.ingest_firehose, events)
    check(count == n, "ingest count is wrong")
    check_book_is_consistent(book, ids, "after ingest")
    buys = sum(1 for e in events if e["side"] == BUY)
    check(len(book.get_orders(BUY)) == buys, "buy count is wrong")
    row["ingest_s"] = seconds
    row["ingest_per_s"] = n / seconds

    # 2. lookups
    probes = [str(rng.randint(1, n)) for _ in range(100_000)]
    _, seconds = timed(lambda: [book.get_order(p) for p in probes])
    row["lookup_us"] = seconds / len(probes) * 1e6

    # 3. best bid / best ask (checked against brute force)
    best_bid_price, best_ask_price = brute_force_best(book)
    check(book.best_bid().price == best_bid_price, "best_bid is wrong")
    check(book.best_ask().price == best_ask_price, "best_ask is wrong")
    _, seconds = timed(lambda: [(book.best_bid(), book.best_ask()) for _ in range(100)])
    row["best_ms"] = seconds / 200 * 1e3

    # 4. cancel 10% of the orders at random
    to_cancel = rng.sample(sorted(ids), n // 10)
    results, seconds = timed(lambda: [book.cancel_order(i) for i in to_cancel])
    check(all(results), "a cancel failed")
    ids -= set(to_cancel)
    check_book_is_consistent(book, ids, "after cancel")
    check(all(book.get_order(i) is None for i in to_cancel[:2000]), "cancelled order still found")
    check(not book.cancel_order(to_cancel[0]), "cancelling twice should return False")
    row["cancel_s"] = seconds
    row["cancel_us"] = seconds / len(to_cancel) * 1e6

    # 5. cancelled ids can be used again
    again = [
        {"order_id": i, "side": SELL, "price": 10_000, "quantity": 1}
        for i in to_cancel[:1000]
    ]
    check(book.ingest_firehose(again) == 1000, "re-insert failed")
    ids |= set(to_cancel[:1000])
    check_book_is_consistent(book, ids, "after re-insert")

    # 6. async ingest of the same events
    async def stream():
        for event in events:
            yield event
            await asyncio.sleep(0)

    async_book = OrderBook()
    count, seconds = timed(lambda: asyncio.run(async_book.ingest_async(stream())))
    check(count == n and len(async_book) == n, "async ingest is wrong")
    row["async_s"] = seconds

    # 7. validate trades from the reference matcher (if the validator is present)
    row["trades"] = None
    row["validate_s"] = None
    if n <= MATCH_VALIDATION_LIMIT:
        try:
            from trade_validator import reference_match, validate_trades
        except ImportError:
            pass
        else:
            orders = [Order(e["order_id"], e["side"], float(e["price"]), e["quantity"])
                      for e in events]
            trades = reference_match(orders)
            problems, seconds = timed(
                lambda: validate_trades(orders, trades, price_rule="resting"))
            check(problems == [], f"trade validation found problems: {problems[:3]}")
            row["trades"] = len(trades)
            row["validate_s"] = seconds

    # 8. memory used by n orders (measured on its own, tracing slows things down)
    tracemalloc.start()
    memory_book = OrderBook()
    memory_book.ingest_firehose(events)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    row["memory_mb"] = peak / 1e6
    return row


def format_table(rows):
    def num(value, fmt):
        return "-" if value is None else format(value, fmt)

    lines = [
        f"{'orders':>9} | {'ingest s':>8} | {'orders/s':>10} | {'lookup us':>9} | "
        f"{'best ms':>7} | {'cancel 10%':>10} | {'async s':>7} | {'trades':>7} | "
        f"{'validate s':>10} | {'peak MB':>7}",
        "-" * 110,
    ]
    for r in rows:
        lines.append(
            f"{r['orders']:>9,} | {r['ingest_s']:>8.3f} | {r['ingest_per_s']:>10,.0f} | "
            f"{r['lookup_us']:>9.2f} | {r['best_ms']:>7.3f} | {r['cancel_s']:>9.3f}s | "
            f"{r['async_s']:>7.3f} | {num(r['trades'], ',d'):>7} | "
            f"{num(r['validate_s'], '.3f'):>10} | {r['memory_mb']:>7.1f}"
        )
    return "\n".join(lines)


def main(argv):
    save = "--save" in argv
    sizes = [int(a.replace("_", "").replace(",", "")) for a in argv if a.replace("_", "").replace(",", "").isdigit()]
    sizes = sizes or list(DEFAULT_SIZES)

    rows = []
    for n in sizes:
        print(f"running {n:,} orders ...", flush=True)
        rows.append(run_size(n))

    table = format_table(rows)
    print("\n" + table)
    print("\nALL STRESS CHECKS PASSED")
    if save:
        with open("stress_results.txt", "w", encoding="utf-8") as handle:
            handle.write(table + "\n")
        print("saved to stress_results.txt")


if __name__ == "__main__":
    main(sys.argv[1:])