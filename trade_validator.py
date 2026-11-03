"""ChronosMatch - Trade Validator (Teammate 3, Day 8)

Checks that the trades made by a matching engine are correct.
It does NOT make trades. It only inspects them.

A trade is a dictionary:
    {"trade_id": "T1", "buy_order_id": "B2", "sell_order_id": "S1",
     "price": 100.0, "quantity": 5}
(trade_id is optional. If your engine uses other field names, change the
names in the FIELDS section below.)

Modes:
    "basic"      per-trade checks only
    "continuous" basic + no missed matches at the end
    "batch"      continuous + price-time priority (assumes ALL orders were in
                 the book before matching started)
"""

import heapq
from collections.abc import Iterable, Mapping
from typing import Optional

from orderbook import BUY, SELL, Order

# ---- FIELDS: change these names if the engine's trades use other keys ----
TRADE_ID = "trade_id"
BUY_ID = "buy_order_id"
SELL_ID = "sell_order_id"
PRICE = "price"
QUANTITY = "quantity"
# ---------------------------------------------------------------------------

MODES = ("basic", "continuous", "batch")


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def validate_trades(
    orders: Iterable[Order],
    trades: Iterable[Mapping[str, object]],
    mode: str = "batch",
    price_rule: Optional[str] = None,
    max_errors: int = 20,
) -> list[str]:
    """Return a list of problems found. An empty list means the trades are valid.

    orders      : every order given to the engine, in arrival order
    trades      : the trades the engine produced, in execution order
    mode        : "basic", "continuous" or "batch" (see the top of this file)
    price_rule  : None = any price between the two limit prices is accepted;
                  "resting" = the trade must use the price of the order that
                  arrived first
    max_errors  : stop collecting after this many problems

    Tip: fix the FIRST problem first. Later ones can be side effects of it.
    """
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}")
    if price_rule not in (None, "resting"):
        raise ValueError("price_rule must be None or 'resting'")

    errors: list[str] = []

    def err(message: str) -> None:
        if len(errors) < max_errors:
            errors.append(message)

    # ---- index the orders ----
    by_id: dict[str, Order] = {}
    arrival: dict[str, int] = {}
    for position, order in enumerate(orders):
        if order.order_id in by_id:
            err(f"Input problem: duplicate order id {order.order_id!r}")
            continue
        by_id[order.order_id] = order
        arrival[order.order_id] = position

    remaining = {oid: o.quantity for oid, o in by_id.items()}

    # best waiting order first: highest buy price / lowest sell price,
    # then the earliest arrival
    buy_heap = [(-o.price, arrival[oid], oid) for oid, o in by_id.items() if o.side == BUY]
    sell_heap = [(o.price, arrival[oid], oid) for oid, o in by_id.items() if o.side == SELL]
    heapq.heapify(buy_heap)
    heapq.heapify(sell_heap)

    def clean(heap: list) -> None:
        """Drop fully filled orders from the top of a heap."""
        while heap and remaining[heap[0][2]] <= 0:
            heapq.heappop(heap)

    seen_trade_ids: set = set()

    # ---- check every trade ----
    for number, trade in enumerate(trades, start=1):
        label = f"Trade {number}"

        if not isinstance(trade, Mapping):
            err(f"{label}: must be a dictionary")
            continue

        missing = [f for f in (BUY_ID, SELL_ID, PRICE, QUANTITY) if f not in trade]
        if missing:
            err(f"{label}: missing field {missing[0]!r}")
            continue

        buy_id, sell_id = trade[BUY_ID], trade[SELL_ID]
        price, qty = trade[PRICE], trade[QUANTITY]

        if TRADE_ID in trade:
            if trade[TRADE_ID] in seen_trade_ids:
                err(f"{label}: duplicate trade id {trade[TRADE_ID]!r}")
            seen_trade_ids.add(trade[TRADE_ID])

        usable = True
        if not _is_number(price) or price <= 0:
            err(f"{label}: price must be a positive number, got {price!r}")
            usable = False
        if not _is_int(qty) or qty <= 0:
            err(f"{label}: quantity must be a positive whole number, got {qty!r}")
            usable = False

        buy = by_id.get(buy_id)
        sell = by_id.get(sell_id)
        if buy is None:
            err(f"{label}: unknown buy order {buy_id!r}")
            usable = False
        elif buy.side != BUY:
            err(f"{label}: order {buy_id!r} is not a buy order")
            usable = False
        if sell is None:
            err(f"{label}: unknown sell order {sell_id!r}")
            usable = False
        elif sell.side != SELL:
            err(f"{label}: order {sell_id!r} is not a sell order")
            usable = False

        if not usable:
            continue                      # cannot check further or update state

        # the two orders must agree on a price
        if buy.price < sell.price:
            err(f"{label}: does not cross (buy limit {buy.price} is below "
                f"sell limit {sell.price})")
        elif not (sell.price <= price <= buy.price):
            err(f"{label}: price {price} is outside limit prices "
                f"[{sell.price}, {buy.price}]")

        if price_rule == "resting":
            first = buy if arrival[buy_id] < arrival[sell_id] else sell
            if price != first.price:
                err(f"{label}: price {price} is not the resting price "
                    f"{first.price} (order {first.order_id!r} arrived first)")

        # priority: the traded orders must be the best waiting ones
        if mode == "batch":
            clean(buy_heap)
            clean(sell_heap)
            # an order with nothing left is reported as an overfill below,
            # so it is not reported twice as a priority problem
            if remaining[buy_id] > 0 and buy_heap and buy_heap[0][2] != buy_id:
                top = by_id[buy_heap[0][2]]
                err(f"{label}: priority violated, buy {buy_id!r} traded but the best "
                    f"waiting buy is {top.order_id!r} at {top.price}")
            if remaining[sell_id] > 0 and sell_heap and sell_heap[0][2] != sell_id:
                top = by_id[sell_heap[0][2]]
                err(f"{label}: priority violated, sell {sell_id!r} traded but the best "
                    f"waiting sell is {top.order_id!r} at {top.price}")

        # no order may trade more than it has
        if qty > remaining[buy_id]:
            err(f"{label}: overfill, buy {buy_id!r} has only {remaining[buy_id]} left "
                f"but trade quantity is {qty}")
        if qty > remaining[sell_id]:
            err(f"{label}: overfill, sell {sell_id!r} has only {remaining[sell_id]} left "
                f"but trade quantity is {qty}")

        remaining[buy_id] = max(0, remaining[buy_id] - qty)
        remaining[sell_id] = max(0, remaining[sell_id] - qty)

    # ---- final check: nothing that could trade was left waiting ----
    if mode in ("continuous", "batch"):
        clean(buy_heap)
        clean(sell_heap)
        if buy_heap and sell_heap:
            best_buy = by_id[buy_heap[0][2]]
            best_sell = by_id[sell_heap[0][2]]
            if best_buy.price >= best_sell.price:
                err(f"Missed match: buy {best_buy.order_id!r} at {best_buy.price} "
                    f"({remaining[best_buy.order_id]} left) still crosses sell "
                    f"{best_sell.order_id!r} at {best_sell.price} "
                    f"({remaining[best_sell.order_id]} left)")

    return errors


def assert_valid_trades(orders, trades, **options) -> None:
    """Raise AssertionError listing every problem, or return quietly if valid."""
    errors = validate_trades(orders, trades, **options)
    if errors:
        raise AssertionError("Invalid trades:\n  " + "\n  ".join(errors))


def summarize(orders: Iterable[Order], trades: Iterable[Mapping[str, object]]) -> dict:
    """Simple numbers for reports (assumes the trades are valid)."""
    orders = list(orders)
    filled: dict[str, int] = {}
    total_qty = 0
    count = 0
    for t in trades:
        count += 1
        total_qty += t[QUANTITY]
        filled[t[BUY_ID]] = filled.get(t[BUY_ID], 0) + t[QUANTITY]
        filled[t[SELL_ID]] = filled.get(t[SELL_ID], 0) + t[QUANTITY]
    full = sum(1 for o in orders if filled.get(o.order_id, 0) == o.quantity)
    part = sum(1 for o in orders if 0 < filled.get(o.order_id, 0) < o.quantity)
    return {
        "orders": len(orders),
        "trades": count,
        "quantity_traded": total_qty,
        "fully_filled_orders": full,
        "partly_filled_orders": part,
        "unfilled_orders": len(orders) - full - part,
    }


# ---------------------------------------------------------------------------
# Simple REFERENCE matcher. Used only to test the validator.
# It is NOT the team's engine. Rules: best price first, then earliest order;
# trade price = price of the order that arrived first (the resting order).
# ---------------------------------------------------------------------------
def reference_match(orders: Iterable[Order]) -> list[dict]:
    orders = list(orders)
    remaining = {o.order_id: o.quantity for o in orders}
    buys = [(-o.price, i, o) for i, o in enumerate(orders) if o.side == BUY]
    sells = [(o.price, i, o) for i, o in enumerate(orders) if o.side == SELL]
    heapq.heapify(buys)
    heapq.heapify(sells)

    trades: list[dict] = []
    while buys and sells:
        _, buy_pos, buy = buys[0]
        _, sell_pos, sell = sells[0]
        if buy.price < sell.price:
            break
        qty = min(remaining[buy.order_id], remaining[sell.order_id])
        price = buy.price if buy_pos < sell_pos else sell.price
        trades.append({
            TRADE_ID: f"T{len(trades) + 1}",
            BUY_ID: buy.order_id,
            SELL_ID: sell.order_id,
            PRICE: price,
            QUANTITY: qty,
        })
        remaining[buy.order_id] -= qty
        remaining[sell.order_id] -= qty
        if remaining[buy.order_id] == 0:
            heapq.heappop(buys)
        if remaining[sell.order_id] == 0:
            heapq.heappop(sells)
    return trades


if __name__ == "__main__":
    import random

    random.seed(1)
    demo_orders = [
        Order(str(i), random.choice([BUY, SELL]),
              float(random.randint(9990, 10010)), random.randint(1, 50))
        for i in range(1, 501)
    ]
    demo_trades = reference_match(demo_orders)

    print("Correct trades:")
    print("  ", summarize(demo_orders, demo_trades))
    print("   problems found:", validate_trades(demo_orders, demo_trades, price_rule="resting"))

    broken = [dict(t) for t in demo_trades]
    broken[0][QUANTITY] += 1000
    del broken[5]
    print("\nTrades with 2 deliberate faults (quantity too big, one trade removed):")
    for problem in validate_trades(demo_orders, broken, max_errors=6):
        print("   -", problem)