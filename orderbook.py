"""ChronosMatch - Order Book (Teammate 3)
Combined Day 1-8 reference implementation.

Day 1: Basic order-book structure.
Day 2: Insert buy/sell orders with validation.
Day 3: Retrieve orders and look up an order by ID.
Day 4: Cancel orders and inspect best bid/ask prices.
Day 5: Ingest firehose events into the order book.
Day 6: Ingest from an async firehose; fast lookup by order ID.
Day 7: Tests for order insertion (see test_day7_insertion.py).
Day 8: Validate matched trades against the order book.

This is a storage/order-book module, not a full trade-matching engine.
Trade validation only CHECKS trades; it never changes the book.
"""

import math
from collections.abc import AsyncIterable, Iterable, Mapping
from dataclasses import dataclass
from typing import Optional

BUY = "B"
SELL = "S"


@dataclass(frozen=True)
class Order:
    order_id: str
    side: str
    price: float
    quantity: int


class OrderBook:
    def __init__(self) -> None:
        # Day 1: Store buy and sell orders separately.
        self.buys: list[Order] = []
        self.sells: list[Order] = []
        # Day 6: order_id -> Order, so lookups and duplicate checks are instant.
        self._by_id: dict[str, Order] = {}

    def add_order(self, order: Order) -> None:
        """Day 2: Validate and insert a buy or sell order."""
        if not isinstance(order.order_id, str) or not order.order_id.strip():
            raise ValueError("Order ID cannot be empty")
        if order.side not in (BUY, SELL):
            raise ValueError("Side must be 'B' (buy) or 'S' (sell)")
        if isinstance(order.price, bool) or not isinstance(order.price, (int, float)):
            raise ValueError("Price must be a number")
        if not math.isfinite(order.price):
            raise ValueError("Price must be finite")
        if order.price <= 0:
            raise ValueError("Price must be positive")
        if isinstance(order.quantity, bool) or not isinstance(order.quantity, int):
            raise ValueError("Quantity must be an integer")
        if order.quantity <= 0:
            raise ValueError("Quantity must be positive")
        if order.order_id in self._by_id:
            raise ValueError(f"Duplicate order ID: {order.order_id}")

        book = self.buys if order.side == BUY else self.sells
        book.append(order)
        self._by_id[order.order_id] = order

    def ingest_firehose(self, events: Iterable[Mapping[str, object]]) -> int:
        """Add firehose events with order_id, side, price, and quantity fields.

        Events are inserted one at a time. If an event is bad, a ValueError
        (or TypeError for a non-mapping event) is raised and the events
        before it stay in the book.
        """
        ingested = 0
        for event_number, event in enumerate(events, start=1):
            if not isinstance(event, Mapping):
                raise TypeError(f"Firehose event {event_number} must be a mapping")

            try:
                order_id = event["order_id"]
                side = event["side"]
                price = event["price"]
                quantity = event["quantity"]
            except KeyError as exc:
                raise ValueError(
                    f"Firehose event {event_number} is missing field {exc.args[0]!r}"
                ) from exc

            if not isinstance(order_id, str):
                raise ValueError(
                    f"Firehose event {event_number} order_id must be a string"
                )
            if not isinstance(side, str):
                raise ValueError(
                    f"Firehose event {event_number} side must be a string"
                )
            if isinstance(price, bool) or not isinstance(price, (int, float)):
                raise ValueError(
                    f"Firehose event {event_number} price must be a number"
                )
            if isinstance(quantity, bool) or not isinstance(quantity, int):
                raise ValueError(
                    f"Firehose event {event_number} quantity must be an integer"
                )

            order = Order(order_id, side, float(price), quantity)
            try:
                self.add_order(order)
            except ValueError as exc:
                raise ValueError(f"Firehose event {event_number}: {exc}") from exc
            ingested += 1

        return ingested

    async def ingest_async(
        self, async_events: AsyncIterable[Mapping[str, object]]
    ) -> int:
        """Day 6: Collect events from an async firehose, then ingest them."""
        batch = [event async for event in async_events]
        return self.ingest_firehose(batch)

    def get_orders(self, side: str) -> list[Order]:
        """Day 3: Return a copy of orders for the requested side."""
        if side == BUY:
            return list(self.buys)
        if side == SELL:
            return list(self.sells)
        raise ValueError("Side must be 'B' (buy) or 'S' (sell)")

    def get_all_orders(self) -> list[Order]:
        """Return all buy orders followed by all sell orders."""
        return self.get_orders(BUY) + self.get_orders(SELL)

    def get_order(self, order_id: str) -> Optional[Order]:
        """Return one order by ID, or None if it does not exist."""
        return self._by_id.get(order_id)

    def cancel_order(self, order_id: str) -> bool:
        """Day 4: Remove an order by ID. Return True if it was found."""
        order = self._by_id.pop(order_id, None)
        if order is None:
            return False
        book = self.buys if order.side == BUY else self.sells
        book.remove(order)
        return True

    def best_bid(self) -> Optional[Order]:
        """Day 4: Return the highest-priced buy order, or None."""
        return max(self.buys, key=lambda order: order.price, default=None)

    def best_ask(self) -> Optional[Order]:
        """Day 4: Return the lowest-priced sell order, or None."""
        return min(self.sells, key=lambda order: order.price, default=None)

    def snapshot(self) -> dict[str, list[Order]]:
        """Return a simple snapshot for display/integration."""
        return {"buys": self.get_orders(BUY), "sells": self.get_orders(SELL)}

    def __len__(self) -> int:
        return len(self.buys) + len(self.sells)


# ---------------------------------------------------------------
# Day 8: Trade validation
# ---------------------------------------------------------------

@dataclass(frozen=True)
class Trade:
    trade_id: str
    buy_order_id: str
    sell_order_id: str
    price: float
    quantity: int


def check_trades(book: OrderBook, trades: Iterable[Trade]) -> list[str]:
    """Return a list of problems found. An empty list means all trades are valid.

    Rules checked for every trade:
      - trade_id is a non-empty string and unique within the batch
      - price is a finite, positive number; quantity is a positive integer
      - both order IDs exist in the book, the buy ID is a buy order and the
        sell ID is a sell order
      - the orders cross: buy limit price >= sell limit price
      - the trade price lies between the sell and buy limit prices
      - total quantity traded per order, across the batch, never exceeds the
        order's quantity (no overfills)

    Trades with a problem do not count toward the overfill totals.
    """
    problems: list[str] = []
    seen_ids: set[str] = set()
    filled: dict[str, int] = {}

    for n, trade in enumerate(trades, start=1):
        errors: list[str] = []

        # --- trade id ---
        if not isinstance(trade.trade_id, str) or not trade.trade_id.strip():
            errors.append("trade ID cannot be empty")
        elif trade.trade_id in seen_ids:
            errors.append(f"duplicate trade ID {trade.trade_id!r}")
        if isinstance(trade.trade_id, str):
            seen_ids.add(trade.trade_id)

        # --- price and quantity ---
        price_ok = (
            not isinstance(trade.price, bool)
            and isinstance(trade.price, (int, float))
            and math.isfinite(trade.price)
            and trade.price > 0
        )
        if not price_ok:
            errors.append("price must be a finite positive number")

        qty_ok = (
            not isinstance(trade.quantity, bool)
            and isinstance(trade.quantity, int)
            and trade.quantity > 0
        )
        if not qty_ok:
            errors.append("quantity must be a positive integer")

        # --- orders ---
        buy = book.get_order(trade.buy_order_id) if isinstance(trade.buy_order_id, str) else None
        sell = book.get_order(trade.sell_order_id) if isinstance(trade.sell_order_id, str) else None

        if buy is None:
            errors.append(f"buy order {trade.buy_order_id!r} not found")
        elif buy.side != BUY:
            errors.append(f"order {buy.order_id!r} is not a buy order")
            buy = None

        if sell is None:
            errors.append(f"sell order {trade.sell_order_id!r} not found")
        elif sell.side != SELL:
            errors.append(f"order {sell.order_id!r} is not a sell order")
            sell = None

        # --- price relationships ---
        if buy is not None and sell is not None:
            if buy.price < sell.price:
                errors.append(
                    f"orders do not cross (buy {buy.price} < sell {sell.price})"
                )
            elif price_ok and not (sell.price <= trade.price <= buy.price):
                errors.append(
                    f"trade price {trade.price} outside "
                    f"[{sell.price}, {buy.price}]"
                )

        # --- overfill ---
        if qty_ok:
            for order in (buy, sell):
                if order is None:
                    continue
                total = filled.get(order.order_id, 0) + trade.quantity
                if total > order.quantity:
                    errors.append(
                        f"order {order.order_id!r} overfilled "
                        f"({total} > {order.quantity})"
                    )

        if errors:
            problems.extend(f"Trade {n}: {e}" for e in errors)
        else:
            filled[buy.order_id] = filled.get(buy.order_id, 0) + trade.quantity
            filled[sell.order_id] = filled.get(sell.order_id, 0) + trade.quantity

    return problems


def validate_trades(book: OrderBook, trades: Iterable[Trade]) -> int:
    """Raise ValueError listing every problem, or return the number of valid trades."""
    trades = list(trades)
    problems = check_trades(book, trades)
    if problems:
        raise ValueError("; ".join(problems))
    return len(trades)