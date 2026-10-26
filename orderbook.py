"""ChronosMatch - Order Book (Teammate 3)
Combined Day 1-6 reference implementation.

Day 1: Basic order-book structure.
Day 2: Insert buy/sell orders with validation.
Day 3: Retrieve orders and look up an order by ID.
Day 4: Cancel orders and inspect best bid/ask prices.
Day 5: Ingest firehose events into the order book.
Day 6: Ingest from an async firehose; fast lookup by order ID.

This is a storage/order-book module, not a full trade-matching engine.
"""
# import math
import math
from dataclasses import dataclass
from collections.abc import AsyncIterable, Iterable, Mapping
from typing import Optional


from dataclasses import dataclass
from collections.abc import AsyncIterable, Iterable, Mapping
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
        # if order.price <= 0:
        

    def add_order(self, order: Order) -> None:
        """Day 2: Validate and insert a buy or sell order."""
        if not isinstance(order.order_id, str) or not order.order_id.strip():
            raise ValueError("Order ID cannot be empty")
        if order.side not in (BUY, SELL):
            raise ValueError("Side must be 'B' (buy) or 'S' (sell)")
        if isinstance(order.price, bool) or not isinstance(order.price, (int, float)):
            raise ValueError("Price must be a number")
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

    def ingest_firehose(
        self, events: Iterable[Mapping[str, object]]
    ) -> int:
        """Add firehose events with order_id, side, price, and quantity fields."""
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
    