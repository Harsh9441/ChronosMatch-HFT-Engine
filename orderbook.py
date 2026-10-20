
"""ChronosMatch - Order Book (Teammate 3)
Combined Day 1-4 reference implementation.

Day 1: Basic order-book structure.
Day 2: Insert buy/sell orders with validation.
Day 3: Retrieve orders and look up an order by ID.
Day 4: Cancel orders and inspect best bid/ask prices.

This is a storage/order-book module, not a full trade-matching engine.
"""

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
        if self.get_order(order.order_id) is not None:
            raise ValueError(f"Duplicate order ID: {order.order_id}")

        book = self.buys if order.side == BUY else self.sells
        book.append(order)

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
        for order in self.buys:
            if order.order_id == order_id:
                return order
        for order in self.sells:
            if order.order_id == order_id:
                return order
        return None

    def cancel_order(self, order_id: str) -> bool:
        """Day 4: Remove an order by ID. Return True if it was found."""
        for book in (self.buys, self.sells):
            for index, order in enumerate(book):
                if order.order_id == order_id:
                    del book[index]
                    return True
        return False

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
