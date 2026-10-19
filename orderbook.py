
"""ChronosMatch - Order Book
Teammate 3: Day 1, Day 2, Day 3
"""

from dataclasses import dataclass
from typing import Optional

BUY = "B"
SELL = "S"


@dataclass
class Order:
    order_id: str
    side: str
    price: float
    quantity: int


class OrderBook:
    def __init__(self):
        # Day 1: Basic order-book structure
        self.buys: list[Order] = []
        self.sells: list[Order] = []

    def add_order(self, order: Order) -> None:
        # Day 2: Insert buy or sell orders
        if not order.order_id:
            raise ValueError("Order ID cannot be empty")
        if order.side not in (BUY, SELL):
            raise ValueError("Side must be B or S")
        if order.price <= 0 or order.quantity <= 0:
            raise ValueError("Price and quantity must be positive")

        book = self.buys if order.side == BUY else self.sells

        if any(existing.order_id == order.order_id
               for existing in self.get_all_orders()):
            raise ValueError("Duplicate order ID")

        book.append(order)

    def get_orders(self, side: str) -> list[Order]:
        # Day 3: Retrieve orders by side
        if side == BUY:
            return list(self.buys)
        if side == SELL:
            return list(self.sells)
        raise ValueError("Side must be B or S")

    def get_all_orders(self) -> list[Order]:
        return self.get_orders(BUY) + self.get_orders(SELL)

    def get_order(self, order_id: str) -> Optional[Order]:
        # Retrieve a single order by ID
        for order in self.get_all_orders():
            if order.order_id == order_id:
                return order
        return None
