""" Order Book (Teammate 3)
Day 1: skeleton only. Stores buy/sell orders in simple Python structures.
"""
from collections import deque
from dataclasses import dataclass

BUY = "B"
SELL = "S"


@dataclass
class Order:
    order_id: int
    side: str        # "B" = buy, "S" = sell
    price: int       # in cents, e.g. 10050 = 100.50
    qty: int
    timestamp: int = 0


class OrderBook:
    def __init__(self):
        self.bids = {}    # buy orders:  price -> queue of orders
        self.asks = {}    # sell orders: price -> queue of orders
        self.orders = {}  # order_id -> Order (fast lookup)
        self.count = 0    # total orders received

    def __repr__(self):
        return (f"OrderBook(bid_levels={len(self.bids)}, "
                f"ask_levels={len(self.asks)}, orders={self.count})")


if __name__ == "__main__":
    book = OrderBook()
    print(book)
    print("Sample order:", Order(1, BUY, 10050, 10))