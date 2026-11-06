"""
ChronosMatch - Order Book (Teammate 3)
Combined Day 1-12 implementation.

Day 1: Basic order-book structure.
Day 2: Insert buy/sell orders with validation.
Day 3: Retrieve orders and look up by ID.
Day 4: Cancel orders and inspect best bid/ask.
Day 5: Ingest firehose events.
Day 6: Async firehose ingestion.
Day 7: Insertion testing support.
Day 8: Trade validation.
Day 9: Heap-based best bid/ask and stress-test support.
Day 10: Retrieve orders in arrival order.
Day 11: Binary IPC encoding and decoding.
Day 12: Batch IPC transmission and ingestion.

This is a storage/order-book module, not a full matching engine.
"""

import heapq
import math
import struct

from collections.abc import AsyncIterable, Iterable, Mapping
from dataclasses import dataclass
from itertools import count
from typing import Optional


BUY = "B"
SELL = "S"


# ============================================================
# DAY 1-2: ORDER MODEL AND BASIC ORDER BOOK
# ============================================================

@dataclass(frozen=True)
class Order:
    order_id: str
    side: str
    price: float
    quantity: int


class OrderBook:

    def __init__(self) -> None:
        # Day 1: Separate buy and sell storage.
        self._buys: dict[str, Order] = {}
        self._sells: dict[str, Order] = {}

        # Day 6: Fast order lookup.
        self._by_id: dict[str, Order] = {}

        # Day 9: Heaps for best bid and ask.
        self._bid_heap: list[tuple[float, int, Order]] = []
        self._ask_heap: list[tuple[float, int, Order]] = []

        self._seq_by_id: dict[str, int] = {}
        self._next_seq = count()

    @property
    def buys(self) -> list[Order]:
        """Return buy orders in arrival order."""
        return list(self._buys.values())

    @property
    def sells(self) -> list[Order]:
        """Return sell orders in arrival order."""
        return list(self._sells.values())

    # --------------------------------------------------------
    # DAY 2: INSERT ORDERS WITH VALIDATION
    # --------------------------------------------------------

    def add_order(self, order: Order) -> None:

        if not isinstance(order, Order):
            raise TypeError("order must be an Order object")

        if not isinstance(order.order_id, str) or not order.order_id.strip():
            raise ValueError("Order ID cannot be empty")

        if order.side not in (BUY, SELL):
            raise ValueError("Side must be 'B' (buy) or 'S' (sell)")

        if (
            isinstance(order.price, bool)
            or not isinstance(order.price, (int, float))
        ):
            raise ValueError("Price must be a number")

        if not math.isfinite(order.price):
            raise ValueError("Price must be finite")

        if order.price <= 0:
            raise ValueError("Price must be positive")

        if (
            isinstance(order.quantity, bool)
            or not isinstance(order.quantity, int)
        ):
            raise ValueError("Quantity must be an integer")

        if order.quantity <= 0:
            raise ValueError("Quantity must be positive")

        if order.order_id in self._by_id:
            raise ValueError(f"Duplicate order ID: {order.order_id}")

        seq = next(self._next_seq)

        if order.side == BUY:
            self._buys[order.order_id] = order
            heapq.heappush(
                self._bid_heap,
                (-order.price, seq, order)
            )
        else:
            self._sells[order.order_id] = order
            heapq.heappush(
                self._ask_heap,
                (order.price, seq, order)
            )

        self._by_id[order.order_id] = order
        self._seq_by_id[order.order_id] = seq

    # --------------------------------------------------------
    # DAY 3: RETRIEVE ORDERS
    # --------------------------------------------------------

    def get_orders(self, side: str) -> list[Order]:

        if side == BUY:
            return list(self._buys.values())

        if side == SELL:
            return list(self._sells.values())

        raise ValueError("Side must be 'B' (buy) or 'S' (sell)")

    def get_all_orders(self) -> list[Order]:
        return self.get_orders(BUY) + self.get_orders(SELL)

    def get_order(self, order_id: str) -> Optional[Order]:
        return self._by_id.get(order_id)

    # --------------------------------------------------------
    # DAY 4: CANCEL AND BEST BID/ASK
    # --------------------------------------------------------

    def cancel_order(self, order_id: str) -> bool:

        order = self._by_id.pop(order_id, None)

        if order is None:
            return False

        self._seq_by_id.pop(order_id, None)

        if order.side == BUY:
            self._buys.pop(order_id, None)
            self._compact(self._bid_heap, self._buys, BUY)
        else:
            self._sells.pop(order_id, None)
            self._compact(self._ask_heap, self._sells, SELL)

        return True

    def _compact(self, heap, live: dict, side: str) -> None:
        """Rebuild a heap when it contains too many cancelled entries."""

        if len(heap) <= 2 * len(live) + 64:
            return

        heap.clear()

        for order in live.values():
            seq = next(self._next_seq)
            self._seq_by_id[order.order_id] = seq

            key = -order.price if side == BUY else order.price
            heap.append((key, seq, order))

        heapq.heapify(heap)

    def _top(self, heap) -> Optional[Order]:
        """Return the best active order."""

        while heap:
            _, seq, order = heap[0]

            if self._seq_by_id.get(order.order_id) == seq:
                return order

            heapq.heappop(heap)

        return None

    def best_bid(self) -> Optional[Order]:
        """Return highest-priced buy order."""
        return self._top(self._bid_heap)

    def best_ask(self) -> Optional[Order]:
        """Return lowest-priced sell order."""
        return self._top(self._ask_heap)

    # --------------------------------------------------------
    # DAY 5: FIREHOSE INGESTION
    # --------------------------------------------------------

    def ingest_firehose(
        self,
        events: Iterable[Mapping[str, object]]
    ) -> int:
        """Insert a sequence of event dictionaries."""

        ingested = 0

        for event_number, event in enumerate(events, start=1):

            if not isinstance(event, Mapping):
                raise TypeError(
                    f"Firehose event {event_number} must be a mapping"
                )

            try:
                order_id = event["order_id"]
                side = event["side"]
                price = event["price"]
                quantity = event["quantity"]

            except KeyError as exc:
                raise ValueError(
                    f"Firehose event {event_number} "
                    f"is missing field {exc.args[0]!r}"
                ) from exc

            if not isinstance(order_id, str):
                raise ValueError(
                    f"Firehose event {event_number} "
                    "order_id must be a string"
                )

            if not isinstance(side, str):
                raise ValueError(
                    f"Firehose event {event_number} "
                    "side must be a string"
                )

            if (
                isinstance(price, bool)
                or not isinstance(price, (int, float))
            ):
                raise ValueError(
                    f"Firehose event {event_number} "
                    "price must be a number"
                )

            if (
                isinstance(quantity, bool)
                or not isinstance(quantity, int)
            ):
                raise ValueError(
                    f"Firehose event {event_number} "
                    "quantity must be an integer"
                )

            order = Order(
                order_id,
                side,
                float(price),
                quantity
            )

            try:
                self.add_order(order)
            except ValueError as exc:
                raise ValueError(
                    f"Firehose event {event_number}: {exc}"
                ) from exc

            ingested += 1

        return ingested

    # --------------------------------------------------------
    # DAY 6: ASYNC FIREHOSE INGESTION
    # --------------------------------------------------------

    async def ingest_async(
        self,
        async_events: AsyncIterable[Mapping[str, object]]
    ) -> int:

        batch = [event async for event in async_events]

        return self.ingest_firehose(batch)

    # --------------------------------------------------------
    # DAY 11-12: INGEST FROM BINARY IPC
    # --------------------------------------------------------

    def ingest_encoded(self, payload) -> int:
        """Decode a binary IPC batch and insert it.

        Decoding finishes before any insert, so broken bytes leave the book
        untouched. A bad order partway through keeps the earlier ones,
        the same as ingest_firehose.
        """
        return self.ingest_firehose(decode_events(payload))

    # --------------------------------------------------------
    # DAY 10: ARRIVAL-ORDER RETRIEVAL
    # --------------------------------------------------------

    def get_orders_in_arrival_order(self) -> list[Order]:
        """Return buy and sell orders mixed, oldest first."""
        return list(self._by_id.values())

    # --------------------------------------------------------
    # SNAPSHOT AND LENGTH
    # --------------------------------------------------------

    def snapshot(self) -> dict[str, list[Order]]:
        return {
            "buys": self.get_orders(BUY),
            "sells": self.get_orders(SELL)
        }

    def __len__(self) -> int:
        return len(self._buys) + len(self._sells)


# ============================================================
# DAY 8: TRADE VALIDATION
# ============================================================

@dataclass(frozen=True)
class Trade:
    trade_id: str
    buy_order_id: str
    sell_order_id: str
    price: float
    quantity: int


def check_trades(
    book: OrderBook,
    trades: Iterable[Trade]
) -> list[str]:
    """Return problems found in a batch of trades."""

    problems: list[str] = []
    seen_ids: set[str] = set()
    filled: dict[str, int] = {}

    for n, trade in enumerate(trades, start=1):

        errors: list[str] = []

        if not isinstance(trade.trade_id, str) or not trade.trade_id.strip():
            errors.append("trade ID cannot be empty")

        elif trade.trade_id in seen_ids:
            errors.append(f"duplicate trade ID {trade.trade_id!r}")

        if isinstance(trade.trade_id, str):
            seen_ids.add(trade.trade_id)

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

        buy = (
            book.get_order(trade.buy_order_id)
            if isinstance(trade.buy_order_id, str)
            else None
        )

        sell = (
            book.get_order(trade.sell_order_id)
            if isinstance(trade.sell_order_id, str)
            else None
        )

        if buy is None:
            errors.append(
                f"buy order {trade.buy_order_id!r} not found"
            )
        elif buy.side != BUY:
            errors.append(
                f"order {buy.order_id!r} is not a buy order"
            )
            buy = None

        if sell is None:
            errors.append(
                f"sell order {trade.sell_order_id!r} not found"
            )
        elif sell.side != SELL:
            errors.append(
                f"order {sell.order_id!r} is not a sell order"
            )
            sell = None

        if buy is not None and sell is not None:

            if buy.price < sell.price:
                errors.append(
                    f"orders do not cross "
                    f"(buy {buy.price} < sell {sell.price})"
                )

            elif price_ok and not (
                sell.price <= trade.price <= buy.price
            ):
                errors.append(
                    f"trade price {trade.price} outside "
                    f"[{sell.price}, {buy.price}]"
                )

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
            problems.extend(
                f"Trade {n}: {error}" for error in errors
            )

        else:
            filled[buy.order_id] = (
                filled.get(buy.order_id, 0) + trade.quantity
            )

            filled[sell.order_id] = (
                filled.get(sell.order_id, 0) + trade.quantity
            )

    return problems


def validate_trades(
    book: OrderBook,
    trades: Iterable[Trade]
) -> int:
    """Raise ValueError if any trade is invalid."""

    trades = list(trades)
    problems = check_trades(book, trades)

    if problems:
        raise ValueError("; ".join(problems))

    return len(trades)


# ============================================================
# DAY 11-12: OPTIMIZED BINARY IPC
# ============================================================

# Wire format: a 4-byte event count, then each event as
#   id length (2 bytes), side (1 byte, char code), price (8-byte float),
#   quantity (8 bytes), then the id text.  All big-endian.
_IPC_HEADER = struct.Struct("!I")      # event count
_IPC_EVENT = struct.Struct("!HcdQ")    # id length, side, price, quantity


def _check_fields(where, order_id, side, price, quantity):
    """Raise ValueError naming the event if any field is unusable."""

    if not isinstance(order_id, str):
        raise ValueError(f"{where}: invalid order ID (must be text)")

    if not order_id.strip():
        raise ValueError(f"{where}: invalid order ID (cannot be empty)")

    if len(order_id.encode("utf-8")) > 65535:
        raise ValueError(f"{where}: order ID is too long")

    if side not in (BUY, SELL):
        raise ValueError(f"{where}: invalid side (must be 'B' or 'S')")

    if isinstance(price, bool) or not isinstance(price, (int, float)):
        raise ValueError(f"{where}: invalid price (must be a number)")

    if not math.isfinite(price):
        raise ValueError(f"{where}: invalid price (must be finite)")

    if price <= 0:
        raise ValueError(f"{where}: invalid price (must be positive)")

    if isinstance(quantity, bool) or not isinstance(quantity, int):
        raise ValueError(f"{where}: invalid quantity (must be a whole number)")

    if quantity <= 0:
        raise ValueError(f"{where}: invalid quantity (must be positive)")

    if quantity > 0xFFFFFFFFFFFFFFFF:
        raise ValueError(f"{where}: invalid quantity (too large)")


def encode_events(events: Iterable) -> bytes:
    """Encode order events (dicts or Order objects) into one binary message."""

    events = list(events)

    if len(events) > 0xFFFFFFFF:
        raise ValueError("Too many events")

    payload = bytearray(_IPC_HEADER.size)
    _IPC_HEADER.pack_into(payload, 0, len(events))

    for n, event in enumerate(events, start=1):

        if isinstance(event, Mapping):
            try:
                order_id = event["order_id"]
                side = event["side"]
                price = event["price"]
                quantity = event["quantity"]
            except KeyError as exc:
                raise ValueError(
                    f"Firehose event {n} is missing field {exc.args[0]!r}"
                ) from exc
        elif all(hasattr(event, f)
                 for f in ("order_id", "side", "price", "quantity")):
            order_id, side = event.order_id, event.side
            price, quantity = event.price, event.quantity
        else:
            raise TypeError(f"Firehose event {n} must be a mapping")

        _check_fields(f"Event {n}", order_id, side, price, quantity)

        id_bytes = order_id.encode("utf-8")
        offset = len(payload)
        payload.extend(bytes(_IPC_EVENT.size + len(id_bytes)))
        _IPC_EVENT.pack_into(
            payload, offset, len(id_bytes),
            side.encode("ascii"), float(price), quantity
        )
        start = offset + _IPC_EVENT.size
        payload[start:start + len(id_bytes)] = id_bytes

    return bytes(payload)


def _side_label(code: int) -> str:
    """Show a side byte as its character if printable, else as a number."""
    return chr(code) if 32 < code < 127 else str(code)


def decode_events(payload) -> list[dict]:
    """Decode a binary IPC message into order dictionaries."""

    try:
        view = memoryview(payload)
    except TypeError as exc:
        raise ValueError("IPC payload must be bytes-like") from exc

    if len(view) < _IPC_HEADER.size:
        raise ValueError("Incomplete IPC header")

    (event_count,) = _IPC_HEADER.unpack_from(view)

    events = []
    offset = _IPC_HEADER.size

    for n in range(1, event_count + 1):
        where = f"IPC event {n}"

        if len(view) - offset < _IPC_EVENT.size:
            raise ValueError("Unsupported IPC version")

        id_length, side_bytes, price, quantity = (
            _IPC_EVENT.unpack_from(view, offset)
        )
        offset += _IPC_EVENT.size

        if len(view) - offset < id_length:
            raise ValueError(f"{where}: invalid order ID length")

        try:
            order_id = bytes(view[offset:offset + id_length]).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(
                f"{where}: order ID is not valid text"
            ) from exc
        offset += id_length

        code = side_bytes[0]
        side = chr(code)

        if side not in (BUY, SELL):
            raise ValueError(f"side code {code}")

        if not order_id.strip():
            raise ValueError(f"{where}: invalid order ID (cannot be empty)")

        if not math.isfinite(price):
            raise ValueError(f"{where}: invalid price (must be finite)")

        if price <= 0:
            raise ValueError(f"{where}: invalid price (must be positive)")

        if quantity <= 0:
            raise ValueError(f"{where}: invalid quantity (must be positive)")

        events.append({
            "order_id": order_id,
            "side": side,
            "price": price,
            "quantity": quantity,
        })

    if offset != len(view):
        raise ValueError(
            "Unexpected trailing IPC data: extra bytes after the last event"
        )

    return events


def send_events(connection, events: Iterable) -> int:
    """Send a batch as one binary message (connection needs send_bytes)."""
    events = list(events)
    connection.send_bytes(encode_events(events))
    return len(events)


def receive_events(connection) -> list[dict]:
    """Receive and decode one IPC batch."""
    return decode_events(connection.recv_bytes())


def ingest_encoded(book: OrderBook, payload) -> int:
    """Decode IPC data and insert it into an OrderBook."""
    return book.ingest_encoded(payload)