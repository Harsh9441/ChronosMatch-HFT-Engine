"""
ChronosMatch - Order Book (Teammate 3)
Combined Day 1-21 implementation.

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
Day 13: Load test with 50,000+ orders.
Day 14: Zero-copy architecture verification.
Day 15: Final benchmark report: python orderbook.py --report [--quick] [--skip-tests]
Day 16: Database layer (SQLite) joining the order book and a matching
        engine: python orderbook.py --database
Day 17: Trade persistence validation: python orderbook.py --persistence
Day 18: Missing and failed records (quarantine, retry, reconcile):
        python orderbook.py --failures
Day 19: Recovery and resiliency testing: python orderbook.py --resilience
Day 20: Export logs for reporting: python orderbook.py --export-logs DB FOLDER
Day 21: End-to-end validation: python orderbook.py --end-to-end
Check an existing database file: python orderbook.py --validate-db FILE
Test count per file (no run): python orderbook.py --count-tests

This is a storage/order-book module, not a full matching engine.
"""

import csv
import datetime
import hashlib
import heapq
import json
import math
import os
import platform
import random
import re
import sqlite3
import struct
import subprocess
import tempfile
import threading
import time
import tracemalloc

from collections.abc import AsyncIterable, Iterable, Mapping
from dataclasses import asdict, dataclass
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
            del self._buys[order_id]
            self._compact(self._bid_heap, self._buys, BUY)
        else:
            del self._sells[order_id]
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
#   side (1 byte), id length (2 bytes), price (8-byte float),
#   quantity (8 bytes), then the id text.  All big-endian.
_IPC_HEADER = struct.Struct("!I")      # event count
_IPC_EVENT = struct.Struct("!cHdQ")    # side, id length, price, quantity


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
            payload, offset,
            side.encode("ascii"), len(id_bytes), float(price), quantity
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
            raise ValueError(f"{where}: incomplete data")

        side_bytes, id_length, price, quantity = (
            _IPC_EVENT.unpack_from(view, offset)
        )
        offset += _IPC_EVENT.size

        if id_length == 0 or len(view) - offset < id_length:
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

        if not order_id.strip():
            raise ValueError(f"{where}: invalid order ID (cannot be empty)")

        if side not in (BUY, SELL):
            raise ValueError(
                f"{where}: invalid side code {_side_label(code)}"
            )

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


# ============================================================
# DAY 13: LOAD TEST (50,000+ ORDERS)
# ============================================================

def make_load_events(n_orders: int, seed: int = 13) -> list[dict]:
    """Build a repeatable list of valid order events."""

    rng = random.Random(seed)

    return [
        {
            "order_id": f"L{i}",
            "side": BUY if rng.random() < 0.5 else SELL,
            "price": round(rng.uniform(90, 110), 2),
            "quantity": rng.randint(1, 1000),
        }
        for i in range(n_orders)
    ]


def _best_prices_match(book: OrderBook) -> bool:
    """Compare best_bid / best_ask with a brute-force scan."""

    buys = book.get_orders(BUY)
    sells = book.get_orders(SELL)
    bid, ask = book.best_bid(), book.best_ask()

    if buys:
        bid_ok = bid is not None and bid.price == max(o.price for o in buys)
    else:
        bid_ok = bid is None

    if sells:
        ask_ok = ask is not None and ask.price == min(o.price for o in sells)
    else:
        ask_ok = ask is None

    return bid_ok and ask_ok


def run_load_test(
    n_orders: int = 50_000,
    cancel_fraction: float = 0.2,
    seed: int = 13
) -> dict:
    """Push n_orders through encode, decode, ingest and cancel, and check
    every result. Returns timings, rates and a dict of pass/fail checks."""

    if n_orders < 1:
        raise ValueError("n_orders must be at least 1")

    if not 0 <= cancel_fraction < 1:
        raise ValueError("cancel_fraction must be from 0 up to (not incl.) 1")

    rng = random.Random(seed)
    events = make_load_events(n_orders, seed)
    ids = [e["order_id"] for e in events]

    def timed(fn):
        start = time.perf_counter()
        value = fn()
        return value, max(time.perf_counter() - start, 1e-9)

    payload, encode_s = timed(lambda: encode_events(events))
    decoded, decode_s = timed(lambda: decode_events(payload))

    direct = OrderBook()
    _, ingest_s = timed(lambda: direct.ingest_firehose(events))

    book = OrderBook()
    _, ingest_encoded_s = timed(lambda: book.ingest_encoded(payload))

    checks: dict[str, bool] = {}
    arrival = book.get_orders_in_arrival_order()

    checks["decoded_equals_input"] = decoded == events
    checks["encoded_path_matches_direct_path"] = (
        arrival == direct.get_orders_in_arrival_order()
    )
    checks["all_orders_stored"] = len(book) == n_orders
    checks["no_duplicate_ids"] = (
        len({o.order_id for o in arrival}) == n_orders
    )
    checks["arrival_order_kept"] = [o.order_id for o in arrival] == ids
    checks["sides_add_up"] = len(book.buys) + len(book.sells) == n_orders

    found, lookup_s = timed(
        lambda: all(book.get_order(i) is not None for i in ids)
    )
    checks["every_id_found"] = found
    checks["best_prices_correct_before_cancel"] = _best_prices_match(book)

    n_cancel = int(n_orders * cancel_fraction)
    cancelled = rng.sample(ids, n_cancel)
    cancelled_set = set(cancelled)

    results, cancel_s = timed(
        lambda: [book.cancel_order(i) for i in cancelled]
    )
    remaining = [i for i in ids if i not in cancelled_set]

    checks["cancels_succeeded"] = all(results)
    checks["cancelled_orders_gone"] = all(
        book.get_order(i) is None for i in cancelled
    )
    checks["count_after_cancel"] = len(book) == n_orders - n_cancel
    checks["arrival_order_after_cancel"] = (
        [o.order_id for o in book.get_orders_in_arrival_order()] == remaining
    )
    checks["best_prices_correct_after_cancel"] = _best_prices_match(book)
    checks["cancelling_again_returns_false"] = not any(
        book.cancel_order(i) for i in cancelled[:100]
    )

    return {
        "orders": n_orders,
        "cancelled": n_cancel,
        "payload_bytes": len(payload),
        "encode_s": encode_s,
        "decode_s": decode_s,
        "ingest_s": ingest_s,
        "ingest_encoded_s": ingest_encoded_s,
        "lookup_s": lookup_s,
        "cancel_s": cancel_s,
        "encode_rate": n_orders / encode_s,
        "decode_rate": n_orders / decode_s,
        "ingest_rate": n_orders / ingest_s,
        "ingest_encoded_rate": n_orders / ingest_encoded_s,
        "lookup_rate": n_orders / lookup_s,
        "cancel_rate": max(n_cancel, 1) / cancel_s,
        "checks": checks,
        "ok": all(checks.values()),
    }


def format_load_report(r: dict) -> str:
    lines = [
        f"LOAD TEST: {r['orders']:,} orders, {r['cancelled']:,} cancelled, "
        f"payload {r['payload_bytes']:,} bytes",
        f"  encode            {r['encode_s']:8.3f} s  {r['encode_rate']:>12,.0f} /s",
        f"  decode            {r['decode_s']:8.3f} s  {r['decode_rate']:>12,.0f} /s",
        f"  ingest (dicts)    {r['ingest_s']:8.3f} s  {r['ingest_rate']:>12,.0f} /s",
        f"  ingest (encoded)  {r['ingest_encoded_s']:8.3f} s  "
        f"{r['ingest_encoded_rate']:>12,.0f} /s",
        f"  lookup            {r['lookup_s']:8.3f} s  {r['lookup_rate']:>12,.0f} /s",
        f"  cancel            {r['cancel_s']:8.3f} s  {r['cancel_rate']:>12,.0f} /s",
        "  checks:",
    ]
    lines += [
        f"    {'ok  ' if ok else 'FAIL'} {name}"
        for name, ok in r["checks"].items()
    ]
    lines.append("  RESULT: " + ("ALL CHECKS PASSED" if r["ok"] else "FAILED"))
    return "\n".join(lines)


# ============================================================
# DAY 14: ZERO-COPY ARCHITECTURE VERIFICATION
# ============================================================

_ZC_TEST_VALUE = 123456789


def _zero_copy_child(name: str, ready, done) -> None:
    """Child process: write a number straight into the shared block."""

    from multiprocessing import shared_memory

    shm = shared_memory.SharedMemory(name=name)
    shm.buf[0:8] = struct.pack("<Q", _ZC_TEST_VALUE)
    ready.set()
    done.wait(30)
    shm.close()


def verify_zero_copy(n_orders: int = 2000) -> dict:
    """Measure where data is shared and where it is copied.

    Checks 1-3 prove the transport is zero-copy. Check 4 measures the order
    book intake, which is expected to copy (it builds Python objects).
    """

    import multiprocessing as mp
    from multiprocessing import shared_memory

    checks: list[dict] = []

    def add(name: str, passed: bool, detail: str) -> None:
        checks.append({"check": name, "passed": bool(passed), "detail": detail})

    started_tracing = not tracemalloc.is_tracing()
    if started_tracing:
        tracemalloc.start()

    try:
        # 1. A memoryview slice shares memory and allocates (almost) nothing.
        buf = bytearray(1_000_000)
        mv = memoryview(buf)
        before = tracemalloc.get_traced_memory()[0]
        window = mv[1000:900_000]
        growth = tracemalloc.get_traced_memory()[0] - before
        buf[1000] = 99
        add("memoryview slice is zero-copy",
            window[0] == 99 and window.obj is buf and growth < 4096,
            f"slice of 899,000 bytes used {growth} extra bytes and saw a "
            "later write")
        window.release()

        # 2. bytes(...) of the same slice is a real copy.
        mv_slice = mv[1000:900_000]
        copied = bytes(mv_slice)
        buf[1000] = 7
        add("bytes(view[...]) makes an independent copy",
            copied[0] == 99 and buf[1000] == 7,
            "the copy kept the old value after the source changed")
        mv_slice.release()
        mv.release()

        # 3. Two processes share one block with nothing sent through a pipe.
        ctx = mp.get_context("spawn")
        shm = shared_memory.SharedMemory(create=True, size=4096)
        ready, done = ctx.Event(), ctx.Event()
        proc = ctx.Process(target=_zero_copy_child, args=(shm.name, ready, done))
        value = None
        try:
            proc.start()
            if ready.wait(60):
                value = struct.unpack_from("<Q", shm.buf, 0)[0]
        finally:
            done.set()
            proc.join(10)
            if proc.is_alive():
                proc.terminate()
            shm.close()
            shm.unlink()
        add("two processes share one memory block",
            value == _ZC_TEST_VALUE,
            f"parent read {value} written by the child; nothing was pickled "
            "or sent through a pipe")

        # 4. Order book intake: does it keep views of shared memory?
        events = make_load_events(n_orders, seed=14)
        payload = encode_events(events)
        shm = shared_memory.SharedMemory(create=True, size=len(payload))
        try:
            shm.buf[:len(payload)] = payload

            window = shm.buf[:len(payload)]
            before = tracemalloc.get_traced_memory()[0]
            decoded = decode_events(window)
            grown = tracemalloc.get_traced_memory()[0] - before
            window.release()

            book = OrderBook()
            window = shm.buf[:len(payload)]
            book.ingest_encoded(window)
            window.release()

            shm.buf[:len(payload)] = bytes(len(payload))   # wipe the block

            decoded_intact = decoded == events
            book_intact = [
                {"order_id": o.order_id, "side": o.side,
                 "price": o.price, "quantity": o.quantity}
                for o in book.get_orders_in_arrival_order()
            ] == events
        finally:
            shm.close()
            shm.unlink()

        factor = grown / max(len(payload), 1)
        copied_out = decoded_intact and book_intact
        add("order book copies orders out of shared memory",
            copied_out,
            f"{len(payload):,} shared bytes became {grown:,} bytes of Python "
            f"objects ({factor:.1f}x); orders survived the block being wiped")

    finally:
        if started_tracing:
            tracemalloc.stop()

    transport_ok = all(c["passed"] for c in checks[:3])

    return {
        "checks": checks,
        "transport_zero_copy": transport_ok,
        "book_is_zero_copy": not checks[3]["passed"],
        "memory_growth_factor": factor,
        "ok": all(c["passed"] for c in checks),
    }


def format_zero_copy_report(z: dict) -> str:
    lines = ["ZERO-COPY VERIFICATION"]
    for c in z["checks"]:
        lines.append(f"  {'ok  ' if c['passed'] else 'FAIL'} {c['check']}")
        lines.append(f"       {c['detail']}")
    lines += [
        f"  Transport (shared memory) is zero-copy: {z['transport_zero_copy']}",
        f"  Order book is zero-copy: {z['book_is_zero_copy']} "
        f"(decoded data is {z['memory_growth_factor']:.1f}x larger in Python "
        "objects)",
        "  RESULT: " + ("VERIFIED" if z["ok"] else "FAILED"),
    ]
    return "\n".join(lines)


# ============================================================
# DAY 16: DATABASE LAYER (ORDER BOOK + MATCHING ENGINE + SQLITE)
# ============================================================

# Orders, trades and fills are stored in SQLite so they survive a restart.
# Rules enforced by the database itself: IDs are never reused, trades need
# real orders on the right sides, prices stay inside the limits, orders are
# never overfilled, and a trade batch is saved completely or not at all.
# The book keeps ORIGINAL quantities; open quantity = quantity - filled.

_MAX_INT = 2 ** 63 - 1                      # largest integer SQLite can store

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    seq       INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id  TEXT    NOT NULL UNIQUE,
    side      TEXT    NOT NULL CHECK (side IN ('B', 'S')),
    price     REAL    NOT NULL CHECK (price > 0),
    quantity  INTEGER NOT NULL CHECK (quantity > 0),
    filled    INTEGER NOT NULL DEFAULT 0
              CHECK (filled >= 0 AND filled <= quantity),
    status    TEXT    NOT NULL DEFAULT 'open'
              CHECK (status IN ('open', 'partial', 'filled', 'cancelled'))
);
CREATE TABLE IF NOT EXISTS trades (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id       TEXT    NOT NULL UNIQUE,
    buy_order_id   TEXT    NOT NULL REFERENCES orders(order_id),
    sell_order_id  TEXT    NOT NULL REFERENCES orders(order_id),
    price          REAL    NOT NULL CHECK (price > 0),
    quantity       INTEGER NOT NULL CHECK (quantity > 0)
);
CREATE TABLE IF NOT EXISTS events (
    seq   INTEGER PRIMARY KEY AUTOINCREMENT,
    kind  TEXT NOT NULL,
    ref   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rejected (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    kind      TEXT    NOT NULL CHECK (kind IN ('order', 'trade', 'cancel')),
    ref       TEXT,
    reason    TEXT    NOT NULL,
    payload   TEXT,
    resolved  INTEGER NOT NULL DEFAULT 0 CHECK (resolved IN (0, 1))
);
CREATE INDEX IF NOT EXISTS idx_rejected_open ON rejected(resolved, kind);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_trades_buy    ON trades(buy_order_id);
CREATE INDEX IF NOT EXISTS idx_trades_sell   ON trades(sell_order_id);
CREATE INDEX IF NOT EXISTS idx_events_ref    ON events(kind, ref);
"""


def _to_json(value) -> str:
    """Keep a copy of a rejected record as text, whatever it is."""

    if isinstance(value, Trade):
        value = asdict(value)

    try:
        return json.dumps(value, default=repr)
    except (TypeError, ValueError):
        return json.dumps(repr(value))


def _trade_ref(trade):
    ref = trade.get("trade_id") if isinstance(trade, Mapping) else None
    return ref if isinstance(ref, str) else None


def _chunks(items: list, size: int = 500):
    for start in range(0, len(items), size):
        yield items[start:start + size]


class DatabaseBusyError(RuntimeError):
    """The database stayed locked or busy after every retry."""


class OrderDatabase:
    """Tables and operations. Every write is one all-or-nothing transaction."""

    def __init__(self, path: str = ":memory:", read_only: bool = False,
                 retries: int = 3, timeout: float = 5.0) -> None:
        self.path = path
        self.retries = retries

        if read_only:
            # Open an existing file for reading only: nothing can be changed.
            from pathlib import Path

            if path == ":memory:" or not os.path.exists(path):
                raise FileNotFoundError(f"No database file at {path}")

            self.conn = sqlite3.connect(
                Path(path).resolve().as_uri() + "?mode=ro", uri=True
            )
            self.conn.execute("PRAGMA foreign_keys = ON")
            return

        self.conn = sqlite3.connect(path, timeout=timeout)
        self.conn.execute("PRAGMA foreign_keys = ON")

        if path != ":memory:":
            self.conn.execute("PRAGMA journal_mode = WAL")
            self.conn.execute("PRAGMA synchronous = NORMAL")

        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "OrderDatabase":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # --------------------------------------------------------
    # ORDERS
    # --------------------------------------------------------

    def _write_quarantine(self, rejected, resolve) -> None:
        """Quarantine and resolve rows, inside the caller's transaction."""

        if rejected:
            self.conn.executemany(
                "INSERT INTO rejected (kind, ref, reason, payload) "
                "VALUES (?, ?, ?, ?)", list(rejected)
            )

        if resolve:
            self.conn.executemany(
                "UPDATE rejected SET resolved = 1 WHERE id = ?",
                [(i,) for i in resolve]
            )

    def _save_orders_once(self, orders: Iterable[Order], rejected=(),
                          resolve=()) -> int:
        """Store new orders. If any is refused, none are stored.

        Quarantine rows for bad events are saved in the same transaction.
        """

        rows = []

        for order in orders:
            if not isinstance(order, Order):
                raise TypeError("save_orders needs Order objects")

            if order.quantity > _MAX_INT:
                raise ValueError(
                    f"Order {order.order_id!r}: quantity too large "
                    "for the database"
                )

            rows.append(
                (order.order_id, order.side, float(order.price), order.quantity)
            )

        if not rows and not rejected and not resolve:
            return 0

        with self.conn:
            if rows:
                try:
                    self.conn.executemany(
                        "INSERT INTO orders (order_id, side, price, quantity) "
                        "VALUES (?, ?, ?, ?)", rows
                    )
                except sqlite3.IntegrityError as exc:
                    raise ValueError(
                        f"Database refused the orders: {exc}"
                    ) from exc

                self.conn.executemany(
                    "INSERT INTO events (kind, ref) VALUES ('order', ?)",
                    [(row[0],) for row in rows]
                )

            self._write_quarantine(rejected, resolve)

        return len(rows)

    def _mark_cancelled_once(self, order_id: str) -> bool:
        """Mark an open or partly filled order cancelled. False if none."""

        with self.conn:
            cursor = self.conn.execute(
                "UPDATE orders SET status = 'cancelled' "
                "WHERE order_id = ? AND status IN ('open', 'partial')",
                (order_id,)
            )

            if cursor.rowcount == 0:
                return False

            self.conn.execute(
                "INSERT INTO events (kind, ref) VALUES ('cancel', ?)",
                (order_id,)
            )

        return True

    def get_order(self, order_id: str):
        row = self.conn.execute(
            "SELECT order_id, side, price, quantity, filled, status "
            "FROM orders WHERE order_id = ?", (order_id,)
        ).fetchone()

        if row is None:
            return None

        keys = ("order_id", "side", "price", "quantity", "filled", "status")
        return dict(zip(keys, row))

    def known_ids(self, ids: Iterable[str]) -> set[str]:
        """Which of these order IDs have ever been used?"""

        ids = list(set(ids))
        found: set[str] = set()

        for chunk in _chunks(ids):
            marks = ",".join("?" * len(chunk))
            found.update(
                row[0] for row in self.conn.execute(
                    f"SELECT order_id FROM orders WHERE order_id IN ({marks})",
                    chunk
                )
            )

        return found

    def load_open_orders(self) -> list[Order]:
        """Open and partly filled orders in arrival order (original quantity)."""

        return [
            Order(order_id, side, price, quantity)
            for order_id, side, price, quantity in self.conn.execute(
                "SELECT order_id, side, price, quantity FROM orders "
                "WHERE status IN ('open', 'partial') ORDER BY seq"
            )
        ]

    def filled_map(self) -> dict[str, int]:
        """How much of each partly or fully filled order has traded."""

        return dict(self.conn.execute(
            "SELECT order_id, filled FROM orders WHERE filled > 0"
        ))

    def filled_ids(self, ids: Iterable[str]) -> set[str]:
        """Which of these orders are completely filled?"""

        ids = list(set(ids))
        done: set[str] = set()

        for chunk in _chunks(ids):
            marks = ",".join("?" * len(chunk))
            done.update(
                row[0] for row in self.conn.execute(
                    f"SELECT order_id FROM orders "
                    f"WHERE status = 'filled' AND order_id IN ({marks})",
                    chunk
                )
            )

        return done

    # --------------------------------------------------------
    # DAY 18: RETRIES, QUARANTINE, STATUS LOOKUP
    # --------------------------------------------------------

    def _retry(self, action):
        """Run a write; retry with a short pause if the database is busy."""

        delay = 0.05

        for attempt in range(self.retries + 1):
            try:
                return action()
            except sqlite3.OperationalError as exc:
                text = str(exc).lower()
                busy = "locked" in text or "busy" in text

                if not busy:
                    raise

                if attempt == self.retries:
                    raise DatabaseBusyError(
                        f"Database stayed busy after {self.retries + 1} tries: {exc}"
                    ) from exc

                time.sleep(delay)
                delay *= 2

    def save_orders(self, orders: Iterable[Order], rejected=None,
                    resolve=None) -> int:
        """Store new orders. If any is refused, none are stored."""
        orders = list(orders)
        rejected = list(rejected or [])
        resolve = list(resolve or [])
        return self._retry(
            lambda: self._save_orders_once(orders, rejected, resolve)
        )

    def mark_cancelled(self, order_id: str) -> bool:
        """Mark an open or partly filled order cancelled. False if none."""
        return self._retry(lambda: self._mark_cancelled_once(order_id))

    def save_trades(self, trades: Iterable, rejected=None, resolve=None) -> int:
        """Store trades and update fills. The whole batch or nothing."""
        trades = list(trades)
        rejected = list(rejected or [])
        resolve = list(resolve or [])
        return self._retry(
            lambda: self._save_trades_once(trades, rejected, resolve)
        )

    def statuses(self, ids: Iterable[str]) -> dict[str, str]:
        """Status of each known order ID (unknown IDs are left out)."""

        ids = list(set(ids))
        found: dict[str, str] = {}

        for chunk in _chunks(ids):
            marks = ",".join("?" * len(chunk))
            found.update(self.conn.execute(
                f"SELECT order_id, status FROM orders "
                f"WHERE order_id IN ({marks})", chunk
            ))

        return found

    def save_rejected(self, records: Iterable[tuple]) -> int:
        """Quarantine records as (kind, ref, reason, payload) tuples."""

        rows = list(records)

        if not rows:
            return 0

        def write():
            with self.conn:
                self.conn.executemany(
                    "INSERT INTO rejected (kind, ref, reason, payload) "
                    "VALUES (?, ?, ?, ?)", rows
                )

        self._retry(write)
        return len(rows)

    def rejected_records(self, unresolved_only: bool = True) -> list[tuple]:
        """(id, kind, ref, reason, payload) for quarantined records."""

        where = "WHERE resolved = 0 " if unresolved_only else ""
        return self.conn.execute(
            "SELECT id, kind, ref, reason, payload FROM rejected "
            + where + "ORDER BY id"
        ).fetchall()

    def count_rejected(self, unresolved_only: bool = True) -> int:
        where = "WHERE resolved = 0" if unresolved_only else ""
        return self.conn.execute(
            "SELECT COUNT(*) FROM rejected " + where
        ).fetchone()[0]

    def resolve_rejected(self, ids: Iterable[int]) -> int:
        ids = list(ids)

        def write():
            with self.conn:
                self.conn.executemany(
                    "UPDATE rejected SET resolved = 1 WHERE id = ?",
                    [(i,) for i in ids]
                )

        if ids:
            self._retry(write)

        return len(ids)

    # --------------------------------------------------------
    # TRADES
    # --------------------------------------------------------

    @staticmethod
    def _trade_fields(n: int, trade) -> tuple:
        try:
            if isinstance(trade, Mapping):
                fields = (trade["trade_id"], trade["buy_order_id"],
                          trade["sell_order_id"], trade["price"],
                          trade["quantity"])
            else:
                fields = (trade.trade_id, trade.buy_order_id,
                          trade.sell_order_id, trade.price, trade.quantity)
        except (KeyError, AttributeError) as exc:
            raise ValueError(f"Trade {n}: missing field {exc}") from exc

        trade_id, buy_id, sell_id, price, quantity = fields

        if not isinstance(trade_id, str) or not trade_id.strip():
            raise ValueError(f"Trade {n}: trade ID cannot be empty")

        if not isinstance(buy_id, str) or not isinstance(sell_id, str):
            raise ValueError(f"Trade {n}: order IDs must be text")

        if (isinstance(price, bool) or not isinstance(price, (int, float))
                or not math.isfinite(price) or price <= 0):
            raise ValueError(f"Trade {n}: price must be finite and positive")

        if (isinstance(quantity, bool) or not isinstance(quantity, int)
                or not 0 < quantity <= _MAX_INT):
            raise ValueError(f"Trade {n}: quantity must be a positive integer")

        return trade_id, buy_id, sell_id, float(price), quantity

    def _order_row(self, order_id: str):
        return self.conn.execute(
            "SELECT side, price, quantity, filled, status "
            "FROM orders WHERE order_id = ?", (order_id,)
        ).fetchone()

    def _save_trades_once(self, trades: Iterable, rejected=(),
                          resolve=()) -> int:
        """Store trades and update each order's fill and status.

        The whole batch is saved or none of it is, including any quarantine
        rows saved with it.
        """

        trades = list(trades)

        if not trades and not rejected and not resolve:
            return 0

        with self.conn:
            self._write_quarantine(rejected, resolve)

            for n, trade in enumerate(trades, start=1):
                trade_id, buy_id, sell_id, price, qty = self._trade_fields(n, trade)

                buy = self._order_row(buy_id)
                sell = self._order_row(sell_id)

                if buy is None:
                    raise ValueError(f"Trade {n}: buy order {buy_id!r} not found")
                if sell is None:
                    raise ValueError(f"Trade {n}: sell order {sell_id!r} not found")
                if buy[0] != BUY:
                    raise ValueError(f"Trade {n}: order {buy_id!r} is not a buy order")
                if sell[0] != SELL:
                    raise ValueError(f"Trade {n}: order {sell_id!r} is not a sell order")

                for order_id, row in ((buy_id, buy), (sell_id, sell)):
                    if row[4] == "cancelled":
                        raise ValueError(
                            f"Trade {n}: order {order_id!r} is cancelled"
                        )

                if not sell[1] <= price <= buy[1]:
                    raise ValueError(
                        f"Trade {n}: price {price} outside "
                        f"[{sell[1]}, {buy[1]}]"
                    )

                try:
                    self.conn.execute(
                        "INSERT INTO trades (trade_id, buy_order_id, "
                        "sell_order_id, price, quantity) VALUES (?, ?, ?, ?, ?)",
                        (trade_id, buy_id, sell_id, price, qty)
                    )
                except sqlite3.IntegrityError as exc:
                    raise ValueError(
                        f"Trade {n}: duplicate trade ID {trade_id!r}"
                    ) from exc

                for order_id, row in ((buy_id, buy), (sell_id, sell)):
                    total = self.conn.execute(
                        "SELECT filled FROM orders WHERE order_id = ?",
                        (order_id,)
                    ).fetchone()[0] + qty

                    if total > row[2]:
                        raise ValueError(
                            f"Trade {n}: order {order_id!r} overfilled "
                            f"({total} > {row[2]})"
                        )

                    self.conn.execute(
                        "UPDATE orders SET filled = ?, status = ? "
                        "WHERE order_id = ?",
                        (total, "filled" if total == row[2] else "partial",
                         order_id)
                    )

                self.conn.execute(
                    "INSERT INTO events (kind, ref) VALUES ('trade', ?)",
                    (trade_id,)
                )

        return len(trades)

    def next_trade_number(self) -> int:
        return self.conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM trades"
        ).fetchone()[0]

    def trades_for(self, order_id: str) -> list[dict]:
        keys = ("trade_id", "buy_order_id", "sell_order_id", "price", "quantity")
        return [
            dict(zip(keys, row)) for row in self.conn.execute(
                "SELECT trade_id, buy_order_id, sell_order_id, price, quantity "
                "FROM trades WHERE buy_order_id = ? OR sell_order_id = ? "
                "ORDER BY seq", (order_id, order_id)
            )
        ]

    # --------------------------------------------------------
    # CHECKS AND COUNTS
    # --------------------------------------------------------

    def stats(self) -> dict:
        counts = dict(self.conn.execute(
            "SELECT status, COUNT(*) FROM orders GROUP BY status"
        ))
        traded = self.conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(quantity), 0) FROM trades"
        ).fetchone()

        return {
            "orders": sum(counts.values()),
            "open": counts.get("open", 0),
            "partial": counts.get("partial", 0),
            "filled": counts.get("filled", 0),
            "cancelled": counts.get("cancelled", 0),
            "trades": traded[0],
            "traded_quantity": traded[1],
        }

    def integrity_problems(self) -> list[str]:
        """Return anything inconsistent in the stored data (empty = healthy)."""

        problems: list[str] = []

        for row in self.conn.execute(
            "SELECT o.order_id, o.filled, COALESCE(t.q, 0) FROM orders o "
            "LEFT JOIN (SELECT oid, SUM(q) AS q FROM ("
            "  SELECT buy_order_id AS oid, quantity AS q FROM trades "
            "  UNION ALL "
            "  SELECT sell_order_id, quantity FROM trades) GROUP BY oid) t "
            "ON t.oid = o.order_id WHERE o.filled != COALESCE(t.q, 0)"
        ):
            problems.append(
                f"order {row[0]!r}: filled {row[1]} but its trades add up to {row[2]}"
            )

        for (order_id,) in self.conn.execute(
            "SELECT order_id FROM orders WHERE "
            "(status = 'filled' AND filled != quantity) OR "
            "(status = 'open' AND filled != 0) OR "
            "(status = 'partial' AND (filled <= 0 OR filled >= quantity))"
        ):
            problems.append(f"order {order_id!r}: status does not match its fill")

        for (trade_id,) in self.conn.execute(
            "SELECT t.trade_id FROM trades t "
            "JOIN orders b ON b.order_id = t.buy_order_id "
            "JOIN orders s ON s.order_id = t.sell_order_id "
            "WHERE b.side != 'B' OR s.side != 'S' "
            "OR t.price < s.price OR t.price > b.price"
        ):
            problems.append(f"trade {trade_id!r}: wrong sides or price out of range")

        verdict = self.conn.execute("PRAGMA integrity_check").fetchone()[0]
        if verdict != "ok":
            problems.append(f"database file check: {verdict}")

        return problems


# ============================================================
# A SIMPLE MATCHING ENGINE (STAND-IN)
# ============================================================

def reference_match(
    book: OrderBook,
    filled: Mapping[str, int] | None = None,
    first_trade_number: int = 1
) -> list[Trade]:
    """Price-time matching of the orders now resting in the book.

    Best price first, earliest arrival first. Each trade is priced at the
    order that arrived first (the resting order). Any other engine can be
    plugged in if it has this same signature and returns Trade objects.
    """

    filled = filled or {}
    orders = book.get_orders_in_arrival_order()
    arrival = {o.order_id: i for i, o in enumerate(orders)}
    remaining = {o.order_id: o.quantity - filled.get(o.order_id, 0)
                 for o in orders}

    buys = sorted(
        (o for o in orders if o.side == BUY and remaining[o.order_id] > 0),
        key=lambda o: (-o.price, arrival[o.order_id])
    )
    sells = sorted(
        (o for o in orders if o.side == SELL and remaining[o.order_id] > 0),
        key=lambda o: (o.price, arrival[o.order_id])
    )

    trades: list[Trade] = []
    i = j = 0
    number = first_trade_number

    while i < len(buys) and j < len(sells):
        buy, sell = buys[i], sells[j]

        if buy.price < sell.price:
            break

        qty = min(remaining[buy.order_id], remaining[sell.order_id])
        price = buy.price if arrival[buy.order_id] < arrival[sell.order_id] \
            else sell.price

        trades.append(Trade(f"T{number}", buy.order_id, sell.order_id, price, qty))
        number += 1

        remaining[buy.order_id] -= qty
        remaining[sell.order_id] -= qty

        if remaining[buy.order_id] == 0:
            i += 1
        if remaining[sell.order_id] == 0:
            j += 1

    return trades


# ============================================================
# ORDER BOOK + DATABASE KEPT IN STEP
# ============================================================

class PersistentBook:

    def __init__(self, db: OrderDatabase | None = None,
                 book: OrderBook | None = None) -> None:
        self.db = db if db is not None else OrderDatabase()
        self.book = book if book is not None else OrderBook()
        self.needs_reconcile = False       # set if a cleanup failed after a commit

    @classmethod
    def restore(cls, db: OrderDatabase) -> "PersistentBook":
        """Rebuild the in-memory book from the database after a restart."""

        book = OrderBook()

        for order in db.load_open_orders():
            book.add_order(order)

        return cls(db, book)

    def submit(self, events: Iterable[Mapping], rejected=None,
               resolve=None) -> int:
        """Add order events to the book and the database.

        An ID that the database has seen before (even a cancelled one) is
        refused before anything changes. If the book rejects an event partway
        through, the earlier events stay in both the book and the database.
        """

        events = list(events)

        ids = [e["order_id"] for e in events
               if isinstance(e, Mapping) and isinstance(e.get("order_id"), str)]
        used = self.db.known_ids(ids)

        if used:
            for n, event in enumerate(events, start=1):
                if isinstance(event, Mapping) and event.get("order_id") in used:
                    raise ValueError(
                        f"Firehose event {n}: order ID {event['order_id']!r} "
                        "was used before (the database never reuses IDs)"
                    )

        before = len(self.book)
        error = None
        result = 0

        try:
            result = self.book.ingest_firehose(events)
        except Exception as exc:                          # noqa: BLE001
            error = exc

        added = self.book.get_orders_in_arrival_order()[before:]

        if added or rejected or resolve:
            try:
                self.db.save_orders(added, rejected, resolve)
            except Exception:
                # The database refused or failed: undo the book so the two
                # never disagree, then report the real problem.
                for order in added:
                    self.book.cancel_order(order.order_id)
                raise

        if error is not None:
            raise error

        return result

    def cancel(self, order_id: str) -> bool:
        if self.book.get_order(order_id) is None:
            return False

        self.db.mark_cancelled(order_id)       # if this fails, nothing changed
        self.book.cancel_order(order_id)
        return True

    def record_trades(self, trades: Iterable[Trade]) -> int:
        """Check trades against the book, save them, drop filled orders."""

        trades = list(trades)

        if not trades:
            return 0

        validate_trades(self.book, trades)
        saved = self.db.save_trades(trades)

        touched = ({t.buy_order_id for t in trades}
                   | {t.sell_order_id for t in trades})
        self._drop_filled(touched)

        return saved

    def _drop_filled(self, touched: set) -> None:
        """Remove fully filled orders from the book. The trades are already
        saved, so a failure here is not reported as a failed save; the book is
        flagged and repaired before the next engine run."""

        try:
            for order_id in self.db.filled_ids(touched):
                self.book.cancel_order(order_id)
        except sqlite3.Error:
            self.needs_reconcile = True

    def run_engine(self, engine=reference_match, tolerant: bool = False) -> int:
        """Run a matching engine on the book and store what it produces.

        engine(book, filled, first_trade_number) must return Trade objects.
        With tolerant=True, bad trades are quarantined and the good ones are
        still stored; otherwise one bad trade refuses the whole batch.
        """

        if self.needs_reconcile:
            self.reconcile(repair=True)

        trades = list(engine(
            self.book, self.db.filled_map(), self.db.next_trade_number()
        ))

        if tolerant:
            return self.record_trades_tolerant(trades)["saved"]

        return self.record_trades(trades)

    # --------------------------------------------------------
    # DAY 18: MISSING AND FAILED RECORDS
    # --------------------------------------------------------

    @staticmethod
    def _event_problem(event, known: set, seen: set):
        """Why an order event cannot be stored, or None if it is fine."""

        if not isinstance(event, Mapping):
            return "event is not a mapping"

        for field in ("order_id", "side", "price", "quantity"):
            if field not in event:
                return f"missing field {field!r}"

        try:
            _check_fields("", event["order_id"], event["side"],
                          event["price"], event["quantity"])
        except ValueError as exc:
            return str(exc).lstrip(": ")

        if event["quantity"] > _MAX_INT:
            return "quantity too large for the database"

        order_id = event["order_id"]

        if order_id in known:
            return f"order ID {order_id!r} was used before"

        if order_id in seen:
            return f"duplicate order ID {order_id!r} in this batch"

        seen.add(order_id)
        return None

    def submit_tolerant(self, events: Iterable, quarantine: bool = True,
                        resolves=None) -> dict:
        """Store every good order event; quarantine the bad ones.

        Unlike submit(), a bad event never stops the others. Quarantined
        events (with the reason and the original data) go to the rejected
        table so nothing is silently lost.
        """

        events = list(events)
        ids = [e["order_id"] for e in events
               if isinstance(e, Mapping) and isinstance(e.get("order_id"), str)]
        known = self.db.known_ids(ids)
        seen: set = set()

        good, good_positions, rejected = [], [], []

        for n, event in enumerate(events, start=1):
            reason = self._event_problem(event, known, seen)

            if reason is None:
                good.append(event)
                good_positions.append(n)
            else:
                ref = event.get("order_id") if isinstance(event, Mapping) else None
                rejected.append((
                    "order", ref if isinstance(ref, str) else None,
                    f"Firehose event {n}: {reason}", _to_json(event)
                ))

        good_resolve = ([resolves[p - 1] for p in good_positions]
                        if resolves else None)

        if good or (quarantine and rejected) or good_resolve:
            self.submit(good, rejected=rejected if quarantine else None,
                        resolve=good_resolve)

        return {"accepted": len(good), "rejected": len(rejected),
                "accepted_positions": good_positions}

    def record_trades_tolerant(self, trades: Iterable, max_failures: int = 100,
                               quarantine: bool = True, resolves=None) -> dict:
        """Store every good trade; quarantine the bad ones.

        Raises RuntimeError (and stores nothing) if more than max_failures
        trades are bad, which usually means the engine itself is broken.
        """

        trades = list(trades)
        rejected: list[tuple] = []          # (position, ref, reason, payload)
        items: list[tuple] = []             # (position, Trade)

        for n, trade in enumerate(trades, start=1):
            if isinstance(trade, Trade):
                items.append((n, trade))
            elif isinstance(trade, Mapping):
                try:
                    items.append((n, Trade(
                        trade["trade_id"], trade["buy_order_id"],
                        trade["sell_order_id"], trade["price"],
                        trade["quantity"])))
                except KeyError as exc:
                    rejected.append((n, _trade_ref(trade),
                                     f"missing field {exc}", _to_json(trade)))
            else:
                rejected.append((n, None, "not a trade", _to_json(trade)))

        problems: dict[int, list[str]] = {}
        for problem in check_trades(self.book, [t for _, t in items]):
            match = re.match(r"Trade (\d+): (.*)", problem)
            problems.setdefault(int(match.group(1)), []).append(match.group(2))

        good: list[tuple] = []
        for k, (n, trade) in enumerate(items, start=1):
            if k in problems:
                rejected.append((n, trade.trade_id, "; ".join(problems[k]),
                                 _to_json(trade)))
            else:
                good.append((n, trade))

        def too_many():
            return RuntimeError(
                f"{len(rejected)} bad trades (limit {max_failures}); "
                "nothing was stored"
            )

        if len(rejected) > max_failures:
            raise too_many()

        while True:
            try:
                saved = self.db.save_trades(
                    [t for _, t in good],
                    rejected=[("trade", ref, reason, payload)
                              for _, ref, reason, payload in rejected]
                    if quarantine else None,
                    resolve=[resolves[n - 1] for n, _ in good]
                    if resolves else None,
                )
                break
            except ValueError as exc:
                match = re.match(r"Trade (\d+): (.*)", str(exc))
                if not match:
                    raise
                n, trade = good.pop(int(match.group(1)) - 1)
                rejected.append((n, trade.trade_id, match.group(2),
                                 _to_json(trade)))
                if len(rejected) > max_failures:
                    raise too_many() from exc

        if good:
            self._drop_filled({t.buy_order_id for _, t in good}
                              | {t.sell_order_id for _, t in good})

        return {"saved": saved, "rejected": len(rejected),
                "saved_positions": [n for n, _ in good]}

    def reconcile(self, repair: bool = False) -> dict:
        """Compare the in-memory book with the database.

        Finds orders the database never received (unsaved), orders that are
        finished but still resting in the book (stale), orders open in the
        database but missing from the book (missing) and orders whose terms
        differ (mismatched). With repair=True the first three are fixed;
        mismatches are only reported. A re-added order goes to the END of the
        book's arrival order; a restart restores the original order.
        """

        book_orders = {o.order_id: o
                       for o in self.book.get_orders_in_arrival_order()}
        db_open = {o.order_id: o for o in self.db.load_open_orders()}
        statuses = self.db.statuses(list(book_orders))

        unsaved = [i for i in book_orders if i not in statuses]
        stale = [i for i, st in statuses.items() if st in ("filled", "cancelled")]
        missing = [i for i in db_open if i not in book_orders]
        mismatched = [
            i for i in book_orders if i in db_open and
            (book_orders[i].side, book_orders[i].price, book_orders[i].quantity)
            != (db_open[i].side, db_open[i].price, db_open[i].quantity)
        ]

        consistent_before = not (unsaved or stale or missing or mismatched)
        repaired = 0

        if repair:
            if unsaved:
                self.db.save_orders([book_orders[i] for i in unsaved])
                repaired += len(unsaved)
            for order_id in stale:
                self.book.cancel_order(order_id)
                repaired += 1
            for order_id in missing:
                self.book.add_order(db_open[order_id])
                repaired += 1

        if repair:
            self.needs_reconcile = False

        return {
            "unsaved_in_database": unsaved,
            "stale_in_book": stale,
            "missing_in_book": missing,
            "mismatched": mismatched,
            "repaired": repaired,
            "unresolved_rejected": self.db.count_rejected(),
            "consistent_before": consistent_before,
            "consistent_after": (not mismatched) if repair else consistent_before,
        }

    def retry_rejected(self) -> dict:
        """Try every unresolved quarantined order and trade again.

        Useful when the cause was timing, for example a trade that arrived
        before its orders. Records that now succeed are marked resolved; the
        rest stay in quarantine (they are not quarantined a second time).
        """

        rows = self.db.rejected_records()
        order_rows, trade_rows = [], []

        for rid, kind, _ref, _reason, payload in rows:
            try:
                data = json.loads(payload) if payload else None
            except ValueError:
                continue

            if isinstance(data, dict):
                (order_rows if kind == "order" else trade_rows
                 if kind == "trade" else []).append((rid, data))

        resolved_orders = resolved_trades = 0

        if order_rows:
            result = self.submit_tolerant(
                [d for _, d in order_rows], quarantine=False,
                resolves=[rid for rid, _ in order_rows])
            resolved_orders = len(result["accepted_positions"])

        if trade_rows:
            result = self.record_trades_tolerant(
                [d for _, d in trade_rows], max_failures=len(trade_rows),
                quarantine=False, resolves=[rid for rid, _ in trade_rows])
            resolved_trades = len(result["saved_positions"])

        return {"orders_resolved": resolved_orders,
                "trades_resolved": resolved_trades,
                "still_failing": self.db.count_rejected()}


# ============================================================
# DEMO / LOAD RUN
# ============================================================

def run_database_demo(n_orders: int = 50_000, path: str | None = None) -> dict:
    """Orders -> book + database -> matching -> trades stored -> restart."""

    own_dir = None

    if path is None:
        own_dir = tempfile.TemporaryDirectory()
        path = os.path.join(own_dir.name, "orders.db")
    elif os.path.exists(path):
        raise FileExistsError(f"{path} already exists; pick a new file name")

    def timed(fn):
        start = time.perf_counter()
        value = fn()
        return value, max(time.perf_counter() - start, 1e-9)

    events = make_load_events(n_orders, seed=15)
    live = PersistentBook(OrderDatabase(path))

    def submit_all():
        for start in range(0, n_orders, 1000):
            live.submit(events[start:start + 1000])

    try:
        _, submit_s = timed(submit_all)
        trades, match_s = timed(live.run_engine)
        stats = live.db.stats()
        second_run = live.run_engine()
        problems = live.db.integrity_problems()

        live_orders = live.book.get_orders_in_arrival_order()
        live.db.close()

        reopened = OrderDatabase(path)
        restored, restore_s = timed(lambda: PersistentBook.restore(reopened))
        same_book = (restored.book.get_orders_in_arrival_order() == live_orders)
        reopened_problems = reopened.integrity_problems()
        reopened.close()

        checks = {
            "all orders stored": stats["orders"] == n_orders,
            "trades stored": stats["trades"] == trades and trades > 0,
            "matching again finds nothing": second_run == 0,
            "database is consistent": not problems,
            "restored book equals live book": same_book,
            "restored database is consistent": not reopened_problems,
        }

        return {
            "orders": n_orders, "trades": trades,
            "traded_quantity": stats["traded_quantity"],
            "filled": stats["filled"], "partial": stats["partial"],
            "open": stats["open"],
            "submit_s": submit_s, "match_s": match_s, "restore_s": restore_s,
            "submit_rate": n_orders / submit_s,
            "match_rate": n_orders / match_s,
            "restore_rate": max(len(live_orders), 1) / restore_s,
            "checks": checks, "ok": all(checks.values()),
        }
    finally:
        if own_dir is not None:
            own_dir.cleanup()


def format_demo_report(r: dict) -> str:
    lines = [
        f"DATABASE LAYER RUN: {r['orders']:,} orders, {r['trades']:,} trades "
        f"({r['traded_quantity']:,} units traded)",
        f"  orders left: {r['open']:,} open, {r['partial']:,} partly filled; "
        f"{r['filled']:,} fully filled",
        f"  submit (book + database)  {r['submit_s']:8.3f} s  {r['submit_rate']:>10,.0f} orders/s",
        f"  match + store trades      {r['match_s']:8.3f} s  {r['match_rate']:>10,.0f} orders/s",
        f"  restore after restart     {r['restore_s']:8.3f} s  {r['restore_rate']:>10,.0f} orders/s",
        "  checks:",
    ]
    lines += [f"    {'ok  ' if ok else 'FAIL'} {name}"
              for name, ok in r["checks"].items()]
    lines.append("  RESULT: " + ("ALL CHECKS PASSED" if r["ok"] else "FAILED"))
    return "\n".join(lines)


# ============================================================
# DAY 17: TRADE PERSISTENCE VALIDATION
# ============================================================

# Re-checks stored trades from scratch, without trusting the code that wrote
# them: the trade rules, fill totals, the event log, trade/cancel ordering and
# whether the resting book is uncrossed. run_persistence_validation() also
# proves that restarting in the middle of a run changes nothing.

def validate_trade_persistence(
    db: "OrderDatabase",
    expect_matched: bool = True
) -> dict:
    """Validate everything the database holds about orders and trades.

    expect_matched=True also requires that no resting buy price is at or above
    a resting sell price (true when the engine ran after the last order).
    """

    checks: dict[str, bool] = {}
    problems: list[str] = []

    def note(name: str, found: list[str]) -> None:
        checks[name] = not found
        problems.extend(f"{name}: {text}" for text in found[:5])

    note("database file and order fills are consistent",
         db.integrity_problems())

    # Rebuild a book holding every order ever stored, then replay the trades
    # through the same trade rules used on Day 8.
    book = OrderBook()
    for order_id, side, price, quantity in db.conn.execute(
        "SELECT order_id, side, price, quantity FROM orders ORDER BY seq"
    ):
        book.add_order(Order(order_id, side, price, quantity))

    trades = [
        Trade(*row) for row in db.conn.execute(
            "SELECT trade_id, buy_order_id, sell_order_id, price, quantity "
            "FROM trades ORDER BY seq"
        )
    ]
    note("every stored trade passes the trade rules",
         check_trades(book, trades))

    sums = dict(db.conn.execute(
        "SELECT side, COALESCE(SUM(filled), 0) FROM orders GROUP BY side"
    ))
    traded = db.conn.execute(
        "SELECT COALESCE(SUM(quantity), 0) FROM trades"
    ).fetchone()[0]
    found = []
    for side, label in ((BUY, "buyers"), (SELL, "sellers")):
        if sums.get(side, 0) != traded:
            found.append(f"{label} filled {sums.get(side, 0)} "
                         f"but trades total {traded}")
    note("buyer and seller fills balance", found)

    counts = dict(db.conn.execute(
        "SELECT kind, COUNT(*) FROM events GROUP BY kind"
    ))
    stats = db.stats()
    found = [
        f"{kind}: {counts.get(kind, 0)} events but {expected} rows"
        for kind, expected in (("order", stats["orders"]),
                               ("trade", stats["trades"]),
                               ("cancel", stats["cancelled"]))
        if counts.get(kind, 0) != expected
    ]
    note("event log matches the tables", found)

    note("no trade happens after its order was cancelled", [
        f"trade {t!r} came after the cancel of {o!r}"
        for t, o in db.conn.execute(
            "SELECT t.trade_id, o.order_id FROM trades t "
            "JOIN orders o ON o.order_id IN (t.buy_order_id, t.sell_order_id) "
            "JOIN events c ON c.kind = 'cancel' AND c.ref = o.order_id "
            "JOIN events e ON e.kind = 'trade' AND e.ref = t.trade_id "
            "WHERE e.seq > c.seq"
        )
    ])

    note("every trade comes after both of its orders", [
        f"trade {t!r} is older than one of its orders"
        for (t,) in db.conn.execute(
            "SELECT t.trade_id FROM trades t "
            "JOIN events e ON e.kind = 'trade' AND e.ref = t.trade_id "
            "JOIN events b ON b.kind = 'order' AND b.ref = t.buy_order_id "
            "JOIN events s ON s.kind = 'order' AND s.ref = t.sell_order_id "
            "WHERE e.seq < b.seq OR e.seq < s.seq"
        )
    ])

    if expect_matched:
        bid = db.conn.execute(
            "SELECT MAX(price) FROM orders "
            "WHERE side = 'B' AND status IN ('open', 'partial')"
        ).fetchone()[0]
        ask = db.conn.execute(
            "SELECT MIN(price) FROM orders "
            "WHERE side = 'S' AND status IN ('open', 'partial')"
        ).fetchone()[0]
        crossed = bid is not None and ask is not None and bid >= ask
        note("resting orders do not cross",
             [f"best resting buy {bid} is at or above best resting sell {ask}"]
             if crossed else [])

    return {
        "checks": checks,
        "problems": problems,
        "orders": stats["orders"],
        "trades": stats["trades"],
        "ok": all(checks.values()),
    }


def _persistence_round(pb: "PersistentBook", chunk: list[dict]) -> int:
    """One round of work: submit, cancel every tenth order, then match."""

    pb.submit(chunk)

    for i, event in enumerate(chunk):
        if i % 10 == 5:
            pb.cancel(event["order_id"])

    return pb.run_engine()


def _persistence_snapshot(pb: "PersistentBook") -> tuple:
    conn = pb.db.conn
    return (
        conn.execute(
            "SELECT trade_id, buy_order_id, sell_order_id, price, quantity "
            "FROM trades ORDER BY seq"
        ).fetchall(),
        conn.execute(
            "SELECT order_id, side, price, quantity, filled, status "
            "FROM orders ORDER BY seq"
        ).fetchall(),
        pb.book.get_orders_in_arrival_order(),
    )


def run_persistence_validation(
    n_orders: int = 50_000,
    path: str | None = None,
    rounds: int = 10
) -> dict:
    """Run the same work twice, once straight through and once restarting the
    database between rounds, then compare everything and validate the result."""

    if n_orders < 1 or rounds < 1:
        raise ValueError("n_orders and rounds must be at least 1")

    events = make_load_events(n_orders, seed=17)
    size = -(-n_orders // rounds)
    chunks = [events[i:i + size] for i in range(0, n_orders, size)]

    own_dir = None
    if path is None:
        own_dir = tempfile.TemporaryDirectory()
        path = os.path.join(own_dir.name, "persist.db")
    elif os.path.exists(path):
        raise FileExistsError(f"{path} already exists; pick a new file name")

    control = PersistentBook(OrderDatabase())            # never restarted
    live = None
    restarts = 0

    try:
        start = time.perf_counter()
        for chunk in chunks:
            _persistence_round(control, chunk)
        control_s = max(time.perf_counter() - start, 1e-9)

        start = time.perf_counter()
        live = PersistentBook(OrderDatabase(path))
        for k, chunk in enumerate(chunks):
            if k > 0:                                    # simulate a restart
                live.db.close()
                live = PersistentBook.restore(OrderDatabase(path))
                restarts += 1
            _persistence_round(live, chunk)

        before = live.db.stats()
        try:
            live.db.save_trades([Trade("BAD1", "no-such-buy", "no-such-sell", 1.0, 1)])
            refused = False
        except ValueError:
            refused = True
        unchanged = live.db.stats() == before

        live.db.close()
        final = PersistentBook.restore(OrderDatabase(path))
        live = final
        restarts += 1
        restart_s = max(time.perf_counter() - start, 1e-9)

        validation = validate_trade_persistence(final.db)
        snap_a = _persistence_snapshot(control)
        snap_b = _persistence_snapshot(final)

        # The validator must also notice damage: break one fill and re-check.
        row = control.db.conn.execute(
            "SELECT order_id FROM orders WHERE filled > 0 LIMIT 1"
        ).fetchone()
        caught = False
        if row is not None:
            with control.db.conn:
                control.db.conn.execute(
                    "UPDATE orders SET filled = filled - 1 WHERE order_id = ?",
                    (row[0],)
                )
            caught = not validate_trade_persistence(control.db)["ok"]

        checks = dict(validation["checks"])
        checks["restarts do not change the trades"] = snap_a[0] == snap_b[0]
        checks["restarts do not change fills and statuses"] = snap_a[1] == snap_b[1]
        checks["restored book equals the uninterrupted book"] = snap_a[2] == snap_b[2]
        checks["a bad trade batch is refused and changes nothing"] = refused and unchanged
        checks["validator catches a damaged fill"] = caught
        checks["some trades were stored"] = len(snap_b[0]) > 0

        return {
            "orders": n_orders, "rounds": len(chunks), "restarts": restarts,
            "trades": len(snap_b[0]),
            "control_s": control_s, "restart_s": restart_s,
            "checks": checks, "problems": validation["problems"],
            "ok": all(checks.values()),
        }
    finally:
        control.db.close()
        if live is not None:
            try:
                live.db.close()
            except sqlite3.Error:
                pass
        if own_dir is not None:
            own_dir.cleanup()


def format_persistence_report(r: dict) -> str:
    lines = [
        f"TRADE PERSISTENCE: {r['orders']:,} orders in {r['rounds']} rounds, "
        f"{r['restarts']} restarts, {r['trades']:,} trades stored",
        f"  straight through     {r['control_s']:8.3f} s",
        f"  with restarts        {r['restart_s']:8.3f} s",
        "  checks:",
    ]
    lines += [f"    {'ok  ' if ok else 'FAIL'} {name}"
              for name, ok in r["checks"].items()]
    lines += [f"    problem: {p}" for p in r["problems"]]
    lines.append("  RESULT: " + ("ALL CHECKS PASSED" if r["ok"] else "FAILED"))
    return "\n".join(lines)


def validate_database_file(path: str, expect_matched: bool = True) -> dict:
    """Validate an existing database file. The file is opened read-only."""

    db = OrderDatabase(path, read_only=True)

    try:
        result = validate_trade_persistence(db, expect_matched)
    finally:
        db.close()

    result["path"] = path
    return result


def format_validation_report(r: dict) -> str:
    lines = [
        f"TRADE PERSISTENCE CHECK: {r['path']}",
        f"  {r['orders']:,} orders, {r['trades']:,} trades",
        "  checks:",
    ]
    lines += [f"    {'ok  ' if ok else 'FAIL'} {name}"
              for name, ok in r["checks"].items()]
    lines += [f"    problem: {p}" for p in r["problems"]]
    lines.append("  RESULT: " + ("ALL CHECKS PASSED" if r["ok"] else "FAILED"))
    return "\n".join(lines)


# ============================================================
# DAY 18: HANDLING MISSING AND FAILED RECORDS (DEMO RUN)
# ============================================================

def run_failure_handling_demo(n_orders: int = 20_000) -> dict:
    """Feed the database layer bad orders, bad trades and a failing database,
    then check that nothing is lost, nothing disagrees and good data survives."""

    events = make_load_events(n_orders, seed=18)

    breakers = [
        lambda e: {k: v for k, v in e.items() if k != "price"},     # missing field
        lambda e: {**e, "price": -1.0},                              # negative price
        lambda e: {**e, "quantity": 0},                              # zero quantity
        lambda e: {**e, "price": float("nan")},                      # NaN price
        lambda e: {**e, "side": "X"},                                # bad side
        lambda e: None,                                              # not a mapping
        lambda e: {**e, "order_id": "L0"},                           # reused ID
        lambda e: {**e, "order_id": "   "},                          # blank ID
        lambda e: {**e, "quantity": 2.5},                            # fractional
        lambda e: {**e, "price": True},                              # boolean
    ]
    expected_bad = 0
    for i in range(n_orders):
        if i % 50 == 25:
            events[i] = breakers[(i // 50) % len(breakers)](events[i])
            expected_bad += 1

    checks: dict[str, bool] = {}
    folder = tempfile.TemporaryDirectory()
    path = os.path.join(folder.name, "failures.db")
    pb = PersistentBook(OrderDatabase(path))
    other = None

    try:
        # 1. bad order events are quarantined, the good ones stored
        accepted = rejected = 0
        for start in range(0, n_orders, 1000):
            result = pb.submit_tolerant(events[start:start + 1000])
            accepted += result["accepted"]
            rejected += result["rejected"]

        stats = pb.db.stats()
        checks["bad order events are quarantined, good ones stored"] = (
            rejected == expected_bad and accepted == n_orders - expected_bad
            and stats["orders"] == accepted and len(pb.book) == accepted
            and pb.db.count_rejected() == expected_bad
        )
        sample = pb.db.rejected_records()
        checks["every quarantined order keeps its reason and original data"] = all(
            reason and (payload is not None) for _, _, _, reason, payload in sample
        )

        # 2. an ID that was cancelled can never come back
        first_id = events[0]["order_id"]
        pb.cancel(first_id)
        again = pb.submit_tolerant([events[0]], quarantine=False)
        checks["a cancelled order's ID cannot come back"] = (
            again["accepted"] == 0 and again["rejected"] == 1
        )

        # 3. bad trades are quarantined, good trades stored
        def messy_engine(book, filled, first_number):
            real = reference_match(book, filled, first_number)
            r0 = real[0]
            return real + [
                Trade("BADT1", "NO-SUCH-BUY", "NO-SUCH-SELL", 100.0, 1),
                Trade("BADT2", r0.buy_order_id, r0.sell_order_id, r0.price, 10 ** 9),
                Trade("BADT3", r0.sell_order_id, r0.buy_order_id, r0.price, 1),
                Trade("BADT4", r0.buy_order_id, r0.sell_order_id, 10 ** 6, 1),
                Trade(r0.trade_id, r0.buy_order_id, r0.sell_order_id, r0.price, 1),
                "garbage",
                {"trade_id": "BADT7"},
            ], len(real)

        engine_out = {}

        def engine(book, filled, first_number):
            trades, engine_out["real"] = messy_engine(book, filled, first_number)
            return trades

        before_trades = pb.db.stats()["trades"]
        got = pb.record_trades_tolerant(
            list(engine(pb.book, pb.db.filled_map(), pb.db.next_trade_number()))
        )
        checks["bad trades are quarantined, good trades stored"] = (
            got["saved"] == engine_out["real"] and got["rejected"] == 7
            and pb.db.stats()["trades"] == before_trades + engine_out["real"]
        )
        checks["stored data still validates after quarantining"] = (
            validate_trade_persistence(pb.db)["ok"]
        )

        # 4. a failed database write rolls the book back
        book_before, db_before = len(pb.book), pb.db.stats()["orders"]
        extra = [{"order_id": f"F{i}", "side": BUY, "price": 1.0, "quantity": 1}
                 for i in range(3)]

        def boom(*_args):
            raise sqlite3.OperationalError("disk I/O error")

        pb.db._save_orders_once = boom
        try:
            pb.submit(extra)
            raised = False
        except sqlite3.OperationalError:
            raised = True
        finally:
            del pb.db._save_orders_once

        checks["a failed database write rolls the book back"] = (
            raised and len(pb.book) == book_before
            and pb.db.stats()["orders"] == db_before
        )
        pb.submit(extra)
        checks["the book works again after a failed write"] = (
            len(pb.book) == book_before + 3
            and pb.db.stats()["orders"] == db_before + 3
        )

        # 5. a locked database gives a clear error and changes nothing
        other = sqlite3.connect(path)
        other.execute("BEGIN EXCLUSIVE")
        pb.db.conn.execute("PRAGMA busy_timeout = 50")
        pb.db.retries = 1
        book_before = len(pb.book)
        try:
            pb.submit([{"order_id": "BUSY1", "side": BUY, "price": 1.0, "quantity": 1}])
            busy_error = False
        except DatabaseBusyError:
            busy_error = True
        checks["a busy database gives a clear error and changes nothing"] = (
            busy_error and len(pb.book) == book_before
        )
        other.rollback()
        other.close()
        other = None
        pb.db.conn.execute("PRAGMA busy_timeout = 5000")
        pb.db.retries = 3
        pb.submit([{"order_id": "BUSY1", "side": BUY, "price": 1.0, "quantity": 1}])
        checks["writes succeed again once the lock is released"] = (
            pb.db.get_order("BUSY1") is not None
        )

        # 6. book and database disagree: find it, then repair it
        pb.book.add_order(Order("GHOST1", BUY, 1.0, 1))                 # never saved
        open_one = pb.db.load_open_orders()[0]
        pb.book.cancel_order(open_one.order_id)                         # lost from book
        filled = pb.db.conn.execute(
            "SELECT order_id, side, price, quantity FROM orders "
            "WHERE status = 'filled' LIMIT 1"
        ).fetchone()
        pb.book.add_order(Order(*filled))                               # finished but resting

        found = pb.reconcile()
        checks["reconcile finds missing, unsaved and stale records"] = (
            found["unsaved_in_database"] == ["GHOST1"]
            and found["missing_in_book"] == [open_one.order_id]
            and found["stale_in_book"] == [filled[0]]
            and not found["consistent_before"]
        )
        pb.reconcile(repair=True)
        checks["repair makes the book and database agree"] = (
            pb.reconcile()["consistent_before"]
        )

        # 7. a trade that arrived before its orders is stored after a retry
        def late_engine(book, filled, first_number):
            return [Trade("TLATE", "LATE1", "LATE2", 100.0, 5)]

        pb.run_engine(late_engine, tolerant=True)                       # orders not here yet
        unresolved_before = pb.db.count_rejected()
        pb.submit([
            {"order_id": "LATE1", "side": BUY, "price": 101.0, "quantity": 5},
            {"order_id": "LATE2", "side": SELL, "price": 100.0, "quantity": 5},
        ])
        retried = pb.retry_rejected()
        late = pb.db.get_order("LATE1")
        checks["a trade for a late-arriving order is stored after a retry"] = (
            retried["trades_resolved"] == 1 and late["status"] == "filled"
        )
        checks["retrying leaves unrelated failures in quarantine"] = (
            retried["still_failing"] == unresolved_before - 1
            and retried["orders_resolved"] == 0
        )
        checks["nothing crosses and the data validates at the end"] = (
            validate_trade_persistence(pb.db)["ok"]
        )

        return {
            "orders": n_orders, "bad_events": expected_bad,
            "quarantined": pb.db.count_rejected(),
            "quarantined_ever": pb.db.count_rejected(unresolved_only=False),
            "trades": pb.db.stats()["trades"],
            "checks": checks, "ok": all(checks.values()),
        }
    finally:
        if other is not None:
            other.close()
        pb.db.close()
        folder.cleanup()


def format_failure_report(r: dict) -> str:
    lines = [
        f"FAILURE HANDLING: {r['orders']:,} order events ({r['bad_events']:,} damaged), "
        f"{r['trades']:,} trades stored",
        f"  still in quarantine: {r['quarantined']:,} "
        f"({r['quarantined_ever']:,} quarantined in total)",
        "  checks:",
    ]
    lines += [f"    {'ok  ' if ok else 'FAIL'} {name}"
              for name, ok in r["checks"].items()]
    lines.append("  RESULT: " + ("ALL CHECKS PASSED" if r["ok"] else "FAILED"))
    return "\n".join(lines)


# ============================================================
# DAY 19: RECOVERY AND RESILIENCY TESTING
# ============================================================

# Proves the data survives trouble: real processes killed mid-run, a failure
# injected at every single database statement, an uncommitted transaction cut
# off, a damaged database file replaced from a backup, and the same events
# delivered twice. The common test is always the same: after the trouble, the
# data is valid and the final result equals a run that never had any trouble.

class FaultInjectingConnection:
    """Wraps a sqlite3 connection and fails on one chosen statement.

    A testing aid: statement number fail_at raises an OperationalError, as a
    disk error would. Everything else passes through to the real connection.
    """

    def __init__(self, conn, fail_at: int | None = None) -> None:
        self._conn = conn
        self.fail_at = fail_at
        self.statements = 0

    def _tick(self) -> None:
        self.statements += 1

        if self.fail_at is not None and self.statements == self.fail_at:
            raise sqlite3.OperationalError("disk I/O error (injected)")

    def execute(self, *args, **kwargs):
        self._tick()
        return self._conn.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        self._tick()
        return self._conn.executemany(*args, **kwargs)

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, *exc):
        return self._conn.__exit__(*exc)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _raw(db: "OrderDatabase"):
    return getattr(db.conn, "_conn", db.conn)


def _db_dump(db: "OrderDatabase") -> tuple:
    conn = _raw(db)
    return tuple(
        conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
        for table in ("orders", "trades", "events", "rejected")
    )


def fault_sweep(make_state, operation):
    """Run operation(pb) once per database statement, failing that statement.

    After every failure the data must be all-or-nothing: exactly as it was
    before, or exactly the finished result (a failure in a read that comes
    after the commit can still raise). If only the book cleanup was cut short,
    the book must be repairable. Running the operation again must always end
    in the same final state as a run with no failure.
    Returns (statements_tested, all_ok).
    """

    pb = make_state()
    counter = FaultInjectingConnection(pb.db.conn)
    pb.db.conn = counter
    operation(pb)
    total = counter.statements
    pb.db.conn = counter._conn
    final = (_db_dump(pb.db), pb.book.get_orders_in_arrival_order())
    pb.db.close()

    all_ok = total > 0

    for k in range(1, total + 1):
        pb = make_state()
        before = (_db_dump(pb.db), pb.book.get_orders_in_arrival_order())
        pb.db.conn = FaultInjectingConnection(pb.db.conn, fail_at=k)

        try:
            operation(pb)
        except sqlite3.OperationalError:
            pass

        pb.db.conn = pb.db.conn._conn

        if pb.needs_reconcile:
            # The work was committed; only the book cleanup was cut short.
            pb.reconcile(repair=True)
            step_ok = (_db_dump(pb.db),
                       pb.book.get_orders_in_arrival_order()) == final
        else:
            after = (_db_dump(pb.db), pb.book.get_orders_in_arrival_order())
            all_or_nothing = after == before or after == final

            if after != final:
                try:
                    operation(pb)
                except Exception:                       # noqa: BLE001
                    pass                                # not recoverable: the check below fails

            recovered = (_db_dump(pb.db),
                         pb.book.get_orders_in_arrival_order()) == final
            step_ok = all_or_nothing and recovered

        all_ok = all_ok and step_ok
        pb.db.close()

    return total, all_ok


# ---------- backup, health check, recovery ----------

def backup_database(source_path: str, dest_path: str) -> str:
    """Make a consistent copy of a database, even while it is in use."""

    if os.path.exists(dest_path):
        raise FileExistsError(f"{dest_path} already exists")

    source = sqlite3.connect(source_path)
    target = sqlite3.connect(dest_path)

    try:
        source.backup(target)
    finally:
        target.close()
        source.close()

    return dest_path


def database_health(path: str) -> tuple[bool, str]:
    """(healthy, message). Opening the file also lets SQLite finish any
    recovery left over from a crash."""

    if not os.path.exists(path):
        return False, "file does not exist"

    try:
        conn = sqlite3.connect(path)

        try:
            verdict = conn.execute("PRAGMA integrity_check").fetchone()[0]
            tables = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'")}
        finally:
            conn.close()
    except sqlite3.DatabaseError as exc:
        return False, str(exc)

    missing = {"orders", "trades", "events"} - tables

    if missing:
        return False, "missing tables: " + ", ".join(sorted(missing))

    return verdict == "ok", verdict


def recover_database(path: str, backup_path: str) -> dict:
    """Replace a damaged database with a backup. The damaged file is kept
    (renamed) so nothing is destroyed. Work done after the backup is lost."""

    import shutil

    healthy, message = database_health(path)

    if healthy:
        return {"action": "none", "message": "database is healthy"}

    if not os.path.exists(backup_path):
        raise FileNotFoundError(f"No backup at {backup_path}")

    good, why = database_health(backup_path)

    if not good:
        raise ValueError(f"The backup is damaged too: {why}")

    stamp = time.strftime("%Y%m%d-%H%M%S")
    kept, n = f"{path}.corrupt-{stamp}", 0

    while os.path.exists(kept):
        n += 1
        kept = f"{path}.corrupt-{stamp}-{n}"

    if os.path.exists(path):
        os.replace(path, kept)

    for suffix in ("-wal", "-shm"):
        if os.path.exists(path + suffix):
            os.replace(path + suffix, kept + suffix)

    shutil.copy2(backup_path, path)

    return {"action": "restored", "damaged_file_kept_as": kept,
            "message": message}


# ---------- real process crashes ----------

def _crash_worker(path: str, n_orders: int, rounds: int, pause: float = 0.02) -> None:
    """Runs the whole workload and can be killed at any moment. Every step is
    safe to repeat, so a restarted worker just carries on where data ends."""

    events = make_load_events(n_orders, seed=19)
    size = -(-n_orders // rounds)
    pb = PersistentBook.restore(OrderDatabase(path))

    for k, start in enumerate(range(0, n_orders, size)):
        chunk = events[start:start + size]

        pb.submit_tolerant(chunk, quarantine=False)
        print(f"PROGRESS {k} submitted", flush=True)
        time.sleep(pause)

        for i, event in enumerate(chunk):
            if i % 10 == 5:
                pb.cancel(event["order_id"])
        print(f"PROGRESS {k} cancelled", flush=True)
        time.sleep(pause)

        pb.run_engine()
        print(f"PROGRESS {k} matched", flush=True)
        time.sleep(pause)

    print("DONE", flush=True)


def _txn_worker(path: str) -> None:
    """Opens a transaction, writes rows, never commits, and waits to be killed."""

    db = OrderDatabase(path)

    for i in range(100):
        db.conn.execute(
            "INSERT INTO orders (order_id, side, price, quantity) "
            "VALUES (?, 'B', 1.0, 1)", (f"TXN-{i}",)
        )

    print("READY", flush=True)
    time.sleep(600)


def _run_worker(path: str, n_orders: int, rounds: int, kill_after_lines=None,
                kill_after_seconds=None, pause: float = 0.02,
                timeout: float = 180) -> dict:
    """Start a worker process and optionally kill it part-way through."""

    import sys

    proc = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "--crash-worker", path,
         str(n_orders), str(rounds), str(pause)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    timers = [threading.Timer(timeout, proc.kill)]

    if kill_after_seconds is not None:
        timers.append(threading.Timer(kill_after_seconds, proc.kill))

    for timer in timers:
        timer.start()

    seen, done, killed, tail = 0, False, False, []

    try:
        for line in proc.stdout:
            line = line.strip()
            tail = (tail + [line])[-5:]

            if line == "DONE":
                done = True
            elif line.startswith("PROGRESS"):
                seen += 1
                if (kill_after_lines is not None and seen >= kill_after_lines
                        and proc.poll() is None):
                    proc.kill()
                    killed = True
                    break

        proc.wait()
    finally:
        for timer in timers:
            timer.cancel()
        proc.stdout.close()

    if not done and not killed and proc.returncode != 0:
        killed = kill_after_seconds is not None

    return {"killed": killed, "done": done, "returncode": proc.returncode,
            "progress_lines": seen, "tail": tail}


def kill_during_open_transaction(path: str) -> dict:
    """Kill a process that has written rows but not committed them."""

    import sys

    db = OrderDatabase(path)
    before = db.stats()["orders"]
    db.close()

    proc = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "--txn-worker", path],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    watchdog = threading.Timer(60, proc.kill)
    watchdog.start()
    ready = False

    try:
        for line in proc.stdout:
            if line.strip() == "READY":
                ready = True
                break

        proc.kill()
        proc.wait()
    finally:
        watchdog.cancel()
        proc.stdout.close()

    healthy, _ = database_health(path)
    db = OrderDatabase(path)

    try:
        after = db.stats()["orders"]
        leaked = db.conn.execute(
            "SELECT COUNT(*) FROM orders WHERE order_id LIKE 'TXN-%'"
        ).fetchone()[0]
        valid = validate_trade_persistence(db, expect_matched=False)["ok"]
    finally:
        db.close()

    return {"ready": ready, "rows_before": before, "rows_after": after,
            "uncommitted_rows_visible": leaked, "healthy": healthy,
            "valid": valid}


# ---------- the suite ----------

def run_resilience_suite(n_orders: int = 3000, rounds: int = 6,
                         crashes: int = 4, seed: int = 19) -> dict:
    """Run every recovery scenario and compare against a trouble-free run."""

    rng = random.Random(seed)
    checks: dict[str, bool] = {}
    fault_points: dict[str, int] = {}

    events = make_load_events(n_orders, seed=19)
    size = -(-n_orders // rounds)
    chunks = [events[i:i + size] for i in range(0, n_orders, size)]

    folder = tempfile.TemporaryDirectory()
    path = os.path.join(folder.name, "crash.db")

    try:
        # A. the trouble-free reference run
        control = PersistentBook(OrderDatabase())
        for chunk in chunks:
            _persistence_round(control, chunk)
        expected = _persistence_snapshot(control)

        # B. kill a real worker process again and again, checking after each
        interrupted, valid_each_time = 0, True
        total_lines = 3 * len(chunks)

        for c in range(crashes):
            if c % 2 == 0:
                outcome = _run_worker(path, n_orders, rounds,
                                      kill_after_lines=rng.randint(1, total_lines - 1))
            else:
                outcome = _run_worker(path, n_orders, rounds,
                                      kill_after_seconds=rng.uniform(0.25, 0.9))

            interrupted += 1 if outcome["killed"] else 0

            if os.path.exists(path):
                healthy, _ = database_health(path)
                db = OrderDatabase(path)
                try:
                    valid = validate_trade_persistence(db, expect_matched=False)["ok"]
                finally:
                    db.close()
                valid_each_time = valid_each_time and healthy and valid

        finish = _run_worker(path, n_orders, rounds)

        start = time.perf_counter()
        restarted = PersistentBook.restore(OrderDatabase(path))
        recovery_s = time.perf_counter() - start
        start = time.perf_counter()
        final_validation = validate_trade_persistence(restarted.db)
        validate_s = time.perf_counter() - start
        actual = _persistence_snapshot(restarted)
        restarted.db.close()

        checks["the worker really was killed part-way through"] = interrupted >= 1
        checks["the database is valid after every crash"] = valid_each_time
        checks["the restarted worker finishes the job"] = finish["done"]
        checks["final trades equal a run with no crash"] = actual[0] == expected[0]
        checks["final orders equal a run with no crash"] = actual[1] == expected[1]
        checks["the restored book equals the book of a run with no crash"] = (
            actual[2] == expected[2]
        )
        checks["the final data passes every persistence check"] = final_validation["ok"]

        # C. a transaction that was never committed is discarded
        txn = kill_during_open_transaction(path)
        checks["rows from an uncommitted transaction vanish after a kill"] = (
            txn["ready"] and txn["uncommitted_rows_visible"] == 0
            and txn["rows_after"] == txn["rows_before"]
            and txn["healthy"] and txn["valid"]
        )

        # D. a failure at every single statement leaves nothing half-done
        def fresh(setup=()):
            def make():
                pb = PersistentBook(OrderDatabase())
                if setup:
                    pb.submit(list(setup))
                return pb
            return make

        trade_setup = [
            {"order_id": "B1", "side": BUY, "price": 101.0, "quantity": 10},
            {"order_id": "S1", "side": SELL, "price": 100.0, "quantity": 4},
            {"order_id": "S2", "side": SELL, "price": 101.0, "quantity": 8},
        ]
        t1 = Trade("T1", "B1", "S1", 100.5, 4)
        t2 = Trade("T2", "B1", "S2", 101.0, 6)
        bad_t = Trade("TX", "NOPE", "S1", 100.0, 1)
        three = [{"order_id": f"N{i}", "side": BUY, "price": 50.0 + i, "quantity": 1}
                 for i in range(3)]

        def late_state():
            pb = PersistentBook(OrderDatabase())
            pb.run_engine(lambda b, f, n: [Trade("TL", "L1", "L2", 100.0, 5)],
                          tolerant=True)
            pb.submit([
                {"order_id": "L1", "side": BUY, "price": 101.0, "quantity": 5},
                {"order_id": "L2", "side": SELL, "price": 100.0, "quantity": 5},
            ])
            return pb

        sweeps = {
            "saving orders": (fresh(), lambda pb: pb.submit(three)),
            "saving orders with quarantine": (
                fresh(), lambda pb: pb.submit_tolerant(
                    [three[0], None, three[1], {"order_id": "Z", "side": "B"}])),
            "cancelling an order": (
                fresh(three), lambda pb: pb.cancel("N1")),
            "saving trades": (
                fresh(trade_setup), lambda pb: pb.record_trades([t1, t2])),
            "saving trades with quarantine": (
                fresh(trade_setup),
                lambda pb: pb.record_trades_tolerant([t1, bad_t, t2])),
            "running the engine": (
                fresh(trade_setup), lambda pb: pb.run_engine()),
            "retrying quarantined trades": (
                late_state, lambda pb: pb.retry_rejected()),
        }

        for label, (make, operation) in sweeps.items():
            points, ok = fault_sweep(make, operation)
            fault_points[label] = points
            checks[f"a failure at any step of {label} leaves nothing half-done"] = ok

        # E. a read-only database refuses writes and the book rolls back
        path_ro = os.path.join(folder.name, "readonly.db")
        PersistentBook(OrderDatabase(path_ro)).db.close()
        ro = PersistentBook(OrderDatabase(path_ro, read_only=True))
        try:
            ro.submit(three)
            refused = False
        except sqlite3.OperationalError:
            refused = True
        checks["a read-only database refuses writes and the book rolls back"] = (
            refused and len(ro.book) == 0
        )
        ro.db.close()

        # F. the same events delivered again change nothing
        replay = PersistentBook(OrderDatabase())
        replay.submit_tolerant(chunks[0], quarantine=False)
        once = (_db_dump(replay.db), replay.book.get_orders_in_arrival_order())
        again = [replay.submit_tolerant(chunks[0], quarantine=False)["accepted"]
                 for _ in range(3)]
        checks["delivering the same events again changes nothing"] = (
            again == [0, 0, 0]
            and (_db_dump(replay.db), replay.book.get_orders_in_arrival_order()) == once
        )
        replay.db.close()

        # G. a damaged file is replaced from a backup
        good = PersistentBook(OrderDatabase(os.path.join(folder.name, "live.db")))
        for chunk in chunks[:2]:
            _persistence_round(good, chunk)
        saved_state = _persistence_snapshot(good)
        good.db.close()
        live_path = os.path.join(folder.name, "live.db")
        backup_path = backup_database(live_path, os.path.join(folder.name, "backup.db"))

        with open(live_path, "r+b") as handle:          # overwrite the file header
            handle.write(b"\x00garbage-garbage-garbage\x00" * 8)
        for suffix in ("-wal", "-shm"):
            if os.path.exists(live_path + suffix):
                os.remove(live_path + suffix)

        damaged_seen = not database_health(live_path)[0]
        recovery = recover_database(live_path, backup_path)
        back = PersistentBook.restore(OrderDatabase(live_path))
        same = _persistence_snapshot(back) == saved_state
        valid_back = validate_trade_persistence(back.db)["ok"]
        back.db.close()
        checks["a damaged database file is detected"] = damaged_seen
        checks["recovery restores the backup and keeps the damaged file"] = (
            recovery["action"] == "restored"
            and os.path.exists(recovery["damaged_file_kept_as"])
            and same and valid_back
        )

        return {
            "orders": n_orders, "rounds": len(chunks), "crashes": crashes,
            "interrupted": interrupted, "trades": len(expected[0]),
            "recovery_s": recovery_s, "validate_s": validate_s,
            "fault_points": fault_points,
            "checks": checks, "ok": all(checks.values()),
        }
    finally:
        folder.cleanup()


def format_resilience_report(r: dict) -> str:
    lines = [
        f"RECOVERY AND RESILIENCY: {r['orders']:,} orders in {r['rounds']} rounds; "
        f"{r['interrupted']} of {r['crashes']} worker runs killed part-way; "
        f"{r['trades']:,} trades",
        f"  restart and rebuild the book  {r['recovery_s']:.3f} s",
        f"  full validation               {r['validate_s']:.3f} s",
        "  failure points tested: " + ", ".join(
            f"{label} {points}" for label, points in r["fault_points"].items()),
        "  checks:",
    ]
    lines += [f"    {'ok  ' if ok else 'FAIL'} {name}"
              for name, ok in r["checks"].items()]
    lines.append("  RESULT: " + ("ALL CHECKS PASSED" if r["ok"] else "FAILED"))
    return "\n".join(lines)


# ============================================================
# DAY 20: EXPORT LOGS FOR REPORTING
# ============================================================

# Writes the audit log, orders, trades and quarantined records to CSV and/or
# JSON Lines files, plus a summary and a manifest with a SHA-256 for every
# file. Everything is read in ONE snapshot, so the files agree with each
# other, and the export is built in a temporary folder so a failure never
# leaves half an export behind. The database is only read.

_EXPORT_TABLES = {
    "orders": (
        "SELECT seq, order_id, side, price, quantity, filled, status "
        "FROM orders ORDER BY seq",
        ("seq", "order_id", "side", "price", "quantity", "filled", "status")),
    "trades": (
        "SELECT seq, trade_id, buy_order_id, sell_order_id, price, quantity "
        "FROM trades ORDER BY seq",
        ("seq", "trade_id", "buy_order_id", "sell_order_id", "price",
         "quantity")),
    "events": (
        "SELECT e.seq, e.kind, e.ref, o.side, "
        "COALESCE(o.price, t.price), COALESCE(o.quantity, t.quantity), "
        "t.buy_order_id, t.sell_order_id FROM events e "
        "LEFT JOIN orders o ON e.kind IN ('order', 'cancel') AND o.order_id = e.ref "
        "LEFT JOIN trades t ON e.kind = 'trade' AND t.trade_id = e.ref "
        "ORDER BY e.seq",
        ("seq", "kind", "ref", "side", "price", "quantity", "buy_order_id",
         "sell_order_id")),
    "rejected": (
        "SELECT id, kind, ref, reason, payload, resolved FROM rejected "
        "ORDER BY id",
        ("id", "kind", "ref", "reason", "payload", "resolved")),
}

_EXPORT_EXT = {"csv": ".csv", "jsonl": ".jsonl"}


def _safe_cell(value):
    """Stop a spreadsheet from running text that starts like a formula."""

    if isinstance(value, str) and value[:1] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + value

    return value


def _write_export(cursor, columns, path: str, fmt: str, safe: bool) -> int:
    rows = 0

    if fmt == "csv":
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(columns)
            for row in cursor:
                writer.writerow([_safe_cell(v) if safe else v for v in row])
                rows += 1
    else:
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            for row in cursor:
                handle.write(json.dumps(dict(zip(columns, row)),
                                        ensure_ascii=False) + "\n")
                rows += 1

    return rows


def _sha256_of(path: str) -> str:
    digest = hashlib.sha256()

    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)

    return digest.hexdigest()


def export_logs(source, out_dir: str, formats=("csv",), overwrite: bool = False,
                spreadsheet_safe: bool = True, validate: bool = True) -> dict:
    """Export the database's logs for reporting.

    source is a database file path (opened read-only) or an OrderDatabase.
    Files written: orders, trades, events (the audit log with the order or
    trade details joined in) and rejected, in each requested format, plus
    summary.json and manifest.json. With spreadsheet_safe=True, text that
    starts with = + - @ gets a leading apostrophe in CSV files so Excel cannot
    treat it as a formula; JSON Lines files always hold the raw values.
    Note: the log records the ORDER of events (seq) but not clock times.
    """

    import shutil

    formats = tuple(formats)
    unknown = set(formats) - set(_EXPORT_EXT)

    if not formats or unknown:
        raise ValueError(f"formats must be from {sorted(_EXPORT_EXT)}")

    targets = [f"{name}{_EXPORT_EXT[f]}" for name in _EXPORT_TABLES for f in formats]
    targets += ["summary.json", "manifest.json"]
    os.makedirs(out_dir, exist_ok=True)

    if not overwrite:
        existing = [t for t in targets if os.path.exists(os.path.join(out_dir, t))]
        if existing:
            raise FileExistsError(
                "Export files already exist: " + ", ".join(existing)
                + " (use overwrite=True or a new folder)"
            )

    opened_here = isinstance(source, str)
    if opened_here:
        try:
            db = OrderDatabase(source, read_only=True)
        except sqlite3.OperationalError:
            db = OrderDatabase(source)           # e.g. a log left by a crash
    else:
        db = source

    tmp = tempfile.mkdtemp(prefix=".export-", dir=out_dir)

    try:
        conn = _raw(db)
        conn.execute("BEGIN")                    # one consistent snapshot
        try:
            rows = {}
            for name, (sql, columns) in _EXPORT_TABLES.items():
                for fmt in formats:
                    fname = f"{name}{_EXPORT_EXT[fmt]}"
                    rows[fname] = _write_export(
                        conn.execute(sql), columns, os.path.join(tmp, fname),
                        fmt, spreadsheet_safe)

            stats = db.stats()
            unresolved = db.count_rejected()
            validation = (validate_trade_persistence(db, expect_matched=False)
                          if validate else None)
        finally:
            conn.rollback()

        summary = {
            "exported_at_utc": datetime.datetime.now(
                datetime.timezone.utc).isoformat(timespec="seconds"),
            "source": getattr(db, "path", str(source)),
            "python": platform.python_version(),
            "formats": list(formats),
            "spreadsheet_safe_csv": bool(spreadsheet_safe),
            "stats": stats,
            "unresolved_rejected": unresolved,
            "validation": (None if validation is None else {
                "ok": validation["ok"], "checks": validation["checks"],
                "problems": validation["problems"]}),
            "note": "events are ordered by seq; no clock times are stored",
        }
        with open(os.path.join(tmp, "summary.json"), "w", encoding="utf-8") as h:
            json.dump(summary, h, indent=2)

        files = {}
        for fname in list(rows) + ["summary.json"]:
            path = os.path.join(tmp, fname)
            files[fname] = {"rows": rows.get(fname), "bytes": os.path.getsize(path),
                            "sha256": _sha256_of(path)}
        with open(os.path.join(tmp, "manifest.json"), "w", encoding="utf-8") as h:
            json.dump({"files": files}, h, indent=2)

        for fname in os.listdir(tmp):
            os.replace(os.path.join(tmp, fname), os.path.join(out_dir, fname))

    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        if opened_here:
            db.close()

    return {"out_dir": out_dir, "files": files, "stats": stats,
            "unresolved_rejected": unresolved,
            "validation_ok": None if validation is None else validation["ok"]}


def verify_export(out_dir: str, source=None) -> dict:
    """Check an export against its manifest and, optionally, the database."""

    problems: list[str] = []
    manifest_path = os.path.join(out_dir, "manifest.json")

    if not os.path.exists(manifest_path):
        return {"ok": False, "problems": ["manifest.json is missing"], "files": 0}

    with open(manifest_path, encoding="utf-8") as handle:
        manifest = json.load(handle)["files"]

    counted: dict[str, int] = {}

    for fname, info in manifest.items():
        path = os.path.join(out_dir, fname)

        if not os.path.exists(path):
            problems.append(f"{fname}: file is missing")
            continue

        if os.path.getsize(path) != info["bytes"] or _sha256_of(path) != info["sha256"]:
            problems.append(f"{fname}: contents changed since the export")
            continue

        if info["rows"] is not None:
            if fname.endswith(".csv"):
                with open(path, newline="", encoding="utf-8") as handle:
                    n = sum(1 for _ in csv.reader(handle)) - 1
            else:
                with open(path, encoding="utf-8") as handle:
                    n = sum(1 for line in handle if line.strip())

            if n != info["rows"]:
                problems.append(f"{fname}: {n} rows but the manifest says {info['rows']}")

            counted[fname.split(".")[0]] = n

    if source is not None:
        opened_here = isinstance(source, str)
        db = OrderDatabase(source, read_only=True) if opened_here else source
        try:
            for table in _EXPORT_TABLES:
                have = db.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                if table in counted and counted[table] != have:
                    problems.append(
                        f"{table}: export has {counted[table]} rows, database has {have}")
        finally:
            if opened_here:
                db.close()

    return {"ok": not problems, "problems": problems, "files": len(manifest)}


def format_export_report(result: dict, check: dict) -> str:
    lines = [f"LOG EXPORT: {result['out_dir']}",
             f"  {'file':<18} {'rows':>9} {'bytes':>10}  sha256"]

    for fname, info in result["files"].items():
        rows = "" if info["rows"] is None else f"{info['rows']:,}"
        lines.append(f"  {fname:<18} {rows:>9} {info['bytes']:>10,}  {info['sha256'][:16]}")

    healthy = {True: "healthy", False: "PROBLEMS FOUND", None: "not checked"}[
        result["validation_ok"]]
    lines += [f"  data in the database: {healthy}",
              f"  files match their manifest and the database: "
              f"{'yes' if check['ok'] else 'NO'}"]
    lines += [f"    problem: {p}" for p in check["problems"]]
    return "\n".join(lines)


# ============================================================
# DAY 21: END-TO-END VALIDATION
# ============================================================

# The whole path in one run: a producer PROCESS encodes order batches, some
# are damaged in transit, the consumer refuses them and the producer re-sends,
# one batch is delivered twice, the orders go into the book and the database,
# matching runs after every batch, the database is closed and reopened in the
# middle, and the logs are exported and verified at the end. The result is
# compared with a direct in-memory run that had none of that trouble.

def _e2e_producer(conn, n_orders: int, batch: int, corrupt_every: int,
                  duplicate_seq: int) -> None:
    """Producer process: encode batches, send them, obey ACK/NACK."""

    events = make_load_events(n_orders, seed=21)
    chunks = [events[i:i + batch] for i in range(0, n_orders, batch)]

    def send(seq: int, payload: bytes) -> None:
        conn.send_bytes(seq.to_bytes(4, "big") + payload)

    for seq, chunk in enumerate(chunks):
        clean = encode_events(chunk)
        damaged = bool(corrupt_every) and seq % corrupt_every == corrupt_every - 1

        send(seq, clean[:-3] if damaged else clean)

        if conn.recv_bytes() == b"NACK":
            send(seq, clean)                       # re-send the good copy
            conn.recv_bytes()

        if seq == duplicate_seq:
            send(seq, clean)                       # delivered a second time
            conn.recv_bytes()

    conn.send_bytes(b"END")
    conn.close()


def run_end_to_end_validation(n_orders: int = 20_000, batch: int = 1000,
                              corrupt_every: int = 4, duplicate_seq: int = 1,
                              export_dir: str | None = None) -> dict:
    """Run the whole pipeline and compare it with a direct in-memory run."""

    import multiprocessing as mp

    if n_orders < 1 or batch < 1:
        raise ValueError("n_orders and batch must be at least 1")

    events = make_load_events(n_orders, seed=21)
    chunks = [events[i:i + batch] for i in range(0, n_orders, batch)]
    expected_nacks = sum(1 for s in range(len(chunks))
                         if corrupt_every and s % corrupt_every == corrupt_every - 1)

    control = PersistentBook(OrderDatabase())            # direct run, no trouble
    for chunk in chunks:
        _persistence_round(control, chunk)
    expected = _persistence_snapshot(control)
    control.db.close()

    folder = tempfile.TemporaryDirectory()
    path = os.path.join(folder.name, "e2e.db")
    export_path = export_dir or os.path.join(folder.name, "export")
    pb = PersistentBook(OrderDatabase(path))

    ctx = mp.get_context("spawn")
    parent, child = ctx.Pipe(duplex=True)
    proc = ctx.Process(target=_e2e_producer,
                       args=(child, n_orders, batch, corrupt_every, duplicate_seq))

    done: set[int] = set()
    quarantined: dict[int, int] = {}
    nacks = duplicates = restarts = 0
    failure = None
    restart_at = max(1, len(chunks) // 2)
    result: dict = {}

    try:
        proc.start()
        child.close()
        start = time.perf_counter()

        while True:
            try:
                if not parent.poll(120):
                    raise TimeoutError("the producer stopped sending")
                message = parent.recv_bytes()
            except (EOFError, TimeoutError, OSError) as exc:
                failure = f"{type(exc).__name__}: {exc}"
                break

            if message == b"END":
                break

            seq, payload = int.from_bytes(message[:4], "big"), message[4:]

            if seq in done:
                duplicates += 1                          # already stored: just ACK
                parent.send_bytes(b"ACK")
                continue

            try:
                batch_events = decode_events(payload)
            except ValueError as exc:
                nacks += 1
                pb.db.save_rejected([("order", f"batch {seq}",
                                      f"damaged in transit: {exc}",
                                      payload[:64].hex())])
                quarantined[seq] = pb.db.conn.execute(
                    "SELECT MAX(id) FROM rejected").fetchone()[0]
                parent.send_bytes(b"NACK")
                continue

            pb.submit(batch_events)
            for i, event in enumerate(batch_events):
                if i % 10 == 5:
                    pb.cancel(event["order_id"])
            pb.run_engine()
            done.add(seq)

            if seq in quarantined:
                pb.db.resolve_rejected([quarantined.pop(seq)])

            parent.send_bytes(b"ACK")

            if len(done) == restart_at:                  # close and reopen mid-run
                pb.db.close()
                pb = PersistentBook.restore(OrderDatabase(path))
                restarts += 1

        elapsed = max(time.perf_counter() - start, 1e-9)
        proc.join(30)
        if proc.is_alive():
            proc.terminate()
            proc.join(5)

        actual = _persistence_snapshot(pb)
        validation = validate_trade_persistence(pb.db)
        reconciled = pb.reconcile()["consistent_before"]
        live_book = pb.book.get_orders_in_arrival_order()
        rejected_total = pb.db.count_rejected(unresolved_only=False)
        unresolved = pb.db.count_rejected()

        pb.db.close()
        again = PersistentBook.restore(OrderDatabase(path))
        restarts += 1
        restored_equal = again.book.get_orders_in_arrival_order() == live_book

        exported = export_logs(again.db, export_path, formats=("csv", "jsonl"))
        verified = verify_export(export_path, again.db)
        with open(os.path.join(export_path, "summary.json"), encoding="utf-8") as h:
            summary = json.load(h)
        again.db.close()

        checks = {
            "every batch arrived exactly once": done == set(range(len(chunks))),
            "damaged batches were refused and re-sent": nacks == expected_nacks,
            "a duplicate delivery was recognised and ignored":
                duplicates == (1 if 0 <= duplicate_seq < len(chunks) else 0),
            "damaged batches were quarantined, then resolved after the re-send":
                rejected_total == nacks and unresolved == 0,
            "the producer process exited cleanly":
                failure is None and proc.exitcode == 0,
            "final trades equal the direct in-memory run": actual[0] == expected[0],
            "final orders and fills equal the direct run": actual[1] == expected[1],
            "the final book equals the direct run": actual[2] == expected[2],
            "stored data passes every persistence check": validation["ok"],
            "the book and the database agree": reconciled,
            "the book restored after a restart equals the live book": restored_equal,
            "exported files match their manifest and the database": verified["ok"],
            "the export summary reports healthy data":
                bool(summary["validation"] and summary["validation"]["ok"]),
            "the exported rejected log shows the damaged batches":
                exported["files"]["rejected.csv"]["rows"] == nacks,
        }

        result = {
            "orders": n_orders, "batches": len(chunks), "nacks": nacks,
            "duplicates": duplicates, "restarts": restarts,
            "trades": len(actual[0]), "elapsed_s": elapsed,
            "rate": n_orders / elapsed, "failure": failure,
            "export": {"dir": export_path, "files": exported["files"],
                       "verified": verified["ok"]},
            "checks": checks, "ok": all(checks.values()),
        }
    finally:
        if proc.is_alive():
            proc.terminate()
        try:
            pb.db.close()
        except sqlite3.Error:
            pass
        parent.close()
        folder.cleanup()          # a user-chosen export folder is outside it

    return result


def format_end_to_end_report(r: dict) -> str:
    lines = [
        f"END-TO-END: {r['orders']:,} orders in {r['batches']} batches through a "
        f"separate producer process; {r['nacks']} damaged batches re-sent, "
        f"{r['duplicates']} duplicate delivery ignored, {r['restarts']} restarts; "
        f"{r['trades']:,} trades",
        f"  whole run {r['elapsed_s']:.2f} s ({r['rate']:,.0f} orders/s)",
        f"  exported {len(r['export']['files'])} files to {r['export']['dir']}",
        "  checks:",
    ]
    lines += [f"    {'ok  ' if ok else 'FAIL'} {name}"
              for name, ok in r["checks"].items()]
    if r["failure"]:
        lines.append(f"    producer problem: {r['failure']}")
    lines.append("  RESULT: " + ("ALL CHECKS PASSED" if r["ok"] else "FAILED"))
    return "\n".join(lines)


# ============================================================
# BENCHMARK REPORT (DAYS 11-21)
# ============================================================

def _report_load_table(runs: list[dict]) -> str:
    head = "| Step | " + " | ".join(f"{r['orders']:,} orders" for r in runs) + " |"
    rows = [head, "|---|" + "---:|" * len(runs)]

    for label, key in (("Encode to bytes", "encode"),
                       ("Decode from bytes", "decode"),
                       ("Ingest from dicts", "ingest"),
                       ("Ingest from bytes (decode + insert)", "ingest_encoded"),
                       ("Look up every order by ID", "lookup"),
                       ("Cancel orders", "cancel")):
        cells = " | ".join(
            f"{r[key + '_rate']:,.0f} /s ({r[key + '_s'] * 1000:,.0f} ms)"
            for r in runs
        )
        rows.append(f"| {label} | {cells} |")

    return "\n".join(rows)


def _report_findings(runs: list[dict], zc: dict) -> list[str]:
    big = runs[-1]
    stages = {
        "encode": big["encode_rate"], "decode": big["decode_rate"],
        "ingest": big["ingest_rate"], "lookup": big["lookup_rate"],
        "cancel": big["cancel_rate"],
    }
    slowest = min(stages, key=stages.get)
    out = [
        f"At {big['orders']:,} orders the slowest stage is **{slowest}** "
        f"at {stages[slowest]:,.0f} orders/s.",
        f"Ingesting from bytes takes "
        f"{big['ingest_encoded_s'] / big['ingest_s']:.2f}x as long as ingesting "
        f"from dictionaries, because the bytes must be decoded first. Decoding "
        f"alone is about "
        f"{big['decode_s'] / big['ingest_encoded_s'] * 100:.0f}% of that time.",
    ]

    if len(runs) > 1:
        a = runs[0]
        scale = (big["ingest_s"] / a["ingest_s"]) / (big["orders"] / a["orders"])
        out.append(
            f"Going from {a['orders']:,} to {big['orders']:,} orders, ingest "
            f"time grew {scale:.2f}x relative to the number of orders "
            f"(1.00 means perfectly linear)."
        )

    out.append(
        f"The wire format is compact: "
        f"{big['payload_bytes'] / big['orders']:.1f} bytes per order."
    )
    out.append(
        f"Decoded orders take about {zc['memory_growth_factor']:.0f}x more "
        f"memory as Python objects than the bytes they came from. This is the "
        f"copy cost between shared memory and the order book."
    )
    return out


_DAY_LOG = [
    (1, "Basic order-book structure with separate buy and sell storage"),
    (2, "Insert buy and sell orders with validation"),
    (3, "Retrieve orders and look up by ID"),
    (4, "Cancel orders; best bid and best ask"),
    (5, "Firehose event ingestion"),
    (6, "Async firehose ingestion"),
    (7, "Insertion testing: duplicates, bad values, 50,000 inserts"),
    (8, "Trade validation"),
    (9, "Heap-based best bid/ask and stress tests"),
    (10, "Arrival-order retrieval and full integration tests"),
    (11, "IPC analysis: Queue vs Pipe vs shared memory; binary encode/decode"),
    (12, "Batch IPC transmission and ingestion"),
    (13, "Load test with 50,000+ orders"),
    (14, "Zero-copy architecture verification"),
    (15, "Benchmark report, acceptance checklist and final review"),
    (16, "Database layer: SQLite storage for orders, trades and fills, "
         "connected to the matching engine"),
    (17, "Trade persistence validation: rule replay, restart invariance, "
         "tamper detection"),
    (18, "Missing and failed records: quarantine, rollback, retry, reconcile"),
    (19, "Recovery and resiliency testing: killed processes, injected "
         "failures, backups"),
    (20, "Export logs for reporting: CSV/JSON files with a verified manifest"),
    (21, "End-to-end validation: producer process, damaged batches, restart, "
         "export, compared with a direct run"),
]


def count_tests_by_file() -> tuple[dict, list]:
    """Count tests per test file without running them.

    Returns ({file name: test count}, [files that could not be imported]).
    """

    import unittest

    counts: dict[str, int] = {}
    broken: list[str] = []

    def walk(suite) -> None:
        for item in suite:
            if isinstance(item, unittest.TestSuite):
                walk(item)
                continue

            test_id = item.id()

            if test_id.startswith("unittest.loader._FailedTest."):
                broken.append(test_id.rsplit(".", 1)[-1])
            else:
                name = test_id.split(".")[0]
                counts[name] = counts.get(name, 0) + 1

    walk(unittest.defaultTestLoader.discover("."))
    return counts, broken


def _test_day(name: str) -> str:
    """Which project day a test file belongs to."""

    import re

    lowered = name.lower()

    if "orderdb" in lowered:
        return "16"
    if "ipc_efficiency" in lowered:
        return "12"
    if lowered == "test_orderbook":
        return "11-12"
    if "ipc" in lowered:
        return "11"
    found = re.search(r"day(\d+)", lowered)
    if found:
        return found.group(1)
    if "zero" in lowered:
        return "14"
    return "other"


def run_test_suite() -> dict:
    """Run python -m unittest discover in the current folder."""

    import re
    import sys

    try:
        by_file, broken = count_tests_by_file()
    except Exception:                                    # noqa: BLE001
        by_file, broken = {}, []

    proc = subprocess.run(
        [sys.executable, "-m", "unittest", "discover"],
        capture_output=True, text=True
    )
    tail = proc.stderr.strip().splitlines()[-6:]
    text = "\n".join(tail)
    ran = re.search(r"Ran (\d+) tests? in ([\d.]+)s", text)

    return {
        "ran": int(ran.group(1)) if ran else 0,
        "seconds": float(ran.group(2)) if ran else 0.0,
        "ok": proc.returncode == 0 and ran is not None,
        "tail": text,
        "by_file": by_file,
        "broken": broken,
    }


def _tests_summary_line(tests) -> str:
    if tests is None:
        return "- Tests: not run (leave out --skip-tests to include them)."

    return (f"- Tests: **{tests['ran']} total, "
            f"{'all passing' if tests['ok'] else 'SOME FAILING'}**.")


def _total_tests_section(tests) -> list[str]:
    """Total test count, per file, with what Days 16 to 21 added."""

    lines = ["", "### Total tests (including Days 16 to 21)", ""]

    if tests is None:
        return lines + ["Not run. Run without --skip-tests to include them.", ""]

    by_file = tests["by_file"]
    added = {d: sum(n for name, n in by_file.items() if _test_day(name) == d)
             for d in ("16", "17", "18", "19", "20", "21")}

    def day_key(item):
        day = _test_day(item[0])
        digits = "".join(c for c in day.split("-")[0] if c.isdigit())
        return (int(digits) if digits else 999, item[0])

    lines += [f"**{tests['ran']} tests in total, "
              f"{'all passing' if tests['ok'] else 'SOME FAILING'}** "
              f"(ran in {tests['seconds']:.1f} s). "
              + ", ".join(f"Day {d} added {n}" for d, n in added.items())
              + f"; before Day 16 the total was "
              f"{tests['ran'] - sum(added.values())}.",
              "",
              "| Test file | Day | Tests |", "|---|---|---:|"]

    for name, count in sorted(by_file.items(), key=day_key):
        lines.append(f"| {name} | {_test_day(name)} | {count} |")

    lines.append(f"| **Total** | | **{sum(by_file.values())}** |")

    if tests["broken"]:
        lines += ["", "These test files could not be imported, so they are "
                  "not counted: " + ", ".join(sorted(set(tests["broken"]))) + "."]

    lines.append("")
    return lines


def _day15_section(runs: list[dict], zc: dict, ipc_text, tests,
                   db_run: dict, pv: dict, fh: dict, rs: dict,
                   e2e: dict) -> list[str]:
    big = runs[-1]
    by_name = {c["check"]: c["passed"] for c in zc["checks"]}

    def mark(passed) -> str:
        if passed is None:
            return "NOT RUN"
        return "PASS" if passed else "FAIL"

    checklist = [
        (f"{big['orders']:,}+ orders pass every load-test check", big["ok"]),
        ("Cancels keep the book, arrival order and best prices correct",
         big["checks"]["count_after_cancel"]
         and big["checks"]["arrival_order_after_cancel"]
         and big["checks"]["best_prices_correct_after_cancel"]),
        ("Binary ingest builds the same book as dictionary ingest",
         big["checks"]["encoded_path_matches_direct_path"]),
        ("Shared-memory transport verified zero-copy", zc["transport_zero_copy"]),
        ("Copy cost at the order book measured and documented",
         by_name.get("order book copies orders out of shared memory", False)),
        ("IPC methods compared (Day 11 results included)",
         None if not ipc_text else "did not produce" not in ipc_text),
        ("Database layer stores, matches and restores correctly (Day 16)",
         db_run["ok"]),
        ("Stored trades validated; restarts change nothing (Day 17)",
         pv["ok"]),
        ("Missing and failed records are quarantined, rolled back or "
         "repaired (Day 18)", fh["ok"]),
        ("Data survives killed processes, injected failures and a damaged "
         "file (Day 19)", rs["ok"]),
        ("Logs export with a manifest that matches the database (Day 20)",
         e2e["checks"]["exported files match their manifest and the database"]),
        ("End-to-end run equals a direct run despite damage, duplicates and "
         "a restart (Day 21)", e2e["ok"]),
    ]
    if tests is not None:
        checklist.append(
            (f"Full test suite passes ({tests['ran']} tests)", tests["ok"])
        )

    lines = ["", "## Days 15 to 21: Final summary", "",
             "### Work completed, Days 1 to 21", "",
             "| Day | Work |", "|---|---|"]
    lines += [f"| {d} | {text} |" for d, text in _DAY_LOG]

    lines += ["", "### Acceptance checklist", "",
              "| Requirement | Result |", "|---|---|"]
    lines += [f"| {name} | {mark(passed)} |" for name, passed in checklist]
    if tests is None:
        lines.append("| Full test suite passes | NOT RUN (use --tests) |")

    if tests is not None:
        lines += ["", "### Test suite", "",
                  f"{tests['ran']} tests ran in {tests['seconds']:.1f} s: "
                  f"**{'OK' if tests['ok'] else 'FAILED'}**.", ""]
        if not tests["ok"]:
            lines += ["```", tests["tail"], "```"]

    lines += [
        "",
        "### Recommendations",
        "",
        "- Speed up ingest first: it is the slowest stage, so fewer checks and "
        "fewer temporary objects per order give the biggest gain.",
        "- Add an ingest path that takes decoded tuples directly and skips the "
        "dictionary step; keep the checked path and test that both build "
        "identical books.",
        "- Connect the real matching engine through PersistentBook.run_engine "
        "and keep database writes batched (one transaction per batch); "
        "per-order commits are much slower.",
        "- Use small batches (tens to a few hundred messages) for a trading "
        "engine; large batches raise throughput but add waiting time.",
        "- Rerun this report on a multi-core machine before quoting shared "
        "memory against Pipe, because the ranking can change.",
        "- For ARM computers, add memory barriers around the ring-buffer "
        "counters.",
        "",
    ]
    return lines


def build_benchmark_report(
    sizes: tuple = (50_000, 100_000),
    include_ipc: bool = True,
    include_tests: bool = True
) -> tuple[str, bool]:
    """Run the load test and zero-copy check and return (markdown, all_ok).

    If ipc_benchmark.py is in the current folder and include_ipc is True,
    its Day 11 results are added.
    """

    import sys

    runs = [run_load_test(n) for n in sizes]
    zc = verify_zero_copy()
    db_run = run_database_demo(sizes[0])
    pv = run_persistence_validation(sizes[0])
    fh = run_failure_handling_demo(20_000)
    rs = run_resilience_suite(3000, rounds=6, crashes=4)
    e2e = run_end_to_end_validation(10_000)

    ipc_text = None
    if include_ipc and os.path.exists("ipc_benchmark.py"):
        proc = subprocess.run(
            [sys.executable, "ipc_benchmark.py", "--quick", "--save"],
            capture_output=True, text=True
        )
        if os.path.exists("ipc_results.txt"):
            with open("ipc_results.txt", encoding="utf-8") as handle:
                ipc_text = handle.read()
        else:
            ipc_text = ("ipc_benchmark.py did not produce results:\n"
                        + proc.stderr[-800:])

    tests = run_test_suite() if include_tests else None

    all_ok = (all(r["ok"] for r in runs) and zc["ok"] and db_run["ok"] and pv["ok"] and fh["ok"] and rs["ok"] and e2e["ok"]
              and (tests is None or tests["ok"]))

    lines = [
        "# ChronosMatch Benchmark Report (Day 21)",
        "",
        f"Teammate 3 - Days 1 to 21 - {time.strftime('%Y-%m-%d %H:%M')}",
        "",
        f"Machine: {platform.system()} {platform.release()}, "
        f"{os.cpu_count()} logical CPUs, Python {platform.python_version()} "
        f"({platform.python_implementation()}). Numbers depend on this "
        "computer and vary a little from run to run.",
        "",
        "## Summary",
        "",
        f"- Correctness: **{'all checks passed' if all_ok else 'SOME CHECKS FAILED'}** "
        f"({sum(len(r['checks']) for r in runs)} load-test checks and "
        f"{len(zc['checks'])} zero-copy checks).",
        _tests_summary_line(tests),
        f"- Transport between processes (shared memory) is zero-copy: "
        f"**{zc['transport_zero_copy']}**.",
        f"- The order book itself is zero-copy: **{zc['book_is_zero_copy']}** "
        "(it stores Python objects, so it copies by design).",
        "",
        "## Day 13: Load test",
        "",
        _report_load_table(runs),
        "",
        "Each run encodes the orders, decodes them, builds one book from "
        "dictionaries and one from bytes, compares the two, looks up every "
        "order, cancels 20% of them, and re-checks the book. Checks:",
        "",
    ]

    for name in runs[-1]["checks"]:
        passed = all(r["checks"][name] for r in runs)
        lines.append(f"- {'PASS' if passed else 'FAIL'}: {name.replace('_', ' ')}")

    lines += ["", "## Day 14: Zero-copy verification", "",
              "| Check | Result | Evidence |", "|---|---|---|"]
    for c in zc["checks"]:
        lines.append(
            f"| {c['check']} | {'PASS' if c['passed'] else 'FAIL'} | {c['detail']} |"
        )

    lines += [
        "", "## Day 16: Database layer", "",
        f"{db_run['orders']:,} orders went into the order book and a SQLite "
        f"database, were matched ({db_run['trades']:,} trades, "
        f"{db_run['traded_quantity']:,} units), and the book was rebuilt from "
        "the database as after a restart.",
        "",
        "| Step | Time | Orders/s |", "|---|---:|---:|",
        f"| Submit (book + database) | {db_run['submit_s']:.3f} s | "
        f"{db_run['submit_rate']:,.0f} |",
        f"| Match + store trades | {db_run['match_s']:.3f} s | "
        f"{db_run['match_rate']:,.0f} |",
        f"| Restore after restart | {db_run['restore_s']:.3f} s | "
        f"{db_run['restore_rate']:,.0f} |",
        "",
    ]
    lines += [f"- {'PASS' if ok else 'FAIL'}: {name}"
              for name, ok in db_run["checks"].items()]
    lines += [
        "", "## Day 17: Trade persistence validation", "",
        f"{pv['orders']:,} orders were processed in {pv['rounds']} rounds "
        f"(submit, cancel some, match). The same work ran twice: straight "
        f"through, and with the database closed and reopened between rounds "
        f"({pv['restarts']} restarts). {pv['trades']:,} trades were stored "
        f"and the results compared.",
        "",
        f"Straight through: {pv['control_s']:.3f} s. With restarts: "
        f"{pv['restart_s']:.3f} s.",
        "",
    ]
    lines += [f"- {'PASS' if ok else 'FAIL'}: {name}"
              for name, ok in pv["checks"].items()]
    lines += [f"- Problem found: {p}" for p in pv["problems"]]

    lines += [
        "", "## Day 18: Missing and failed records", "",
        f"{fh['orders']:,} order events were sent with {fh['bad_events']:,} "
        "deliberately damaged, plus bad trades, a failing database write, a "
        "locked database and a book that disagreed with the database. "
        f"{fh['quarantined_ever']:,} records were quarantined in total and "
        f"{fh['quarantined']:,} are still waiting after retries.",
        "",
    ]
    lines += [f"- {'PASS' if ok else 'FAIL'}: {name}"
              for name, ok in fh["checks"].items()]

    lines += [
        "", "## Day 19: Recovery and resiliency testing", "",
        f"{rs['orders']:,} orders were processed in {rs['rounds']} rounds while "
        f"a real worker process was killed part-way through "
        f"({rs['interrupted']} of {rs['crashes']} runs interrupted). After "
        "each crash the database was checked, and after the last one the work "
        f"was finished and compared with a run that never crashed "
        f"({rs['trades']:,} trades).",
        "",
        f"Restart and rebuild of the book: {rs['recovery_s']:.3f} s. Full "
        f"validation: {rs['validate_s']:.3f} s.",
        "",
        "Failure points tested (one injected disk error per database "
        "statement): " + ", ".join(
            f"{label} ({points})" for label, points in rs["fault_points"].items())
        + ".",
        "",
    ]
    lines += [f"- {'PASS' if ok else 'FAIL'}: {name}"
              for name, ok in rs["checks"].items()]
    lines += ["", "Limit: restoring from a backup loses any work done after the "
              "backup was taken.", ""]

    lines += [
        "", "## Day 20: Log export for reporting", "",
        f"The logs of the end-to-end run were exported to "
        f"{len(e2e['export']['files'])} files and checked against their "
        f"manifest and the database: "
        f"**{'match' if e2e['export']['verified'] else 'MISMATCH'}**.",
        "",
        "| File | Rows | Bytes | SHA-256 (first 16) |", "|---|---:|---:|---|",
    ]
    for fname, info in e2e["export"]["files"].items():
        rows = "" if info["rows"] is None else f"{info['rows']:,}"
        lines.append(f"| {fname} | {rows} | {info['bytes']:,} | "
                     f"{info['sha256'][:16]} |")
    lines += ["", "Events are ordered by sequence number; the database stores "
              "no clock times.", ""]

    lines += [
        "", "## Day 21: End-to-end validation", "",
        f"{e2e['orders']:,} orders in {e2e['batches']} batches went from a "
        f"separate producer process, through the binary format, into the book "
        f"and database, with matching after every batch. {e2e['nacks']} "
        f"batches were damaged in transit and re-sent, {e2e['duplicates']} "
        f"batch was delivered twice, and the database was closed and reopened "
        f"({e2e['restarts']} restarts). {e2e['trades']:,} trades resulted, and "
        f"the whole run took {e2e['elapsed_s']:.2f} s "
        f"({e2e['rate']:,.0f} orders/s). Everything was compared with a direct "
        "in-memory run.",
        "",
    ]
    lines += [f"- {'PASS' if ok else 'FAIL'}: {name}"
              for name, ok in e2e["checks"].items()]

    lines += _total_tests_section(tests)

    lines += ["", "## Day 11: IPC methods compared", ""]
    if ipc_text:
        lines += ["```", ipc_text.rstrip(), "```"]
    else:
        lines.append("ipc_benchmark.py was not found or was skipped, so this "
                     "section is empty.")

    lines += ["", "## Findings", ""]
    lines += [f"- {text}" for text in _report_findings(runs, zc)]
    lines += _day15_section(runs, zc, ipc_text, tests, db_run, pv, fh, rs, e2e)
    lines += [
        "",
        "## What this means",
        "",
        "- Moving data between processes through shared memory copies nothing: "
        "a value written by one process appears in the other with no pickling "
        "and no pipe.",
        "- The order book is where copying happens: it turns bytes into Python "
        "objects. A truly zero-copy book would keep fixed-size records in "
        "shared memory instead of Python objects, which is a redesign.",
        "- The next speed gain is in the ingest path (fewer checks and fewer "
        "temporary objects per order), not in the transport.",
        "",
        "## Limits of these numbers",
        "",
        "- One run per size on one machine; repeat and use the median.",
        "- Other programs running at the same time slow the numbers down.",
        "- The load test uses random valid orders; real market data differs.",
        "- On x86/x64 the shared-memory counters are safe without memory "
        "barriers; ARM would need them.",
        "",
    ]

    return "\n".join(lines), all_ok


# ============================================================
# BASIC DAY 1-14 DEMONSTRATION
# ============================================================

if __name__ == "__main__":

    import sys

    if "--load" in sys.argv:
        n = 50_000
        if "--orders" in sys.argv:
            n = int(sys.argv[sys.argv.index("--orders") + 1].replace(",", ""))
        report = run_load_test(n)
        print(format_load_report(report))
        sys.exit(0 if report["ok"] else 1)

    if "--report" in sys.argv:
        sizes = (50_000,) if "--quick" in sys.argv else (50_000, 100_000)
        print("running benchmarks, please wait ...", flush=True)
        text, ok = build_benchmark_report(
            sizes,
            include_ipc="--skip-ipc" not in sys.argv,
            include_tests="--skip-tests" not in sys.argv
        )
        with open("benchmark_report.md", "w", encoding="utf-8") as handle:
            handle.write(text)
        print("saved benchmark_report.md")
        sys.exit(0 if ok else 1)

    if "--count-tests" in sys.argv:
        counts, broken = count_tests_by_file()

        def sort_key(item):
            digits = "".join(c for c in _test_day(item[0]).split("-")[0] if c.isdigit())
            return (int(digits) if digits else 999, item[0])

        print(f"{'test file':<34} {'day':<6} tests")
        print("-" * 48)
        for name, count in sorted(counts.items(), key=sort_key):
            print(f"{name:<34} {_test_day(name):<6} {count:>5}")
        print("-" * 48)
        print(f"{'TOTAL':<34} {'':<6} {sum(counts.values()):>5}")
        for name in sorted(set(broken)):
            print(f"CANNOT IMPORT (not counted): {name}")
        sys.exit(1 if broken else 0)

    if "--end-to-end" in sys.argv:
        n = 20_000
        if "--orders" in sys.argv:
            n = int(sys.argv[sys.argv.index("--orders") + 1].replace(",", ""))
        export_to = None
        if "--export-dir" in sys.argv:
            export_to = sys.argv[sys.argv.index("--export-dir") + 1]
        report = run_end_to_end_validation(n, export_dir=export_to)
        print(format_end_to_end_report(report))
        sys.exit(0 if report["ok"] else 1)

    if "--export-logs" in sys.argv:
        i = sys.argv.index("--export-logs")
        if len(sys.argv) < i + 3:
            print("Usage: python orderbook.py --export-logs DATABASE FOLDER "
                  "[--format csv|jsonl|both] [--overwrite] [--raw]")
            sys.exit(2)
        db_file, folder_out = sys.argv[i + 1], sys.argv[i + 2]
        choice = (sys.argv[sys.argv.index("--format") + 1]
                  if "--format" in sys.argv else "csv")
        formats = ("csv", "jsonl") if choice == "both" else (choice,)
        try:
            result = export_logs(db_file, folder_out, formats,
                                 overwrite="--overwrite" in sys.argv,
                                 spreadsheet_safe="--raw" not in sys.argv)
        except (FileNotFoundError, FileExistsError, ValueError, sqlite3.Error) as exc:
            print(f"Cannot export: {exc}")
            sys.exit(2)
        check = verify_export(folder_out, db_file)
        print(format_export_report(result, check))
        sys.exit(0 if check["ok"] and result["validation_ok"] else 1)

    if "--crash-worker" in sys.argv:
        i = sys.argv.index("--crash-worker")
        _crash_worker(sys.argv[i + 1], int(sys.argv[i + 2]), int(sys.argv[i + 3]),
                      float(sys.argv[i + 4]) if len(sys.argv) > i + 4 else 0.02)
        sys.exit(0)

    if "--txn-worker" in sys.argv:
        _txn_worker(sys.argv[sys.argv.index("--txn-worker") + 1])
        sys.exit(0)

    if "--resilience" in sys.argv:
        n = 3000
        if "--orders" in sys.argv:
            n = int(sys.argv[sys.argv.index("--orders") + 1].replace(",", ""))
        crashes = 4
        if "--crashes" in sys.argv:
            crashes = int(sys.argv[sys.argv.index("--crashes") + 1])
        report = run_resilience_suite(n, crashes=crashes)
        print(format_resilience_report(report))
        sys.exit(0 if report["ok"] else 1)

    if "--failures" in sys.argv:
        n = 20_000
        if "--orders" in sys.argv:
            n = int(sys.argv[sys.argv.index("--orders") + 1].replace(",", ""))
        report = run_failure_handling_demo(n)
        print(format_failure_report(report))
        sys.exit(0 if report["ok"] else 1)

    if "--validate-db" in sys.argv:
        file_path = sys.argv[sys.argv.index("--validate-db") + 1]
        try:
            report = validate_database_file(
                file_path, expect_matched="--unmatched" not in sys.argv
            )
        except (FileNotFoundError, sqlite3.Error) as exc:
            print(f"Cannot check {file_path}: {exc}")
            sys.exit(2)
        print(format_validation_report(report))
        sys.exit(0 if report["ok"] else 1)

    if "--persistence" in sys.argv:
        n = 50_000
        if "--orders" in sys.argv:
            n = int(sys.argv[sys.argv.index("--orders") + 1].replace(",", ""))
        rounds = 10
        if "--rounds" in sys.argv:
            rounds = int(sys.argv[sys.argv.index("--rounds") + 1])
        db_path = None
        if "--db" in sys.argv:
            db_path = sys.argv[sys.argv.index("--db") + 1]
        report = run_persistence_validation(n, db_path, rounds)
        print(format_persistence_report(report))
        sys.exit(0 if report["ok"] else 1)

    if "--database" in sys.argv:
        n = 50_000
        if "--orders" in sys.argv:
            n = int(sys.argv[sys.argv.index("--orders") + 1].replace(",", ""))
        db_path = None
        if "--db" in sys.argv:
            db_path = sys.argv[sys.argv.index("--db") + 1]
        report = run_database_demo(n, db_path)
        print(format_demo_report(report))
        sys.exit(0 if report["ok"] else 1)

    if "--zero-copy" in sys.argv:
        report = verify_zero_copy()
        print(format_zero_copy_report(report))
        sys.exit(0 if report["ok"] else 1)

    book = OrderBook()

    book.add_order(Order("B001", BUY, 100.50, 10))
    book.add_order(Order("B002", BUY, 101.00, 20))
    book.add_order(Order("S001", SELL, 102.50, 15))
    book.add_order(Order("S002", SELL, 103.00, 25))

    print("BUY ORDERS:")
    for order in book.get_orders(BUY):
        print(order)

    print("\nSELL ORDERS:")
    for order in book.get_orders(SELL):
        print(order)

    print("\nLOOKUP B001:")
    print(book.get_order("B001"))

    print("\nBEST BID:", book.best_bid())
    print("BEST ASK:", book.best_ask())

    print("\nARRIVAL ORDER:")
    for order in book.get_orders_in_arrival_order():
        print(order)

    ipc_events = [
        {
            "order_id": "B003",
            "side": BUY,
            "price": 99.50,
            "quantity": 5
        }
    ]

    binary_payload = encode_events(ipc_events)
    decoded_events = decode_events(binary_payload)

    print("\nDECODED IPC EVENTS:")
    print(decoded_events)

    ipc_book = OrderBook()
    ingested = ingest_encoded(ipc_book, binary_payload)

    print("\nIPC EVENTS INGESTED:", ingested)
    print("IPC BOOK TOTAL ORDERS:", len(ipc_book))

    print("\nTOTAL ORIGINAL BOOK ORDERS:", len(book))