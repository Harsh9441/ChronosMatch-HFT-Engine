"""
ChronosMatch - Order Book (Teammate 3)
Combined Day 1-17 implementation.

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

This is a storage/order-book module, not a full matching engine.
"""

import heapq
import math
import os
import platform
import random
import sqlite3
import struct
import subprocess
import tempfile
import time
import tracemalloc

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
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_trades_buy    ON trades(buy_order_id);
CREATE INDEX IF NOT EXISTS idx_trades_sell   ON trades(sell_order_id);
CREATE INDEX IF NOT EXISTS idx_events_ref    ON events(kind, ref);
"""


def _chunks(items: list, size: int = 500):
    for start in range(0, len(items), size):
        yield items[start:start + size]


class OrderDatabase:
    """Tables and operations. Every write is one all-or-nothing transaction."""

    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        self.conn = sqlite3.connect(path)
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

    def save_orders(self, orders: Iterable[Order]) -> int:
        """Store new orders. If any is refused, none are stored."""

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

        if not rows:
            return 0

        with self.conn:
            try:
                self.conn.executemany(
                    "INSERT INTO orders (order_id, side, price, quantity) "
                    "VALUES (?, ?, ?, ?)", rows
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError(f"Database refused the orders: {exc}") from exc

            self.conn.executemany(
                "INSERT INTO events (kind, ref) VALUES ('order', ?)",
                [(row[0],) for row in rows]
            )

        return len(rows)

    def mark_cancelled(self, order_id: str) -> bool:
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

    def save_trades(self, trades: Iterable) -> int:
        """Store trades and update each order's fill and status.

        The whole batch is saved or none of it is.
        """

        trades = list(trades)

        if not trades:
            return 0

        with self.conn:
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

    @classmethod
    def restore(cls, db: OrderDatabase) -> "PersistentBook":
        """Rebuild the in-memory book from the database after a restart."""

        book = OrderBook()

        for order in db.load_open_orders():
            book.add_order(order)

        return cls(db, book)

    def submit(self, events: Iterable[Mapping]) -> int:
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

        try:
            return self.book.ingest_firehose(events)
        finally:
            added = self.book.get_orders_in_arrival_order()[before:]
            if added:
                self.db.save_orders(added)

    def cancel(self, order_id: str) -> bool:
        if not self.book.cancel_order(order_id):
            return False

        self.db.mark_cancelled(order_id)
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

        for order_id in self.db.filled_ids(touched):
            self.book.cancel_order(order_id)

        return saved

    def run_engine(self, engine=reference_match) -> int:
        """Run a matching engine on the book and store what it produces.

        engine(book, filled, first_trade_number) must return Trade objects.
        """

        trades = list(engine(
            self.book, self.db.filled_map(), self.db.next_trade_number()
        ))
        return self.record_trades(trades)


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


# ============================================================
# BENCHMARK REPORT (DAYS 11-17)
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
    """Total test count, per file, with what Day 16 added."""

    lines = ["", "### Total tests (including Days 16 and 17)", ""]

    if tests is None:
        return lines + ["Not run. Run without --skip-tests to include them.", ""]

    by_file = tests["by_file"]
    day16 = sum(n for name, n in by_file.items() if _test_day(name) == "16")
    day17 = sum(n for name, n in by_file.items() if _test_day(name) == "17")

    def day_key(item):
        day = _test_day(item[0])
        digits = "".join(c for c in day.split("-")[0] if c.isdigit())
        return (int(digits) if digits else 999, item[0])

    lines += [f"**{tests['ran']} tests in total, "
              f"{'all passing' if tests['ok'] else 'SOME FAILING'}** "
              f"(ran in {tests['seconds']:.1f} s). "
              f"Day 16 added {day16} tests and Day 17 added {day17}; before "
              f"Day 16 the total was {tests['ran'] - day16 - day17}.",
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
                   db_run: dict, pv: dict) -> list[str]:
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
    ]
    if tests is not None:
        checklist.append(
            (f"Full test suite passes ({tests['ran']} tests)", tests["ok"])
        )

    lines = ["", "## Days 15 to 17: Final summary", "",
             "### Work completed, Days 1 to 17", "",
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

    all_ok = (all(r["ok"] for r in runs) and zc["ok"] and db_run["ok"] and pv["ok"]
              and (tests is None or tests["ok"]))

    lines = [
        "# ChronosMatch Benchmark Report (Day 17)",
        "",
        f"Teammate 3 - Days 1 to 17 - {time.strftime('%Y-%m-%d %H:%M')}",
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

    lines += _total_tests_section(tests)

    lines += ["", "## Day 11: IPC methods compared", ""]
    if ipc_text:
        lines += ["```", ipc_text.rstrip(), "```"]
    else:
        lines.append("ipc_benchmark.py was not found or was skipped, so this "
                     "section is empty.")

    lines += ["", "## Findings", ""]
    lines += [f"- {text}" for text in _report_findings(runs, zc)]
    lines += _day15_section(runs, zc, ipc_text, tests, db_run, pv)
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