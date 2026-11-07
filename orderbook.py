"""
ChronosMatch - Order Book (Teammate 3)
Combined Day 1-14 implementation.

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

This is a storage/order-book module, not a full matching engine.
"""

import heapq
import math
import random
import struct
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