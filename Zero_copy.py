"""ChronosMatch - Zero-copy architecture (Teammate 3)

Day 13: move order batches between processes WITHOUT extra copies.
Day 14: verified by test_day14_zero_copy.py and zero_copy_verify.py.

Where copies come from, and how this module removes them
--------------------------------------------------------
Pipe / queue path   producer bytes -> kernel pipe -> a NEW bytes object in the
                    consumer (recv_bytes) -> decode. At least one copy per
                    message. Day 14 measured that recv_bytes_into does not
                    help: CPython reads the message into a temporary buffer
                    first and then copies it, so it is no better.
Shared-memory ring  the producer encodes straight into a shared memory slot
                    and the consumer decodes straight out of that same slot.
                    The batch bytes are written once and never copied.

What can NOT be zero-copy: each order becomes Python objects (str, float,
int, Order). Those are built from the shared bytes, one time, in the book
process. Zero-copy here means the batch itself is never duplicated on the way.

Standalone: needs only Order, OrderBook (add_order) from orderbook.py.

Batch layout (little endian), identical to the Day 12 columnar format:
    uint32 count
    count x uint16 id length, count x uint8 side length,
    count x float64 price,   count x int64 quantity
    all ID text (UTF-8) joined, then all side text (UTF-8) joined
Ring slot layout:  uint32 payload size (0 = end of stream), then the batch.
"""

import multiprocessing as mp
import struct
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from itertools import islice
from multiprocessing import shared_memory

from orderbook import Order

_FRAME = struct.Struct("<I")
_SIZE = struct.Struct("<I")
_ROW_BYTES = 2 + 1 + 8 + 8
_INT64_MIN, _INT64_MAX = -(2**63), 2**63 - 1


# ------------------------------------------------------------------ encoding
def _prepare(events):
    """Validate events strictly and split them into columns (no bytes built yet)."""
    events = list(events)
    id_texts, side_texts = [], []
    id_lens, side_lens, prices, quantities = [], [], [], []
    for n, event in enumerate(events, start=1):
        if not (isinstance(event, dict) or isinstance(event, Mapping)):
            raise TypeError(f"Event {n} must be a mapping")
        try:
            order_id = event["order_id"]
            side = event["side"]
            price = event["price"]
            quantity = event["quantity"]
        except KeyError as exc:
            raise ValueError(f"Event {n} is missing field {exc.args[0]!r}") from exc

        if not isinstance(order_id, str):
            raise ValueError(f"Event {n} order_id must be a string")
        if not isinstance(side, str):
            raise ValueError(f"Event {n} side must be a string")
        if isinstance(price, bool) or not isinstance(price, (int, float)):
            raise ValueError(f"Event {n} price must be a number")
        if isinstance(quantity, bool) or not isinstance(quantity, int):
            raise ValueError(f"Event {n} quantity must be an integer")
        if not _INT64_MIN <= quantity <= _INT64_MAX:
            raise ValueError(f"Event {n} quantity is out of range")

        id_bytes = order_id.encode("utf-8")
        side_bytes = side.encode("utf-8")
        if len(id_bytes) > 0xFFFF:
            raise ValueError(f"Event {n} order_id is too long")
        if len(side_bytes) > 0xFF:
            raise ValueError(f"Event {n} side is too long")
        try:
            price = float(price)
        except OverflowError as exc:
            raise ValueError(f"Event {n} price is out of range") from exc

        id_texts.append(id_bytes)
        side_texts.append(side_bytes)
        id_lens.append(len(id_bytes))
        side_lens.append(len(side_bytes))
        prices.append(price)
        quantities.append(quantity)
    return len(events), id_texts, side_texts, id_lens, side_lens, prices, quantities


def encoded_size(count, id_lens, side_lens):
    return _FRAME.size + count * _ROW_BYTES + sum(id_lens) + sum(side_lens)


def _write_prepared(buffer, prepared, offset):
    k, id_texts, side_texts, id_lens, side_lens, prices, quantities = prepared
    size = encoded_size(k, id_lens, side_lens)
    view = memoryview(buffer)
    try:
        if view.readonly:
            raise ValueError("Buffer is read-only")
        if offset < 0 or offset + size > len(view):
            raise ValueError(
                f"Batch of {k} events needs {size} bytes but only "
                f"{max(len(view) - offset, 0)} are available"
            )
        struct.pack_into(
            f"<I{k}H{k}B{k}d{k}q", view, offset, k, *id_lens, *side_lens, *prices, *quantities
        )
        pos = offset + _FRAME.size + k * _ROW_BYTES
        ids = b"".join(id_texts)
        view[pos:pos + len(ids)] = ids
        pos += len(ids)
        sides = b"".join(side_texts)
        view[pos:pos + len(sides)] = sides
    finally:
        view.release()
    return size


def encode_events_into(buffer, events: Iterable[Mapping[str, object]], offset: int = 0) -> int:
    """Encode a batch directly into a writable buffer (no intermediate bytes).

    Everything is checked BEFORE the first byte is written, so a bad event or
    a batch that does not fit leaves the buffer exactly as it was.
    Returns the number of bytes written.
    """
    return _write_prepared(buffer, _prepare(events), offset)


def encode_events(events: Iterable[Mapping[str, object]]) -> bytes:
    """Encode a batch to bytes (convenience; the ring uses encode_events_into)."""
    prepared = _prepare(events)
    out = bytearray(encoded_size(prepared[0], prepared[3], prepared[4]))
    _write_prepared(out, prepared, 0)
    return bytes(out)


# ------------------------------------------------------------------ decoding
def _split_text(view, lengths):
    """Cut joined UTF-8 text back into one string per length (lengths are bytes)."""
    pieces, pos = [], 0
    try:
        try:
            text = str(view, "ascii")              # common case: decode once
        except UnicodeDecodeError:
            for length in lengths:
                nxt = pos + length
                part = view[pos:nxt]
                try:
                    pieces.append(str(part, "utf-8"))
                finally:
                    part.release()
                pos = nxt
            return pieces
        for length in lengths:
            nxt = pos + length
            pieces.append(text[pos:nxt])
            pos = nxt
        return pieces
    finally:
        view.release()


def decode_orders(buffer) -> list[Order]:
    """Decode a batch straight from any bytes-like object into Order objects.

    Works on bytes, bytearray, memoryview or a shared-memory slice WITHOUT
    copying the batch, and releases every view it takes, so the source buffer
    can be resized or closed afterwards. Raises ValueError if damaged.
    """
    view = memoryview(buffer)
    try:
        if view.ndim != 1 or view.itemsize != 1 or view.format != "B":
            view = view.cast("B")
        if len(view) < _FRAME.size:
            raise ValueError("Truncated batch")
        (k,) = _FRAME.unpack_from(view, 0)
        fixed_end = _FRAME.size + k * _ROW_BYTES
        if len(view) < fixed_end:
            raise ValueError("Truncated batch")

        columns = struct.unpack_from(f"<{k}H{k}B{k}d{k}q", view, _FRAME.size)
        id_lens = columns[:k]
        side_lens = columns[k:2 * k]
        prices = columns[2 * k:3 * k]
        quantities = columns[3 * k:]

        ids_end = fixed_end + sum(id_lens)
        sides_end = ids_end + sum(side_lens)
        if len(view) < sides_end:
            raise ValueError("Truncated batch")
        if len(view) > sides_end:
            raise ValueError("Unexpected extra bytes after the last event")

        ids = _split_text(view[fixed_end:ids_end], id_lens)
        sides = _split_text(view[ids_end:sides_end], side_lens)
    finally:
        view.release()
    return [Order(i, s, p, q) for i, s, p, q in zip(ids, sides, prices, quantities)]


def ingest_batch(book, buffer) -> int:
    """Decode one batch and add it to the book. Errors read like ingest_firehose."""
    ingested = 0
    for event_number, order in enumerate(decode_orders(buffer), start=1):
        try:
            book.add_order(order)
        except ValueError as exc:
            raise ValueError(f"Firehose event {event_number}: {exc}") from exc
        ingested += 1
    return ingested


# ----------------------------------------------------------- shared-memory ring
def _attach(name):
    """Attach to an existing segment.

    Python 3.13+: track=False keeps the resource tracker out of it. Older
    Pythons: a plain attach is correct for processes started by
    multiprocessing from the owner, because they share the owner's resource
    tracker. (A completely unrelated process on Python 3.12 or older would
    need its own handling; use 3.13+ there.)
    """
    try:
        return shared_memory.SharedMemory(name=name, track=False)      # Python 3.13+
    except TypeError:
        return shared_memory.SharedMemory(name=name)


class ShmRing:
    """Single-producer, single-consumer ring of fixed-size slots in shared memory.

    Two semaphores do all the waiting and ordering: `free` counts empty slots,
    `filled` counts slots holding a batch. A slot is only written after
    `free` is acquired and only read after `filled` is acquired, so the two
    sides never touch the same slot at once.

    One producer object and one consumer object only. Create it with
    ShmRing.create(), give handle() to the other process, and attach() there.
    """

    def __init__(self, slots, slot_bytes, shm, free, filled, owner, timeout=30.0):
        self.slots = slots
        self.slot_bytes = slot_bytes
        self.capacity = slot_bytes - _SIZE.size
        self.timeout = timeout
        self._shm = shm
        self._view = shm.buf
        self._free = free
        self._filled = filled
        self._owner = owner
        self._head = 0          # next slot the producer writes (producer side only)
        self._tail = 0          # next slot the consumer reads (consumer side only)

    @classmethod
    def create(cls, slots: int = 16, slot_bytes: int = 256 * 1024, ctx=None, timeout=30.0):
        if slots < 1:
            raise ValueError("slots must be at least 1")
        if slot_bytes <= _SIZE.size + _FRAME.size:
            raise ValueError("slot_bytes is too small")
        ctx = ctx or mp.get_context("spawn")
        shm = shared_memory.SharedMemory(create=True, size=slots * slot_bytes)
        return cls(slots, slot_bytes, shm, ctx.Semaphore(slots), ctx.Semaphore(0),
                   owner=True, timeout=timeout)

    def handle(self):
        """Small picklable tuple to pass to the other process."""
        return (self._shm.name, self.slots, self.slot_bytes, self._free, self._filled)

    @classmethod
    def attach(cls, handle, timeout=30.0):
        name, slots, slot_bytes, free, filled = handle
        return cls(slots, slot_bytes, _attach(name), free, filled, owner=False, timeout=timeout)

    # -- producer side
    def write(self, events) -> int:
        """Encode a batch straight into the next free slot. Returns bytes written."""
        if not self._free.acquire(timeout=self.timeout):
            raise TimeoutError("Ring is full: the consumer is not keeping up")
        base = self._head * self.slot_bytes
        payload = self._view[base + _SIZE.size:base + self.slot_bytes]
        try:
            size = encode_events_into(payload, events)
        except BaseException:
            self._free.release()                   # slot was not used
            raise
        finally:
            payload.release()
        _SIZE.pack_into(self._view, base, size)
        self._head = (self._head + 1) % self.slots
        self._filled.release()
        return size

    def close_stream(self) -> None:
        """Tell the consumer no more batches are coming."""
        if not self._free.acquire(timeout=self.timeout):
            raise TimeoutError("Ring is full: the consumer is not keeping up")
        base = self._head * self.slot_bytes
        _SIZE.pack_into(self._view, base, 0)
        self._head = (self._head + 1) % self.slots
        self._filled.release()

    # -- consumer side
    @contextmanager
    def read(self):
        """Yield a memoryview of the next batch, in place (no copy), or None at the end.

        The view is only valid inside the `with` block; the slot is handed back
        to the producer when the block ends.
        """
        if not self._filled.acquire(timeout=self.timeout):
            raise TimeoutError("Ring is empty: the producer stopped sending")
        base = self._tail * self.slot_bytes
        (size,) = _SIZE.unpack_from(self._view, base)
        self._tail = (self._tail + 1) % self.slots
        if size > self.capacity:
            self._free.release()
            raise ValueError("Corrupt slot: length is larger than the slot")
        view = self._view[base + _SIZE.size:base + _SIZE.size + size] if size else None
        try:
            yield view
        finally:
            if view is not None:
                view.release()
            self._free.release()

    # -- cleanup
    def close(self) -> None:
        self._view = None
        self._shm.close()

    def unlink(self) -> None:
        if self._owner:
            self._shm.unlink()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        self.unlink()


def send_events_shm(ring: ShmRing, events: Iterable[Mapping[str, object]], batch_size: int = 1000) -> int:
    """Send events through a ring in batches, then mark the end. Returns events sent."""
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    sent = 0
    source = iter(events)
    while True:
        batch = list(islice(source, batch_size))
        if not batch:
            break
        ring.write(batch)
        sent += len(batch)
    ring.close_stream()
    return sent


def ingest_ring(book, ring: ShmRing) -> int:
    """Read batches from a ring until the end marker and add them to the book."""
    total = 0
    while True:
        with ring.read() as view:
            if view is None:
                return total
            total += ingest_batch(book, view)