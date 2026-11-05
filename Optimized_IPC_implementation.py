
import struct
import math
from multiprocessing.connection import Connection
from dataclasses import dataclass

BUY = "B"
SELL = "S"

MAGIC = b"CMIP"
VERSION = 1

HEADER = struct.Struct("!4sBIQ")
EVENT = struct.Struct("!QBQQ")

MAX_BATCH = 100000


@dataclass(frozen=True, slots=True)
class Order:
    order_id: int
    side: str
    price: float
    quantity: int


def encode_batch(events, sequence=0, tick_size=0.01):
    """Encode many orders into one compact binary message."""

    if not math.isfinite(tick_size) or tick_size <= 0:
        raise ValueError("Invalid tick size")

    events = list(events)

    if len(events) > MAX_BATCH:
        raise ValueError("Batch too large")

    if not 0 <= sequence < 2**64:
        raise ValueError("Invalid sequence")

    buffer = bytearray(
        HEADER.size + len(events) * EVENT.size
    )

    HEADER.pack_into(
        buffer, 0,
        MAGIC, VERSION, len(events), sequence
    )

    offset = HEADER.size

    for order in events:

        if order.side not in (BUY, SELL):
            raise ValueError("Invalid order side")

        if not isinstance(order.order_id, int) or not 0 <= order.order_id < 2**64:
            raise ValueError("Invalid order ID")

        if not isinstance(order.quantity, int) or not 0 < order.quantity < 2**64:
            raise ValueError("Invalid quantity")

        if not math.isfinite(float(order.price)) or order.price <= 0:
            raise ValueError("Invalid price")

        ticks_float = order.price / tick_size
        ticks = round(ticks_float)

        if not math.isclose(ticks_float, ticks, abs_tol=1e-8):
            raise ValueError("Price does not match tick size")

        EVENT.pack_into(
            buffer,
            offset,
            order.order_id,
            ord(order.side),
            ticks,
            order.quantity
        )

        offset += EVENT.size

    return bytes(buffer)


def decode_batch(data, tick_size=0.01):
    """Decode a binary batch without unnecessary slicing."""

    if not math.isfinite(tick_size) or tick_size <= 0:
        raise ValueError("Invalid tick size")

    view = memoryview(data)

    if len(view) < HEADER.size:
        raise ValueError("Incomplete header")

    magic, version, count, sequence = HEADER.unpack_from(view)

    if magic != MAGIC or version != VERSION:
        raise ValueError("Invalid IPC header")

    if count > MAX_BATCH:
        raise ValueError("Batch limit exceeded")

    expected = HEADER.size + count * EVENT.size

    if len(view) != expected:
        raise ValueError("Invalid message length")

    orders = []
    offset = HEADER.size

    for _ in range(count):

        order_id, side, ticks, quantity = EVENT.unpack_from(
            view, offset
        )

        offset += EVENT.size

        if side not in (ord(BUY), ord(SELL)):
            raise ValueError("Invalid side")

        if ticks == 0 or quantity == 0:
            raise ValueError("Invalid price or quantity")

        orders.append(
            Order(
                order_id,
                chr(side),
                ticks * tick_size,
                quantity
            )
        )

    return sequence, orders


def send_batch(connection, orders, sequence=0):
    """Send an entire batch using one IPC operation."""

    payload = encode_batch(
        orders,
        sequence=sequence
    )

    connection.send_bytes(payload)

    return len(payload)


def receive_batch(connection):
    """Receive and decode one batch."""

    data = connection.recv_bytes(
        maxlength=HEADER.size + MAX_BATCH * EVENT.size
    )

    return decode_batch(data)


class IPCBatcher:

    def __init__(self, batch_size=256):
        if not 1 <= batch_size <= MAX_BATCH:
            raise ValueError("Invalid batch size")

        self.batch_size = batch_size
        self.orders = []
        self.sequence = 0

    def add(self, order):

        self.orders.append(order)

        if len(self.orders) >= self.batch_size:
            return self.flush()

        return None

    def flush(self):

        if not self.orders:
            return None

        payload = encode_batch(
            self.orders,
            sequence=self.sequence
        )

        self.sequence += 1
        self.orders.clear()

        return payload
