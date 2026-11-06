
import multiprocessing as mp
import struct
import time
import unittest
from dataclasses import dataclass

BUY = "B"
SELL = "S"

# order_id, side, price_ticks, quantity
EVENT = struct.Struct("!QBQQ")


@dataclass(frozen=True)
class OrderEvent:
    order_id: int
    side: str
    price: int
    quantity: int


def create_orders(count):
    return [
        OrderEvent(
            i,
            BUY if i % 2 == 0 else SELL,
            10000 + i % 500,
            1 + i % 100
        )
        for i in range(count)
    ]


def encode_events(events):
    payload = bytearray(EVENT.size * len(events))

    for i, event in enumerate(events):
        if event.side not in (BUY, SELL):
            raise ValueError("Invalid order side")

        if min(event.order_id, event.price, event.quantity) < 0:
            raise ValueError("Negative order values")

        EVENT.pack_into(
            payload,
            i * EVENT.size,
            event.order_id,
            ord(event.side),
            event.price,
            event.quantity
        )

    return bytes(payload)


def decode_events(payload):
    if len(payload) % EVENT.size:
        raise ValueError("Invalid payload size")

    result = []

    for offset in range(0, len(payload), EVENT.size):
        order_id, side, price, quantity = EVENT.unpack_from(
            payload, offset
        )

        side = chr(side)

        if side not in (BUY, SELL):
            raise ValueError("Invalid side byte")

        result.append(
            OrderEvent(order_id, side, price, quantity)
        )

    return result


def make_batches(events, batch_size):
    for start in range(0, len(events), batch_size):
        yield events[start:start + batch_size]


# ---------- IPC WORKERS ----------

def pipe_receiver(conn, expected):
    received = 0

    while received < expected:
        data = conn.recv_bytes()
        received += len(decode_events(data))

    conn.close()


def queue_receiver(queue, expected):
    received = 0

    while received < expected:
        data = queue.get()
        received += len(decode_events(data))


# ---------- BENCHMARKS ----------

def benchmark_pipe(events, batch_size):
    receiver, sender = mp.Pipe(duplex=False)

    process = mp.Process(
        target=pipe_receiver,
        args=(receiver, len(events))
    )
    process.start()
    receiver.close()

    start = time.perf_counter()

    for batch in make_batches(events, batch_size):
        sender.send_bytes(encode_events(batch))

    sender.close()
    process.join()

    elapsed = time.perf_counter() - start

    if process.exitcode != 0:
        raise RuntimeError("Pipe receiver failed")

    return elapsed


def benchmark_queue(events, batch_size):
    queue = mp.Queue()

    process = mp.Process(
        target=queue_receiver,
        args=(queue, len(events))
    )
    process.start()

    start = time.perf_counter()

    for batch in make_batches(events, batch_size):
        queue.put(encode_events(batch))

    process.join()
    elapsed = time.perf_counter() - start

    queue.close()
    queue.join_thread()

    if process.exitcode != 0:
        raise RuntimeError("Queue receiver failed")

    return elapsed


# ---------- UNIT TESTS ----------

class TestIPCEfficiency(unittest.TestCase):

    def test_encoding_decoding(self):
        orders = create_orders(1000)

        encoded = encode_events(orders)
        decoded = decode_events(encoded)

        self.assertEqual(decoded, orders)

    def test_empty_batch(self):
        self.assertEqual(decode_events(encode_events([])), [])

    def test_invalid_payload(self):
        with self.assertRaises(ValueError):
            decode_events(b"invalid")

    def test_invalid_side(self):
        with self.assertRaises(ValueError):
            encode_events([
                OrderEvent(1, "X", 100, 10)
            ])

    def test_batch_integrity(self):
        orders = create_orders(103)
        received = []

        for batch in make_batches(orders, 16):
            received.extend(decode_events(encode_events(batch)))

        self.assertEqual(received, orders)

    def test_batch_reduces_message_count(self):
        orders = create_orders(1000)
        batches = list(make_batches(orders, 100))

        self.assertEqual(len(batches), 10)


# ---------- PERFORMANCE REPORT ----------

def run_benchmark():

    total_orders = 50000
    orders = create_orders(total_orders)

    print("\nCHRONOSMATCH IPC PERFORMANCE TEST")
    print("=" * 72)
    print("Total orders:", total_orders)
    print("Event size:", EVENT.size, "bytes")
    print("-" * 72)

    print(
        f"{'Method':<12}"
        f"{'Batch':>10}"
        f"{'Messages':>12}"
        f"{'Time(s)':>12}"
        f"{'Orders/s':>16}"
    )

    for name, function in [
        ("PIPE", benchmark_pipe),
        ("QUEUE", benchmark_queue)
    ]:
        for batch_size in [1, 16, 64, 256, 1024]:

            elapsed = function(orders, batch_size)

            messages = (
                total_orders + batch_size - 1
            ) // batch_size

            throughput = total_orders / elapsed

            print(
                f"{name:<12}"
                f"{batch_size:>10}"
                f"{messages:>12}"
                f"{elapsed:>12.4f}"
                f"{throughput:>16,.0f}"
            )


if __name__ == "__main__":
    mp.freeze_support()
    run_benchmark()
