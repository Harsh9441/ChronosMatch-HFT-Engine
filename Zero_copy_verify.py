"""Day 14 (Teammate 3): measure and verify the zero-copy architecture.

Compares moving 100,000 orders from a producer process to an order book
process two ways:
  pipe  - send_bytes / recv_bytes (the kernel copies, the consumer allocates)
  ring  - shared-memory ring (encoded in place, decoded in place)
and checks that both build exactly the same book as the normal
ingest_firehose path.

Run:   python zero_copy_verify.py            (100,000 orders)
       python zero_copy_verify.py 20000
"""

import hashlib
import multiprocessing as mp
import random
import statistics
import struct
import sys
import threading
import time
import tracemalloc

import zero_copy as zc
from orderbook import BUY, SELL, OrderBook

BATCH = 1000
REPEATS = 3


def make_events(count, seed=0):
    rng = random.Random(seed)
    return [
        {
            "order_id": str(i),
            "side": rng.choice([BUY, SELL]),
            "price": rng.randint(9900, 10100),
            "quantity": rng.randint(1, 100),
        }
        for i in range(1, count + 1)
    ]


def book_digest(book):
    rows = [(o.order_id, o.side, o.price, o.quantity) for o in book.get_all_orders()]
    return hashlib.md5(repr(rows).encode()).hexdigest()[:12]


def child_pipe(conn, out):
    book = OrderBook()
    out.put("ready")
    cpu = time.process_time()
    total = 0
    while True:
        data = conn.recv_bytes()
        if not data:
            break
        total += zc.ingest_batch(book, data)
    out.put((total, time.process_time() - cpu))
    out.put(book_digest(book))


def child_ring(handle, out):
    ring = zc.ShmRing.attach(handle)
    book = OrderBook()
    out.put("ready")
    cpu = time.process_time()
    total = zc.ingest_ring(book, ring)
    out.put((total, time.process_time() - cpu))
    out.put(book_digest(book))
    ring.close()


def run_pipe(ctx, events):
    out = ctx.Queue()
    recv_end, send_end = ctx.Pipe(duplex=False)
    proc = ctx.Process(target=child_pipe, args=(recv_end, out))
    proc.start()
    out.get()
    cpu, start = time.process_time(), time.perf_counter()
    for i in range(0, len(events), BATCH):
        send_end.send_bytes(zc.encode_events(events[i:i + BATCH]))
    send_end.send_bytes(b"")
    total, child_cpu = out.get()
    wall = time.perf_counter() - start
    parent_cpu = time.process_time() - cpu
    digest = out.get()
    proc.join()
    return wall, parent_cpu, child_cpu, total, digest


def run_ring(ctx, events):
    ring = zc.ShmRing.create(slots=16, slot_bytes=64 * 1024, ctx=ctx)
    out = ctx.Queue()
    proc = ctx.Process(target=child_ring, args=(ring.handle(), out))
    proc.start()
    out.get()
    cpu, start = time.process_time(), time.perf_counter()
    zc.send_events_shm(ring, events, BATCH)
    total, child_cpu = out.get()
    wall = time.perf_counter() - start
    parent_cpu = time.process_time() - cpu
    digest = out.get()
    proc.join()
    ring.close()
    ring.unlink()
    return wall, parent_cpu, child_cpu, total, digest


def peak_extra(fn):
    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        base = tracemalloc.get_traced_memory()[0]
        fn()
        return tracemalloc.get_traced_memory()[1] - base
    finally:
        tracemalloc.stop()


def allocation_check():
    events = make_events(4000)
    payload = zc.encode_events(events)
    ring = zc.ShmRing.create(slots=2, slot_bytes=256 * 1024)
    try:
        ring.write(events)
        ring.write(events)
        recv_end, send_end = mp.Pipe(duplex=False)
        sender = threading.Thread(target=lambda: send_end.send_bytes(payload))
        sender.start()
        pipe_peak = peak_extra(lambda: recv_end.recv_bytes())
        sender.join()

        size = {}

        def in_place():
            with ring.read() as view:
                size["n"] = view.nbytes
                struct.unpack_from("<I", view, 0)

        ring_peak = peak_extra(in_place)
        with ring.read():
            pass
        ring.write(events)
        with ring.read() as view:
            same_memory = view.obj is ring._shm.buf.obj
        return len(payload), pipe_peak, ring_peak, same_memory
    finally:
        ring.close()
        ring.unlink()


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
    events = make_events(n)
    plain = OrderBook()
    plain.ingest_firehose(events)
    expect = book_digest(plain)
    ctx = mp.get_context("spawn")
    print(f"Zero-copy verification with {n:,} orders, batches of {BATCH}\n")

    print("1. Producer process -> order book process "
          f"(median of {REPEATS} runs)")
    print(f"   {'path':<8}{'wall s':>9}{'orders/s':>12}{'producer CPU':>14}{'book CPU':>10}{'identical':>11}")
    for label, run in (("pipe", run_pipe), ("ring", run_ring)):
        runs = [run(ctx, events) for _ in range(REPEATS)]
        ok = all(r[3] == n and r[4] == expect for r in runs)
        wall = statistics.median(r[0] for r in runs)
        p_cpu = statistics.median(r[1] for r in runs)
        c_cpu = statistics.median(r[2] for r in runs)
        print(f"   {label:<8}{wall:>8.3f}s{n / wall:>12,.0f}{p_cpu:>13.3f}s{c_cpu:>9.3f}s{'yes' if ok else 'NO':>11}")

    size, pipe_peak, ring_peak, same_memory = allocation_check()
    print(f"\n2. Memory touched to receive one {size:,}-byte batch")
    print(f"   pipe recv_bytes:        {pipe_peak:>9,} bytes allocated  (a full copy)")
    print(f"   ring read in place:     {ring_peak:>9,} bytes allocated")
    print(f"   reader's view is the shared segment itself: {'yes' if same_memory else 'NO'}")


if __name__ == "__main__":
    mp.freeze_support()
    main()