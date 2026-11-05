"""Day 12 (Teammate 3): measure how efficient the order book's IPC path is.

Measures the code in orderbook.py itself (encode_events, decode_events,
ingest_binary, send_events, ingest_from_connection):

  1. Codec and ingest cost per 100,000 events.
  2. A producer process feeding an order book process over a pipe, for
     several batch sizes. Besides wall time it reports CPU time for each
     side. On a machine with 2+ cores the two sides overlap, so the slower
     side's CPU time is what limits throughput ("overlap estimate").

Run:   python ipc_efficiency.py            (100,000 events)
       python ipc_efficiency.py 20000      (faster)
"""

import multiprocessing as mp
import random
import sys
import time

from orderbook import BUY, SELL, OrderBook, decode_events, encode_events, send_events

BATCH_SIZES = (100, 500, 1000, 2000, 5000)


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


def child_book(conn, out):
    book = OrderBook()
    out.put("ready")
    cpu = time.process_time()
    total = book.ingest_from_connection(conn)
    out.put((total, len(book), time.process_time() - cpu))


def timed(fn):
    start = time.perf_counter()
    result = fn()
    return result, time.perf_counter() - start


def codec_costs(events, size=500):
    chunks = [events[i:i + size] for i in range(0, len(events), size)]
    encoded, t_enc = timed(lambda: [encode_events(c) for c in chunks])
    _, t_dec = timed(lambda: [decode_events(b) for b in encoded])
    book = OrderBook()
    _, t_ing = timed(lambda: [book.ingest_binary(b) for b in encoded])
    plain = OrderBook()
    _, t_plain = timed(lambda: [plain.ingest_firehose(c) for c in chunks])
    return t_enc, t_dec, t_ing, t_plain, sum(map(len, encoded)) / len(events)


def pipe_run(ctx, events, batch_size):
    out = ctx.Queue()
    recv_end, send_end = ctx.Pipe(duplex=False)
    proc = ctx.Process(target=child_book, args=(recv_end, out))
    proc.start()
    out.get()                                        # child is ready
    cpu = time.process_time()
    start = time.perf_counter()
    send_events(send_end, events, batch_size)
    total, size, child_cpu = out.get()
    wall = time.perf_counter() - start
    parent_cpu = time.process_time() - cpu
    proc.join()
    assert total == size == len(events), (total, size)
    return wall, parent_cpu, child_cpu


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
    events = make_events(n)
    print(f"IPC efficiency with {n:,} events\n")

    t_enc, t_dec, t_ing, t_plain, per_event = codec_costs(events)
    print("1. Codec and ingest (batches of 500)")
    print(f"   encode_events:                {t_enc:.3f} s   ({n / t_enc:>10,.0f}/s)")
    print(f"   decode_events:                {t_dec:.3f} s   ({n / t_dec:>10,.0f}/s)")
    print(f"   ingest_binary (decode+insert): {t_ing:.3f} s   ({n / t_ing:>10,.0f}/s)")
    print(f"   ingest_firehose (dicts):      {t_plain:.3f} s   ({n / t_plain:>10,.0f}/s)")
    print(f"   message size: {per_event:.1f} bytes per event")

    ctx = mp.get_context("spawn")
    print("\n2. Producer process -> order book process over a pipe")
    print(f"   {'batch':>6}{'wall s':>9}{'events/s':>11}{'producer CPU':>14}{'book CPU':>10}{'overlap est.':>14}")
    for size in BATCH_SIZES:
        wall, p_cpu, c_cpu = pipe_run(ctx, events, size)
        est = n / max(p_cpu, c_cpu)
        print(f"   {size:>6}{wall:>8.3f}s{n / wall:>11,.0f}{p_cpu:>13.3f}s{c_cpu:>9.3f}s{est:>13,.0f}/s")


if __name__ == "__main__":
    mp.freeze_support()
    main()