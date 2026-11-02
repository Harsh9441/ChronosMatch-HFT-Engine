"""ChronosMatch - IPC analysis (Teammate 3, Day 11)

IPC = inter-process communication: how an order travels from one program
(the firehose) to another program (the order book / matching engine).

This script sends the SAME orders between two real processes using several
methods, and measures speed, delay and CPU use for each one:

    1. multiprocessing.Queue   (pickles every message)
    2. Pipe + struct           (raw bytes through the operating system)
    3. Shared-memory ring      (both programs read and write the same memory:
                                this is the "zero-copy" idea of the project)

Each method is tried one message at a time and in batches of 1,000.

Usage:
    python ipc_benchmark.py                 # 200,000 messages per method
    python ipc_benchmark.py --quick         # 50,000 messages (about 1 minute)
    python ipc_benchmark.py --messages 500000
    python ipc_benchmark.py --save          # also write ipc_results.txt
"""

import multiprocessing as mp
import struct
import sys
import time
import traceback
from multiprocessing import shared_memory

# ---------------------------------------------------------------------------
# The message: one order = id, side(0 buy/1 sell), type, price, quantity, time
# ---------------------------------------------------------------------------
WIRE = struct.Struct("<QBBIIQ")          # 26 bytes, the team's order record
REC = struct.Struct("<QBBIIQ6x")         # same record padded to 32 bytes (aligned slot)

SLOT = 32
HEAD_OFF = 0                             # producer's write counter
TAIL_OFF = 64                            # consumer's read counter (own cache line)
DATA_OFF = 128
CAPACITY = 1 << 16                       # 65,536 slots = 2 MiB of shared memory

BATCH = 1000
TIMEOUT = 180                            # seconds before a method is declared stuck


def make_record(oid, ts):
    return (oid, oid & 1, 0, 9900 + (oid * 7) % 200, 1 + oid % 100, ts)


def expected_quantity(n):
    return sum(1 + oid % 100 for oid in range(1, n + 1))


class Tally:
    """Checks every received message and samples the delay of some of them."""

    __slots__ = ("count", "next_id", "bad", "qty", "lats")

    def __init__(self):
        self.count = 0
        self.next_id = 1
        self.bad = 0
        self.qty = 0
        self.lats = []

    def see(self, rec):
        if rec[0] != self.next_id:
            self.bad += 1                       # lost, repeated or reordered
        self.next_id = rec[0] + 1
        self.qty += rec[4]
        self.count += 1
        if not self.count & 63:                 # every 64th message
            self.lats.append(time.time_ns() - rec[5])


def to_events(records):
    """Turn wire records into the dictionaries OrderBook.ingest_firehose wants."""
    return [
        {"order_id": str(r[0]), "side": "B" if r[1] == 0 else "S",
         "price": r[3], "quantity": r[4]}
        for r in records
    ]


def attach(name):
    try:
        return shared_memory.SharedMemory(name=name, track=False)   # Python 3.13+
    except TypeError:
        return shared_memory.SharedMemory(name=name)


def finish(role, results, tally, t_ns, cpu0, extra=None):
    out = {"role": role, "t_ns": t_ns, "cpu": time.process_time() - cpu0}
    if tally is not None:
        out.update(count=tally.count, bad=tally.bad, qty=tally.qty, lats=tally.lats)
    if extra:
        out.update(extra)
    results.put(out)


# ---------------------------------------------------------------------------
# 1. multiprocessing.Queue
# ---------------------------------------------------------------------------
def queue_producer(q, n, batch, ready, go, results):
    try:
        ready.set()
        go.wait()
        cpu0 = time.process_time()
        t0 = time.time_ns()
        if batch == 1:
            for oid in range(1, n + 1):
                q.put(make_record(oid, time.time_ns()))
        else:
            oid = 1
            while oid <= n:
                ts = time.time_ns()
                end = min(oid + batch, n + 1)
                q.put([make_record(i, ts) for i in range(oid, end)])
                oid = end
        q.put(None)
        finish("producer", results, None, t0, cpu0)
    except Exception:
        results.put({"role": "producer", "error": traceback.format_exc()})


def queue_consumer(q, n, batch, sink, ready, results):
    try:
        book = None
        if sink == "book":
            from orderbook import OrderBook
            book = OrderBook()
        tally = Tally()
        ready.set()
        cpu0 = time.process_time()
        while True:
            item = q.get()
            if item is None:
                break
            if batch == 1:
                tally.see(item)
            else:
                for rec in item:
                    tally.see(rec)
                if book is not None:
                    book.ingest_firehose(to_events(item))
        t_end = time.time_ns()
        extra = {"book_count": len(book)} if book is not None else None
        finish("consumer", results, tally, t_end, cpu0, extra)
    except Exception:
        results.put({"role": "consumer", "error": traceback.format_exc()})


# ---------------------------------------------------------------------------
# 2. Pipe + struct
# ---------------------------------------------------------------------------
def pipe_producer(conn, n, batch, ready, go, results):
    try:
        ready.set()
        go.wait()
        cpu0 = time.process_time()
        t0 = time.time_ns()
        size = WIRE.size
        if batch == 1:
            for oid in range(1, n + 1):
                conn.send_bytes(WIRE.pack(*make_record(oid, time.time_ns())))
        else:
            oid = 1
            while oid <= n:
                ts = time.time_ns()
                end = min(oid + batch, n + 1)
                buf = bytearray(size * (end - oid))
                for j, i in enumerate(range(oid, end)):
                    WIRE.pack_into(buf, j * size, *make_record(i, ts))
                conn.send_bytes(buf)
                oid = end
        conn.send_bytes(b"")                       # empty message = finished
        finish("producer", results, None, t0, cpu0)
    except Exception:
        results.put({"role": "producer", "error": traceback.format_exc()})


def pipe_consumer(conn, n, batch, sink, ready, results):
    try:
        tally = Tally()
        ready.set()
        cpu0 = time.process_time()
        size = WIRE.size
        while True:
            data = conn.recv_bytes()
            if not data:
                break
            for off in range(0, len(data), size):
                tally.see(WIRE.unpack_from(data, off))
        finish("consumer", results, tally, time.time_ns(), cpu0)
    except Exception:
        results.put({"role": "consumer", "error": traceback.format_exc()})


# ---------------------------------------------------------------------------
# 3. Shared-memory ring buffer (one writer, one reader, no locks)
#    The producer writes records straight into the shared block and then
#    publishes them by moving HEAD. The consumer reads straight out of the same
#    block and then frees the space by moving TAIL. No bytes are copied
#    between the two programs.
#
#    HEAD and TAIL are read and written through a memoryview cast to "Q", which
#    moves all 8 bytes in one step. (struct's "<Q" moves them ONE BYTE AT A TIME,
#    so the other process can see a half-written number and the counters jump.)
#
#    Safe on x86/x64 CPUs, which never reorder these writes. ARM would need
#    memory barriers.
# ---------------------------------------------------------------------------
def shm_producer(name, n, batch, ready, go, results):
    try:
        shm = attach(name)
        buf = shm.buf
        head_raw = buf[HEAD_OFF:HEAD_OFF + 8]
        tail_raw = buf[TAIL_OFF:TAIL_OFF + 8]
        head_mv = head_raw.cast("Q")
        tail_mv = tail_raw.cast("Q")
        mask = CAPACITY - 1
        ready.set()
        go.wait()
        cpu0 = time.process_time()
        t0 = time.time_ns()
        head = 0
        tail_seen = 0
        sent = 0
        while sent < n:
            free = CAPACITY - (head - tail_seen)
            if free < min(batch, n - sent):                 # looks full: re-read TAIL
                tail_seen = tail_mv[0]
                free = CAPACITY - (head - tail_seen)
                if free == 0:
                    time.sleep(0)                           # wait for the consumer
                    continue
            k = min(batch, n - sent, free)
            ts = time.time_ns()
            for j in range(k):
                oid = sent + j + 1
                stamp = ts if batch > 1 else time.time_ns()
                REC.pack_into(buf, DATA_OFF + ((head + j) & mask) * SLOT,
                              *make_record(oid, stamp))
            head += k
            head_mv[0] = head                               # publish
            sent += k
        finish("producer", results, None, t0, cpu0)
        for view in (head_mv, head_raw, tail_mv, tail_raw):
            view.release()
        del buf
        shm.close()
    except Exception:
        results.put({"role": "producer", "error": traceback.format_exc()})


def shm_consumer(name, n, sink, ready, results):
    try:
        book = None
        if sink == "book":
            from orderbook import OrderBook
            book = OrderBook()
        shm = attach(name)
        buf = shm.buf
        head_raw = buf[HEAD_OFF:HEAD_OFF + 8]
        tail_raw = buf[TAIL_OFF:TAIL_OFF + 8]
        head_mv = head_raw.cast("Q")
        tail_mv = tail_raw.cast("Q")
        mask = CAPACITY - 1
        tally = Tally()
        ready.set()
        cpu0 = time.process_time()
        tail = 0
        while tally.count < n:
            available = head_mv[0] - tail
            if available == 0:
                time.sleep(0)                               # nothing new yet
                continue
            records = []
            for j in range(available):
                rec = REC.unpack_from(buf, DATA_OFF + ((tail + j) & mask) * SLOT)
                tally.see(rec)
                if book is not None:
                    records.append(rec)
            tail += available
            tail_mv[0] = tail                               # free the slots
            if book is not None:
                book.ingest_firehose(to_events(records))
        t_end = time.time_ns()
        extra = {"book_count": len(book)} if book is not None else None
        finish("consumer", results, tally, t_end, cpu0, extra)
        for view in (head_mv, head_raw, tail_mv, tail_raw):
            view.release()
        del buf
        shm.close()
    except Exception:
        results.put({"role": "consumer", "error": traceback.format_exc()})


# ---------------------------------------------------------------------------
# Running one method and turning the numbers into a result row
# ---------------------------------------------------------------------------
def percentile(values, p):
    if not values:
        return float("nan")
    values = sorted(values)
    return values[min(len(values) - 1, int(len(values) * p / 100))]


def run_method(ctx, label, kind, batch, n, sink="count"):
    ready_p, ready_c, go = ctx.Event(), ctx.Event(), ctx.Event()
    results = ctx.Queue()
    shm = None
    if kind == "queue":
        q = ctx.Queue()
        prod = ctx.Process(target=queue_producer, args=(q, n, batch, ready_p, go, results))
        cons = ctx.Process(target=queue_consumer, args=(q, n, batch, sink, ready_c, results))
    elif kind == "pipe":
        recv_end, send_end = ctx.Pipe(duplex=False)
        prod = ctx.Process(target=pipe_producer, args=(send_end, n, batch, ready_p, go, results))
        cons = ctx.Process(target=pipe_consumer, args=(recv_end, n, batch, sink, ready_c, results))
    else:
        shm = shared_memory.SharedMemory(create=True, size=DATA_OFF + CAPACITY * SLOT)
        shm.buf[:DATA_OFF] = bytes(DATA_OFF)
        prod = ctx.Process(target=shm_producer, args=(shm.name, n, batch, ready_p, go, results))
        cons = ctx.Process(target=shm_consumer, args=(shm.name, n, sink, ready_c, results))

    row = {"label": label, "n": n, "error": None}
    try:
        cons.start()
        prod.start()
        if not (ready_c.wait(60) and ready_p.wait(60)):
            raise RuntimeError("a process did not start in 60 s")
        go.set()
        got = {}
        deadline = time.time() + TIMEOUT
        while len(got) < 2:
            res = results.get(timeout=max(1, deadline - time.time()))
            if "error" in res:
                raise RuntimeError(f"{res['role']} failed:\n{res['error']}")
            got[res["role"]] = res
        p, c = got["producer"], got["consumer"]
        elapsed = (c["t_ns"] - p["t_ns"]) / 1e9
        row.update(
            elapsed=elapsed,
            rate=n / elapsed,
            p50=percentile(c["lats"], 50) / 1e6,
            p99=percentile(c["lats"], 99) / 1e6,
            cpu_p=p["cpu"],
            cpu_c=c["cpu"],
            ok=(c["count"] == n and c["bad"] == 0 and c["qty"] == expected_quantity(n)
                and c.get("book_count", n) == n),
        )
    except Exception as error:                                   # noqa: BLE001
        row["error"] = str(error).strip().splitlines()[-1] if str(error).strip() else repr(error)
    finally:
        for proc in (prod, cons):
            proc.join(10)
            if proc.is_alive():
                proc.terminate()
        if shm is not None:
            shm.close()
            try:
                shm.unlink()
            except FileNotFoundError:
                pass
    return row


def baseline(n):
    """Pack and unpack in ONE process: the cost of the message format alone."""
    buf = bytearray(WIRE.size)
    tally = Tally()
    cpu0 = time.process_time()
    t0 = time.perf_counter()
    for oid in range(1, n + 1):
        WIRE.pack_into(buf, 0, *make_record(oid, time.time_ns()))
        tally.see(WIRE.unpack_from(buf, 0))
    elapsed = time.perf_counter() - t0
    return {
        "label": "No IPC: pack + unpack in one process (ceiling)", "n": n, "error": None,
        "elapsed": elapsed, "rate": n / elapsed,
        "p50": percentile(tally.lats, 50) / 1e6, "p99": percentile(tally.lats, 99) / 1e6,
        "cpu_p": time.process_time() - cpu0, "cpu_c": 0.0, "ok": tally.bad == 0,
    }


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def table(rows):
    head = (f"{'method':<50} {'msgs/s':>10} {'time s':>7} {'p50 ms':>8} {'p99 ms':>8} "
            f"{'cpu send':>8} {'cpu recv':>8}  check")
    lines = [head, "-" * len(head)]
    for r in rows:
        if r["error"]:
            lines.append(f"{r['label']:<50} FAILED: {r['error'][:60]}")
            continue
        lines.append(
            f"{r['label']:<50} {r['rate']:>10,.0f} {r['elapsed']:>7.2f} {r['p50']:>8.2f} "
            f"{r['p99']:>8.2f} {r['cpu_p']:>7.2f}s {r['cpu_c']:>7.2f}s  "
            f"{'ok' if r['ok'] else 'WRONG DATA'}"
        )
    return "\n".join(lines)


def findings(rows):
    by = {r["label"]: r for r in rows if not r["error"] and r["ok"]}

    def rate(prefix):
        for label, r in by.items():
            if label.startswith(prefix):
                return r
        return None

    q1 = rate("Queue, 1 message")
    out = []
    if not by:
        return ["No method finished correctly, so there is nothing to compare."]
    ipc = [r for r in by.values() if not r["label"].startswith("No IPC")]
    if ipc:
        best = max(ipc, key=lambda r: r["rate"])
        text = f"Fastest IPC method: {best['label']} at {best['rate']:,.0f} messages/s"
        if q1:
            text += f" ({best['rate'] / q1['rate']:.1f}x the plain Queue)"
        out.append(text + ".")
    for name, one, many in (("Queue", "Queue, 1 message", "Queue, 1000"),
                            ("Pipe", "Pipe, 1 message", "Pipe, 1000"),
                            ("Shared memory", "Shared memory, 1 message", "Shared memory, 1000")):
        a, b = rate(one), rate(many)
        if a and b:
            out.append(f"{name}: batching 1,000 messages changes speed {b['rate'] / a['rate']:.1f}x "
                       f"and median delay from {a['p50']:.2f} ms to {b['p50']:.2f} ms.")
    if ipc:
        quickest = min(ipc, key=lambda r: r["p50"])
        out.append(f"Lowest median delay: {quickest['label']} at {quickest['p50']:.2f} ms "
                   f"(p99 {quickest['p99']:.2f} ms). Batching raises speed but adds waiting time.")
    shm = rate("Shared memory, 1000")
    base = rate("No IPC")
    if shm and base:
        out.append(f"Shared memory reaches {100 * shm['rate'] / base['rate']:.0f}% of the no-IPC ceiling, "
                   f"so most of its time is encoding and decoding, not transport.")
    book = rate("Shared memory ring -> OrderBook")
    if shm and book:
        out.append(f"Feeding the order book drops shared memory from {shm['rate']:,.0f} to "
                   f"{book['rate']:,.0f} messages/s: from here the order book, not IPC, sets the pace.")
    return out


NOTES = """\
How to read the table
  msgs/s     messages delivered per second, from the first send to the last receive
  p50 / p99  delay of a sampled message: time between its creation and its arrival.
             Under load this includes waiting in the queue, so it is a loaded-system delay.
  cpu send / cpu recv   CPU seconds each process used (a process that spins while waiting
             for data uses CPU even when idle)
  check      every message arrived once, in order, and the quantities add up

Copies of one order before it is usable by the receiver
  multiprocessing.Queue   pickle it (copy 1) -> pipe, kernel buffer (copy 2) -> read (copy 3)
                          -> unpickle into new Python objects (copy 4)
  Pipe + struct           pack into bytes (copy 1) -> kernel buffer (copy 2) -> read (copy 3)
                          -> unpack (4)
  Shared-memory ring      pack straight into memory both programs can see (the only write),
                          unpack straight out of it. Nothing is copied between the programs.
"""


def build_report(rows, n):
    lines = [
        f"CHRONOSMATCH - IPC ANALYSIS - Teammate 3 - {time.strftime('%Y-%m-%d')}",
        f"{n:,} messages per method, 2 processes, Python {sys.version.split()[0]}",
        "", table(rows), "", "FINDINGS",
    ]
    lines += [f"  - {text}" for text in findings(rows)]
    lines += ["", NOTES]
    return "\n".join(lines)


def main(argv):
    n = 200_000
    if "--quick" in argv:
        n = 50_000
    if "--messages" in argv:
        n = int(argv[argv.index("--messages") + 1].replace(",", "").replace("_", ""))

    try:
        import orderbook                                   # noqa: F401
        have_book = True
    except ImportError:
        have_book = False

    ctx = mp.get_context("spawn")                          # same behaviour on Windows, Mac, Linux
    plan = [
        ("Queue, 1 message per put", "queue", 1, "count"),
        ("Queue, 1000 messages per put", "queue", BATCH, "count"),
        ("Pipe, 1 message per send (struct)", "pipe", 1, "count"),
        ("Pipe, 1000 messages per send (struct)", "pipe", BATCH, "count"),
        ("Shared memory, 1 message per publish", "shm", 1, "count"),
        ("Shared memory, 1000 messages per publish", "shm", BATCH, "count"),
    ]
    if have_book:
        plan += [
            ("Queue (batches) -> OrderBook", "queue", BATCH, "book"),
            ("Shared memory ring -> OrderBook", "shm", BATCH, "book"),
        ]

    rows = [baseline(n)]
    print(f"{rows[0]['label']}: {rows[0]['rate']:,.0f} msgs/s", flush=True)
    for label, kind, batch, sink in plan:
        print(f"running: {label} ...", flush=True)
        rows.append(run_method(ctx, label, kind, batch, n, sink))

    report = build_report(rows, n)
    print("\n" + report)
    if "--save" in argv:
        with open("ipc_results.txt", "w", encoding="utf-8") as handle:
            handle.write(report + "\n")
        print("saved to ipc_results.txt")
    sys.exit(0 if all(not r["error"] and r["ok"] for r in rows) else 1)


if __name__ == "__main__":
    main(sys.argv[1:])