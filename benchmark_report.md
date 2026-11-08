# ChronosMatch Benchmark Report (Day 16)

Teammate 3 - Days 1 to 16 - 2026-11-08 11:04

Machine: Windows 11, 12 logical CPUs, Python 3.14.3 (CPython). Numbers depend on this computer and vary a little from run to run.

## Summary

- Correctness: **all checks passed** (28 load-test checks and 4 zero-copy checks).
- Transport between processes (shared memory) is zero-copy: **True**.
- The order book itself is zero-copy: **False** (it stores Python objects, so it copies by design).

## Day 13: Load test

| Step | 50,000 orders | 100,000 orders |
|---|---:|---:|
| Encode to bytes | 434,920 /s (115 ms) | 403,264 /s (248 ms) |
| Decode from bytes | 454,153 /s (110 ms) | 532,104 /s (188 ms) |
| Ingest from dicts | 277,084 /s (180 ms) | 278,102 /s (360 ms) |
| Ingest from bytes (decode + insert) | 196,338 /s (255 ms) | 173,486 /s (576 ms) |
| Look up every order by ID | 12,938,954 /s (4 ms) | 6,932,986 /s (14 ms) |
| Cancel orders | 1,195,986 /s (8 ms) | 833,667 /s (24 ms) |

Each run encodes the orders, decodes them, builds one book from dictionaries and one from bytes, compares the two, looks up every order, cancels 20% of them, and re-checks the book. Checks:

- PASS: decoded equals input
- PASS: encoded path matches direct path
- PASS: all orders stored
- PASS: no duplicate ids
- PASS: arrival order kept
- PASS: sides add up
- PASS: every id found
- PASS: best prices correct before cancel
- PASS: cancels succeeded
- PASS: cancelled orders gone
- PASS: count after cancel
- PASS: arrival order after cancel
- PASS: best prices correct after cancel
- PASS: cancelling again returns false

## Day 14: Zero-copy verification

| Check | Result | Evidence |
|---|---|---|
| memoryview slice is zero-copy | PASS | slice of 899,000 bytes used 184 extra bytes and saw a later write |
| bytes(view[...]) makes an independent copy | PASS | the copy kept the old value after the source changed |
| two processes share one memory block | PASS | parent read 123456789 written by the child; nothing was pickled or sent through a pipe |
| order book copies orders out of shared memory | PASS | 46,894 shared bytes became 570,242 bytes of Python objects (12.2x); orders survived the block being wiped |

## Day 16: Database layer

50,000 orders went into the order book and a SQLite database, were matched (25,041 trades, 6,274,700 units), and the book was rebuilt from the database as after a restart.

| Step | Time | Orders/s |
|---|---:|---:|
| Submit (book + database) | 0.546 s | 91,506 |
| Match + store trades | 2.005 s | 24,938 |
| Restore after restart | 0.091 s | 272,846 |

- PASS: all orders stored
- PASS: trades stored
- PASS: matching again finds nothing
- PASS: database is consistent
- PASS: restored book equals live book
- PASS: restored database is consistent

## Day 11: IPC methods compared

```
CHRONOSMATCH - IPC ANALYSIS - Teammate 3 - 2026-11-08
50,000 messages per method, 2 processes, Python 3.14.3

method                                                 msgs/s  time s   p50 ms   p99 ms cpu send cpu recv  check
----------------------------------------------------------------------------------------------------------------
No IPC: pack + unpack in one process (ceiling)        804,135    0.06     0.00     0.00    0.05s    0.00s  ok
Queue, 1 message per put                               83,897    0.60   286.68   516.73    0.06s    0.50s  ok
Queue, 1000 messages per put                        1,298,428    0.04    19.69    23.68    0.02s    0.03s  ok
Pipe, 1 message per send (struct)                      92,134    0.54    20.11    31.01    0.48s    0.55s  ok
Pipe, 1000 messages per send (struct)               1,413,643    0.04     1.40     2.36    0.05s    0.05s  ok
Shared memory, 1 message per publish                  674,448    0.07     0.00     0.03    0.08s    0.09s  ok
Shared memory, 1000 messages per publish              963,950    0.05     1.31     2.14    0.05s    0.06s  ok
Queue (batches) -> OrderBook                          207,692    0.24   103.62   223.61    0.02s    0.23s  ok
Shared memory ring -> OrderBook                       220,254    0.23    21.03   110.33    0.05s    0.22s  ok

FINDINGS
  - Fastest IPC method: Pipe, 1000 messages per send (struct) at 1,413,643 messages/s (16.8x the plain Queue).
  - Queue: batching 1,000 messages changes speed 15.5x and median delay from 286.68 ms to 19.69 ms.
  - Pipe: batching 1,000 messages changes speed 15.3x and median delay from 20.11 ms to 1.40 ms.
  - Shared memory: batching 1,000 messages changes speed 1.4x and median delay from 0.00 ms to 1.31 ms.
  - Lowest median delay: Shared memory, 1 message per publish at 0.00 ms (p99 0.03 ms). Batching raises speed but adds waiting time.
  - Shared memory reaches 120% of the no-IPC ceiling, so most of its time is encoding and decoding, not transport.
  - Feeding the order book drops shared memory from 963,950 to 220,254 messages/s: from here the order book, not IPC, sets the pace.

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
```

## Findings

- At 100,000 orders the slowest stage is **ingest** at 278,102 orders/s.
- Ingesting from bytes takes 1.60x as long as ingesting from dictionaries, because the bytes must be decoded first. Decoding alone is about 33% of that time.
- Going from 50,000 to 100,000 orders, ingest time grew 1.00x relative to the number of orders (1.00 means perfectly linear).
- The wire format is compact: 24.9 bytes per order.
- Decoded orders take about 12x more memory as Python objects than the bytes they came from. This is the copy cost between shared memory and the order book.

## Days 15 and 16: Final summary

### Work completed, Days 1 to 16

| Day | Work |
|---|---|
| 1 | Basic order-book structure with separate buy and sell storage |
| 2 | Insert buy and sell orders with validation |
| 3 | Retrieve orders and look up by ID |
| 4 | Cancel orders; best bid and best ask |
| 5 | Firehose event ingestion |
| 6 | Async firehose ingestion |
| 7 | Insertion testing: duplicates, bad values, 50,000 inserts |
| 8 | Trade validation |
| 9 | Heap-based best bid/ask and stress tests |
| 10 | Arrival-order retrieval and full integration tests |
| 11 | IPC analysis: Queue vs Pipe vs shared memory; binary encode/decode |
| 12 | Batch IPC transmission and ingestion |
| 13 | Load test with 50,000+ orders |
| 14 | Zero-copy architecture verification |
| 15 | Benchmark report, acceptance checklist and final review |
| 16 | Database layer: SQLite storage for orders, trades and fills, connected to the matching engine |

### Acceptance checklist

| Requirement | Result |
|---|---|
| 100,000+ orders pass every load-test check | PASS |
| Cancels keep the book, arrival order and best prices correct | PASS |
| Binary ingest builds the same book as dictionary ingest | PASS |
| Shared-memory transport verified zero-copy | PASS |
| Copy cost at the order book measured and documented | PASS |
| IPC methods compared (Day 11 results included) | PASS |
| Database layer stores, matches and restores correctly (Day 16) | PASS |
| Full test suite passes | NOT RUN (use --tests) |

### Recommendations

- Speed up ingest first: it is the slowest stage, so fewer checks and fewer temporary objects per order give the biggest gain.
- Add an ingest path that takes decoded tuples directly and skips the dictionary step; keep the checked path and test that both build identical books.
- Connect the real matching engine through PersistentBook.run_engine and keep database writes batched (one transaction per batch); per-order commits are much slower.
- Use small batches (tens to a few hundred messages) for a trading engine; large batches raise throughput but add waiting time.
- Rerun this report on a multi-core machine before quoting shared memory against Pipe, because the ranking can change.
- For ARM computers, add memory barriers around the ring-buffer counters.


## What this means

- Moving data between processes through shared memory copies nothing: a value written by one process appears in the other with no pickling and no pipe.
- The order book is where copying happens: it turns bytes into Python objects. A truly zero-copy book would keep fixed-size records in shared memory instead of Python objects, which is a redesign.
- The next speed gain is in the ingest path (fewer checks and fewer temporary objects per order), not in the transport.

## Limits of these numbers

- One run per size on one machine; repeat and use the median.
- Other programs running at the same time slow the numbers down.
- The load test uses random valid orders; real market data differs.
- On x86/x64 the shared-memory counters are safe without memory barriers; ARM would need them.
