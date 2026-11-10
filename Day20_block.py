# ============================================================
# DAY 20: END-TO-END VALIDATION
# ============================================================

# One run that pushes real data through the WHOLE system and checks the result
# against an independent model:
#
#   binary encode -> real Pipe -> decode -> damaged/repeated events mixed in ->
#   book + SQLite -> cancels (incl. a partly filled order) -> matching engine ->
#   stored trades -> restart in the middle -> more rounds -> restart at the end ->
#   stand-alone file check -> backup -> rebuild from backup -> tamper detection
#
# The independent model (_EndToEndOracle) shares no code with OrderBook,
# reference_match or the database. After every round the trades the system
# stored must equal the trades the model computes, and at the end every order's
# status and fill must be identical in both. Any other matching engine can be
# plugged in through the engine argument, so this also validates a real engine.

class _EndToEndOracle:
    """Plain dicts and heaps: an independent model of orders, cancels, fills."""

    def __init__(self) -> None:
        # order_id -> [side, price, quantity, filled, arrival, status]
        self.orders: dict[str, list] = {}
        self._arrival = count()

    def add(self, event: Mapping) -> None:
        self.orders[event["order_id"]] = [
            event["side"], float(event["price"]), event["quantity"],
            0, next(self._arrival), "open"
        ]

    def cancel(self, order_id: str) -> bool:
        row = self.orders.get(order_id)

        if row is None or row[5] not in ("open", "partial"):
            return False

        row[5] = "cancelled"
        return True

    def partial_ids(self) -> list[str]:
        """Partly filled orders, oldest first."""
        return [
            order_id for _, order_id in sorted(
                (row[4], order_id) for order_id, row in self.orders.items()
                if row[5] == "partial"
            )
        ]

    def open_ids(self) -> set[str]:
        return {order_id for order_id, row in self.orders.items()
                if row[5] in ("open", "partial")}

    def best(self, side: str):
        prices = [row[1] for row in self.orders.values()
                  if row[0] == side and row[5] in ("open", "partial")]

        if not prices:
            return None

        return max(prices) if side == BUY else min(prices)

    def states(self) -> dict[str, tuple]:
        return {order_id: (row[5], row[3])
                for order_id, row in self.orders.items()}

    def match(self) -> list[tuple]:
        """Price-time matching with two heaps. Returns
        (buy_id, sell_id, price, quantity) and applies the fills.

        Best price first, earliest arrival first; each trade is priced at the
        order that arrived first. This is the rule reference_match uses, built
        a different way.
        """

        buys: list[tuple] = []
        sells: list[tuple] = []

        for order_id, row in self.orders.items():
            if row[5] not in ("open", "partial"):
                continue

            if row[0] == BUY:
                buys.append((-row[1], row[4], order_id))
            else:
                sells.append((row[1], row[4], order_id))

        heapq.heapify(buys)
        heapq.heapify(sells)
        trades: list[tuple] = []

        while buys and sells:
            neg_bid, buy_arrival, buy_id = buys[0]
            ask, sell_arrival, sell_id = sells[0]

            if -neg_bid < ask:
                break

            buy, sell = self.orders[buy_id], self.orders[sell_id]
            quantity = min(buy[2] - buy[3], sell[2] - sell[3])
            price = -neg_bid if buy_arrival < sell_arrival else ask
            trades.append((buy_id, sell_id, price, quantity))

            for row in (buy, sell):
                row[3] += quantity
                row[5] = "filled" if row[3] == row[2] else "partial"

            if buy[5] == "filled":
                heapq.heappop(buys)
            if sell[5] == "filled":
                heapq.heappop(sells)

        return trades


def _e2e_damaged_events(k: int) -> list:
    """Five different kinds of unusable events for round k."""

    base = {"order_id": f"E{k}", "side": BUY, "price": 100.0, "quantity": 5}

    return [
        {"order_id": f"E{k}-a", "side": BUY, "quantity": 5},       # no price
        {**base, "order_id": f"E{k}-b", "price": -1.0},            # negative
        {**base, "order_id": f"E{k}-c", "quantity": 0},            # zero qty
        {**base, "order_id": f"E{k}-d", "side": "X"},              # bad side
        None,                                                      # not a dict
    ]


def _e2e_transfer(payload: bytes, send_conn, recv_conn,
                  timeout: float = 30.0) -> tuple[list[dict], int]:
    """Send one binary batch through a real Pipe and decode what arrives.

    A helper thread sends while this thread receives, so a batch bigger than
    the pipe's buffer cannot deadlock.
    """

    box: dict = {}

    def sender() -> None:
        try:
            send_conn.send_bytes(payload)
        except Exception as exc:                          # noqa: BLE001
            box["error"] = exc

    thread = threading.Thread(target=sender, daemon=True)
    thread.start()

    try:
        if not recv_conn.poll(timeout):
            raise TimeoutError("the IPC batch never arrived")
        received = recv_conn.recv_bytes()
    finally:
        thread.join(timeout)

    if "error" in box:
        raise box["error"]

    return decode_events(received), len(received)


def run_end_to_end_validation(
    n_orders: int = 20_000,
    path: str | None = None,
    rounds: int = 10,
    engine=reference_match,
    seed: int = 20
) -> dict:
    """Run the whole system end to end and compare it with the independent
    model. Returns timings per stage and a dict of pass/fail checks.

    path:   database file to create (must not exist); a temp file by default.
    engine: any function engine(book, filled, first_trade_number) -> trades.
    """

    import multiprocessing as mp
    import shutil

    if n_orders < 1 or rounds < 1:
        raise ValueError("n_orders and rounds must be at least 1")

    if path is not None and os.path.exists(path):
        raise FileExistsError(f"{path} already exists; pick a new file name")

    events = make_load_events(n_orders, seed)
    size = -(-n_orders // rounds)
    batches = [events[i:i + size] for i in range(0, n_orders, size)]
    middle = len(batches) // 2

    folder = tempfile.TemporaryDirectory()
    if path is None:
        path = os.path.join(folder.name, "e2e.db")
    backup_path = os.path.join(folder.name, "e2e-backup.db")
    tampered_path = os.path.join(folder.name, "e2e-tampered.db")

    stage = {"ipc": 0.0, "submit": 0.0, "cancel": 0.0, "match": 0.0,
             "restart": 0.0, "validate": 0.0}
    flags: dict[str, bool] = {}

    def timed(name: str, fn):
        start = time.perf_counter()
        value = fn()
        stage[name] += time.perf_counter() - start
        return value

    def verdict(name: str, passed) -> None:
        flags[name] = flags.get(name, True) and bool(passed)

    def restart(old: "PersistentBook"):
        """Close the database, reopen it, rebuild the book, compare."""

        live_orders = old.book.get_orders_in_arrival_order()
        live_filled = old.db.filled_map()
        old.db.close()
        fresh = PersistentBook.restore(OrderDatabase(path))
        same = (fresh.book.get_orders_in_arrival_order() == live_orders
                and fresh.db.filled_map() == live_filled)
        return fresh, same

    recv_conn, send_conn = mp.Pipe(duplex=False)
    pb = PersistentBook(OrderDatabase(path))
    oracle = _EndToEndOracle()
    bare = OrderBook()                    # fed only through ingest_encoded
    sent_total = bad_total = payload_bytes = 0
    engine_errors: list[str] = []

    try:
        for k, batch in enumerate(batches):

            if k == middle and k > 0:
                pb, same = timed("restart", lambda: restart(pb))
                verdict("a restart rebuilds exactly the same book and fills",
                        same)

            # 1. binary encode -> real pipe -> decode
            payload = encode_events(batch)
            decoded, nbytes = timed(
                "ipc", lambda: _e2e_transfer(payload, send_conn, recv_conn))
            payload_bytes += nbytes
            verdict("every batch survives the pipe unchanged", decoded == batch)
            bare.ingest_encoded(payload)

            # 2. mix in damaged events and repeats of the previous batch
            junk = _e2e_damaged_events(k)
            replay = batches[k - 1][:3] if k > 0 else []
            half = len(decoded) // 2
            mixed = decoded[:half] + junk + replay + decoded[half:]
            sent_total += len(mixed)
            bad_total += len(junk) + len(replay)

            # 3. book + database
            result = timed("submit", lambda: pb.submit_tolerant(mixed))
            verdict("every valid order is stored",
                    result["accepted"] == len(decoded))
            verdict("every damaged or repeated event is quarantined",
                    result["rejected"] == len(junk) + len(replay))

            for event in decoded:
                oracle.add(event)

            # 4. cancels: some new orders and one older partly filled order
            def cancel_some():
                ids = [e["order_id"] for i, e in enumerate(decoded)
                       if i % 10 == 5]
                ids += oracle.partial_ids()[:1]

                for order_id in ids:
                    got, want = pb.cancel(order_id), oracle.cancel(order_id)
                    verdict("cancels agree with the independent model",
                            got and want)

                if ids:
                    verdict("cancelling twice is refused", not (
                        pb.cancel(ids[0]) or oracle.cancel(ids[0])))

                verdict("cancelling an unknown order is refused",
                        not pb.cancel("NO-SUCH-ORDER"))

            timed("cancel", cancel_some)

            # 5. matching engine, then compare with the model
            def match():
                last = pb.db.conn.execute(
                    "SELECT COALESCE(MAX(seq), 0) FROM trades"
                ).fetchone()[0]

                try:
                    pb.run_engine(engine, tolerant=True)
                except Exception as exc:                  # noqa: BLE001
                    engine_errors.append(f"round {k + 1}: {exc}")

                return [tuple(row) for row in pb.db.conn.execute(
                    "SELECT buy_order_id, sell_order_id, price, quantity "
                    "FROM trades WHERE seq > ? ORDER BY seq", (last,))]

            stored = timed("match", match)
            verdict("every round's stored trades equal the independent "
                    "matcher's", stored == oracle.match())

            bid, ask = pb.book.best_bid(), pb.book.best_ask()
            verdict("the book is not crossed after matching",
                    bid is None or ask is None or bid.price < ask.price)

        # 6. restart once more and carry on with the rebuilt book
        pb, same = timed("restart", lambda: restart(pb))
        verdict("a restart rebuilds exactly the same book and fills", same)

        # 7. compare everything with the independent model
        final = timed("validate", lambda: validate_trade_persistence(pb.db))
        verdict("stored data passes every persistence check", final["ok"])
        stats = pb.db.stats()

        db_state = {
            order_id: (status, filled) for order_id, status, filled in
            pb.db.conn.execute("SELECT order_id, status, filled FROM orders")
        }
        verdict("every order's status and fill equal the independent model",
                db_state == oracle.states())

        quarantined = pb.db.conn.execute(
            "SELECT COUNT(*) FROM rejected WHERE kind = 'order'"
        ).fetchone()[0]
        verdict("nothing is lost: stored + quarantined = everything sent",
                stats["orders"] + quarantined == sent_total
                and stats["orders"] == n_orders
                and quarantined == bad_total)

        book_orders = pb.book.get_orders_in_arrival_order()
        verdict("the book holds exactly the open orders",
                {o.order_id for o in book_orders} == oracle.open_ids())
        verdict("book and database agree",
                pb.reconcile()["consistent_before"])

        bid, ask = pb.book.best_bid(), pb.book.best_ask()
        verdict("best bid and ask equal the independent model",
                (bid.price if bid else None) == oracle.best(BUY)
                and (ask.price if ask else None) == oracle.best(SELL)
                and _best_prices_match(pb.book))

        verdict("the binary path builds the same orders as the dictionaries",
                [(o.order_id, o.side, o.price, o.quantity)
                 for o in bare.get_orders_in_arrival_order()]
                == [(e["order_id"], e["side"], e["price"], e["quantity"])
                    for e in events])

        # 8. repeating work must change nothing
        before = _db_dump(pb.db)
        again = pb.run_engine(engine, tolerant=True)
        replayed = pb.submit_tolerant(batches[-1], quarantine=False)
        retried = pb.retry_rejected()
        verdict("running the engine again finds nothing", again == 0)
        verdict("delivering the last batch again stores nothing",
                replayed["accepted"] == 0)
        verdict("retrying quarantined records resolves nothing",
                retried["orders_resolved"] == 0
                and retried["trades_resolved"] == 0)
        verdict("repeating work leaves the database unchanged",
                _db_dump(pb.db) == before)

        verdict("the matching engine ran without errors", not engine_errors)
        verdict("trades were stored", stats["trades"] > 0 or n_orders < 100)

        # 9. the saved file, a backup, and a planted fault
        final_orders = book_orders
        pb.db.close()

        verdict("the saved file passes the stand-alone checker",
                validate_database_file(path)["ok"])

        backup_database(path, backup_path)
        verdict("a backup passes the stand-alone checker",
                validate_database_file(backup_path)["ok"])

        pb = PersistentBook.restore(OrderDatabase(backup_path))
        verdict("a book rebuilt from the backup equals the live book",
                pb.book.get_orders_in_arrival_order() == final_orders)
        pb.db.close()

        shutil.copy2(backup_path, tampered_path)
        raw = sqlite3.connect(tampered_path)       # foreign keys off: allowed
        try:
            with raw:
                raw.execute(
                    "INSERT INTO trades (trade_id, buy_order_id, "
                    "sell_order_id, price, quantity) "
                    "VALUES ('TAMPER', 'ghost-buy', 'ghost-sell', 1.0, 1)")
        finally:
            raw.close()
        verdict("the checker catches a planted bad trade",
                not validate_database_file(tampered_path)["ok"])

        total_s = max(sum(stage.values()), 1e-9)

        return {
            "orders": n_orders, "rounds": len(batches), "sent": sent_total,
            "damaged": bad_total, "trades": stats["trades"],
            "payload_bytes": payload_bytes, "stages": dict(stage),
            "total_s": total_s, "rate": n_orders / total_s,
            "checks": dict(flags),
            "problems": final["problems"] + engine_errors,
            "ok": all(flags.values()),
        }
    finally:
        recv_conn.close()
        send_conn.close()
        try:
            pb.db.close()
        except sqlite3.Error:
            pass
        folder.cleanup()


def format_end_to_end_report(r: dict) -> str:
    labels = {
        "ipc": "pipe transfer + decode",
        "submit": "submit (book + database)",
        "cancel": "cancels",
        "match": "matching + storing trades",
        "restart": "restarts + rebuild",
        "validate": "full validation",
    }
    lines = [
        f"END-TO-END VALIDATION: {r['orders']:,} orders in {r['rounds']} "
        f"rounds ({r['sent']:,} events sent, {r['damaged']:,} damaged or "
        f"repeated), {r['trades']:,} trades",
        f"  {r['payload_bytes']:,} bytes travelled through a real pipe",
    ]
    lines += [f"  {labels[name]:<28}{seconds:8.3f} s"
              for name, seconds in r["stages"].items()]
    lines.append(f"  {'total':<28}{r['total_s']:8.3f} s  "
                 f"{r['rate']:>10,.0f} orders/s end to end")
    lines.append("  checks:")
    lines += [f"    {'ok  ' if ok else 'FAIL'} {name}"
              for name, ok in r["checks"].items()]
    lines += [f"    problem: {p}" for p in r["problems"]]
    lines.append("  RESULT: " + ("ALL CHECKS PASSED" if r["ok"] else "FAILED"))
    return "\n".join(lines)