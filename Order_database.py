"""ChronosMatch - database layer (Teammate 3, Day 16)

The database layer now lives inside orderbook.py (Day 16). This file only
re-exports it, so `import orderdb` and `python orderdb.py --demo` still work.
"""

import sys

from orderbook import (  # noqa: F401
    SCHEMA,
    OrderDatabase,
    PersistentBook,
    format_demo_report,
    reference_match,
    run_database_demo,
)

if __name__ == "__main__":
    if "--demo" in sys.argv:
        n = 50_000
        if "--orders" in sys.argv:
            n = int(sys.argv[sys.argv.index("--orders") + 1].replace(",", ""))
        db_path = sys.argv[sys.argv.index("--db") + 1] if "--db" in sys.argv else None
        report = run_database_demo(n, db_path)
        print(format_demo_report(report))
        sys.exit(0 if report["ok"] else 1)
    print(__doc__)