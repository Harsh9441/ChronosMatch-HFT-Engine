import curses
import time

def main(stdscr):
    curses.curs_set(0)  # Hide cursor
    stdscr.nodelay(True)
    stdscr.timeout(500)

    while True:
        stdscr.clear()
        stdscr.addstr(0, 0, "ChronosMatch Dashboard")
        stdscr.addstr(2, 0, "Orders received: 0")
        stdscr.addstr(3, 0, "Buffer usage: 0%")
        stdscr.addstr(4, 0, "Latency: TBD")

        stdscr.refresh()
        time.sleep(1)

curses.wrapper(main)
