import asyncio
import time

from order_generator import generate_order


async def firehose(order_rate=1000, duration=5):
    """
    Generate trading orders asynchronously and measure throughput.

    Parameters:
        order_rate: Target number of orders to generate per second.
        duration: Duration of the benchmark in seconds.
    """

    total_orders = order_rate * duration

    print("\n========================================")
    print("       CHRONOSMATCH MARKET FIREHOSE")
    print("========================================")
    print(f"Target rate  : {order_rate:,} orders/sec")
    print(f"Duration     : {duration} seconds")
    print(f"Target orders: {total_orders:,}")
    print("----------------------------------------")

    start_ns = time.perf_counter_ns()

    generated_orders = 0

    for order_id in range(1, total_orders + 1):

        # Generate one order.
        order = generate_order(order_id)

        # The order will be sent to the mmap buffer later.
        # For now, we simply generate it.

        generated_orders += 1

        # Give the asyncio event loop a chance to run.
        await asyncio.sleep(0)

    end_ns = time.perf_counter_ns()

    elapsed_ns = end_ns - start_ns
    elapsed_seconds = elapsed_ns / 1_000_000_000

    actual_rate = generated_orders / elapsed_seconds

    print("----------------------------------------")
    print(f"Orders generated : {generated_orders:,}")
    print(f"Time taken       : {elapsed_seconds:.6f} seconds")
    print(f"Actual throughput: {actual_rate:,.2f} orders/sec")
    print("========================================\n")


async def main():

    # Day 1 benchmark
    await firehose(
        order_rate=1000,
        duration=5
    )


if __name__ == "__main__":
    asyncio.run(main())