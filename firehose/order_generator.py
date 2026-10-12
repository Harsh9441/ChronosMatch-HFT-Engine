import random
import time


def generate_order(order_id):
    """
    Generate one random Buy/Sell trading order.
    """

    side = random.choice(["BUY", "SELL"])

    price = round(random.uniform(100.00, 110.00), 2)

    quantity = random.randint(1, 1000)

    timestamp = time.perf_counter_ns()

    order = {
        "order_id": order_id,
        "side": side,
        "price": price,
        "quantity": quantity,
        "timestamp": timestamp
    }

    return order