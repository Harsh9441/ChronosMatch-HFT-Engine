import struct

# Binary IPC event format
# order_id: unsigned 64-bit
# side: 1 byte (B or S)
# price: unsigned 64-bit
# quantity: unsigned 64-bit

_EVENT = struct.Struct("!QBQQ")


def encode_events(events):
    """Convert order events into binary data."""
    payload = bytearray()

    for event in events:
        if isinstance(event, dict):
            order_id = event["order_id"]
            side = event["side"]
            price = event["price"]
            quantity = event["quantity"]
        else:
            order_id = event.order_id
            side = event.side
            price = event.price
            quantity = event.quantity

        if side not in (BUY, SELL):
            raise ValueError("Side must be BUY or SELL")

        if min(order_id, price, quantity) < 0:
            raise ValueError("Order values cannot be negative")

        payload.extend(
            _EVENT.pack(
                int(order_id),
                ord(side),
                int(price),
                int(quantity)
            )
        )

    return bytes(payload)


def decode_events(payload):
    """Convert binary data back into order events."""
    if len(payload) % _EVENT.size != 0:
        raise ValueError("Invalid IPC payload length")

    events = []

    for offset in range(0, len(payload), _EVENT.size):
        order_id, side_byte, price, quantity = _EVENT.unpack_from(
            payload, offset
        )

        side = chr(side_byte)

        if side not in (BUY, SELL):
            raise ValueError("Invalid order side in IPC payload")

        events.append({
            "order_id": order_id,
            "side": side,
            "price": price,
            "quantity": quantity
        })

    return events


def send_events(connection, events):
    """Encode and send a batch of events through a multiprocessing connection."""
    payload = encode_events(events)
    connection.send_bytes(payload)
    return len(events)git status
