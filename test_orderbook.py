from orderbook import OrderBook, Order, BUY


def test_book_starts_empty():
    book = OrderBook()
    assert book.count == 0
    assert book.bids == {}
    assert book.asks == {}


def test_order_fields():
    o = Order(1, BUY, 10050, 10)
    assert o.price == 10050
    assert o.qty == 10