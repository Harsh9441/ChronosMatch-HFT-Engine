from order_generator import generate_order


for i in range(1, 11):
    order = generate_order(i)
    print(order)