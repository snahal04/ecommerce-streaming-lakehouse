import random
import json
import copy

dbutils.widgets.text("topic_name", "orders")
topic = dbutils.widgets.get("topic_name").lower()
print(f"topic = {topic}")

if topic != "orders":
    raise Exception(f"topic = {topic} is not valid. Must be 'orders'.")

regions = ["US", "UK", "IN", "AU", "CA"]
valid_products = [f"P{i:03d}" for i in range(1, 81)]

data = []
generated_orders = []

for i in range(1, 31):  # Total records = 30

    # 20% chance to generate a duplicate
    if generated_orders and random.random() < 0.20:

        # Pick a previously generated order
        order = copy.deepcopy(random.choice(generated_orders))

    else:
        # Generate a new order
        order = {
            "order_id": str(len(generated_orders) + 1),
            "cust_id": str(random.randint(1000, 9999)),
            "region": random.choice(regions),
            "product_id": random.choice(valid_products),
            "amount": str(random.randint(100, 10000)),
            "quantity": str(random.randint(1, 10))
        }

        # 40% records should have quality issues
        if random.random() < 0.40:

            issue = random.choice([
                "null_product",
                "empty_product",
                "null_amount",
                "invalid_amount",
                "invalid_quantity",
                "null_region",
                "negative_amount"
            ])

            if issue == "null_product":
                order["product_id"] = None

            elif issue == "empty_product":
                order["product_id"] = ""

            elif issue == "null_amount":
                order["amount"] = None

            elif issue == "invalid_amount":
                order["amount"] = "ABC"

            elif issue == "invalid_quantity":
                order["quantity"] = "XYZ"

            elif issue == "null_region":
                order["region"] = None

            elif issue == "negative_amount":
                order["amount"] = str(-random.randint(1, 1000))

        generated_orders.append(copy.deepcopy(order))

    data.append(order)

print(f"Total records generated: {len(data)}")
print(f"Unique order_ids: {len(set(x['order_id'] for x in data))}")


dbutils.jobs.taskValues.set(
    key="orders_data",
    value=data
)