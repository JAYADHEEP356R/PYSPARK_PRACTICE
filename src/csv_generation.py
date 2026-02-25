import csv
import random
from datetime import datetime, timedelta

num_rows = 200000
start_date = datetime(2022, 1, 1)

products = [
    ("P201", "Laptop", "Electronics", 55000),
    ("P202", "Mobile", "Electronics", 25000),
    ("P203", "Headphones", "Electronics", 1500),
    ("P204", "Shoes", "Fashion", 3000),
    ("P205", "T-Shirt", "Fashion", 800),
    ("P206", "Watch", "Accessories", 4500),
    ("P207", "Backpack", "Fashion", 1200),
    ("P208", "Tablet", "Electronics", 35000),
    ("P209", "Camera", "Electronics", 60000),
    ("P210", "Sunglasses", "Accessories", 2000)
]

regions = ["North", "South", "East", "West", None]
payments = ["Card", "UPI", "Cash", None]

with open("sales_data_dirty.csv", mode="w", newline="") as file:
    writer = csv.writer(file)

    writer.writerow([
        "order_id", "order_date", "customer_id", "product_id",
        "product_name", "category", "quantity",
        "price", "region", "payment_method"
    ])

    for i in range(1, num_rows + 1):
        product = random.choice(products)
        order_date = start_date + timedelta(days=random.randint(0, 1200))

        # Introduce bad data randomly
        quantity = random.randint(1, 5)
        if random.random() < 0.03:  # 3% negative quantity
            quantity = -random.randint(1, 5)

        price = product[3]
        if random.random() < 0.02:  # 2% missing price
            price = None

        order_id = 1000 + i
        if random.random() < 0.02:  # 2% duplicate order_id
            order_id = random.randint(1001, 1100)

        if random.random() < 0.01:  # 1% future dates
            order_date = datetime(2030, 1, 1)

        writer.writerow([
            order_id,
            order_date.strftime("%Y-%m-%d"),
            f"C{random.randint(100, 999)}" if random.random() > 0.02 else None,  # 2% null customer_id
            product[0],
            product[1],
            product[2],
            quantity,
            price,
            random.choice(regions),
            random.choice(payments)
        ])

print("Dirty CSV file generated successfully!")