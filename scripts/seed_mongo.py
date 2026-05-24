#!/usr/bin/env python3
"""Seed MongoDB with sample data for testing the Data Control Plane.

Usage:
    python scripts/seed_mongo.py

Or via Docker:
    docker compose exec dagster-user-code python /opt/dagster/app/scripts/seed_mongo.py
"""

import os
import random
from datetime import datetime, timedelta

from pymongo import MongoClient

MONGODB_URI = os.environ.get(
    "MONGODB_URI",
    "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true",
)
DATABASE = os.environ.get("MONGODB_DATABASE", "app_db")

# ─── Sample Data ───

FIRST_NAMES = [
    "Carlos", "María", "Juan", "Ana", "Pedro", "Lucía", "Miguel", "Sofia",
    "David", "Elena", "Andrés", "Carmen", "Pablo", "Laura", "Jorge",
    "Marta", "Diego", "Isabel", "Raúl", "Teresa",
]
LAST_NAMES = [
    "García", "Rodríguez", "Martínez", "López", "González", "Hernández",
    "Pérez", "Sánchez", "Ramírez", "Torres", "Flores", "Rivera",
    "Gómez", "Díaz", "Cruz", "Morales",
]
COUNTRIES = ["ES", "MX", "AR", "CO", "CL", "PE", "US", "DE", "FR", "UK"]
STATUSES = ["active", "inactive", "suspended"]
PRODUCTS = [
    "Laptop Pro 15", "Wireless Mouse", "Mechanical Keyboard", "4K Monitor",
    "USB-C Hub", "External SSD 1TB", "Webcam HD", "Noise-Cancelling Headphones",
    "Ergonomic Chair", "Standing Desk", "Tablet 10inch", "Smartphone X",
    "Smartwatch V3", "Bluetooth Speaker", "Power Bank 20000mAh",
]
ORDER_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]


def seed_users(db, count: int = 200):
    """Insert sample user documents."""
    collection = db["users"]

    # Clear existing
    collection.delete_many({})

    users = []
    for i in range(count):
        name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        email = name.lower().replace(" ", ".").replace("á", "a").replace("é", "e") \
            .replace("í", "i").replace("ó", "o").replace("ú", "u") + f"@example.com"
        user = {
            "name": name,
            "email": email,
            "age": random.randint(18, 65),
            "country": random.choice(COUNTRIES),
            "created_at": datetime.now() - timedelta(days=random.randint(1, 730)),
            "status": random.choice(STATUSES),
        }
        users.append(user)

    result = collection.insert_many(users)
    print(f"✅ Inserted {len(result.inserted_ids)} users into {DATABASE}.users")
    return result.inserted_ids


def seed_orders(db, user_ids: list, count: int = 500):
    """Insert sample order documents."""
    collection = db["orders"]

    # Clear existing
    collection.delete_many({})

    orders = []
    for i in range(count):
        product = random.choice(PRODUCTS)
        quantity = random.randint(1, 5)
        price = round(random.uniform(9.99, 1499.99), 2)
        total = round(price * quantity, 2)

        order = {
            "user_id": str(random.choice(user_ids)),
            "product": product,
            "quantity": quantity,
            "price": price,
            "total": total,
            "order_date": datetime.now() - timedelta(
                days=random.randint(0, 365),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            ),
            "status": random.choice(ORDER_STATUSES),
        }
        orders.append(order)

    result = collection.insert_many(orders)
    print(f"✅ Inserted {len(result.inserted_ids)} orders into {DATABASE}.orders")


def main():
    print(f"🔗 Connecting to MongoDB: {MONGODB_URI}")
    client = MongoClient(MONGODB_URI)
    db = client[DATABASE]

    # Verify connection
    try:
        client.admin.command("ping")
        print("✅ MongoDB connection successful")
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        return

    # Seed data
    print(f"\n📊 Seeding database: {DATABASE}")
    user_ids = seed_users(db)
    seed_orders(db, user_ids)

    # Summary
    print(f"\n📋 Summary:")
    for coll_name in db.list_collection_names():
        count = db[coll_name].count_documents({})
        print(f"   {DATABASE}.{coll_name}: {count} documents")

    client.close()
    print("\n🎉 Seed complete!")


if __name__ == "__main__":
    main()
