"""
Generate sample data for testing all three connectors.
Run once: python sample_data/generate_samples.py
Creates:
  - sample_data/retail.db          (SQLite with intentional DQ issues)
  - sample_data/customers.csv      (CSV with nulls, duplicates)
  - sample_data/transactions.csv   (CSV with outliers)
"""

import sqlite3
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
random.seed(42)
np.random.seed(42)


def generate_sqlite_db():
    """SQLite retail database with realistic DQ issues built in."""
    db_path = os.path.join(OUTPUT_DIR, "retail.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Drop if exists
    for t in ["orders", "products", "employees"]:
        cursor.execute(f"DROP TABLE IF EXISTS {t}")

    # ── ORDERS TABLE (has nulls, duplicates, bad dates) ──
    cursor.execute("""
        CREATE TABLE orders (
            order_id    INTEGER PRIMARY KEY,
            customer_id TEXT,
            product_id  INTEGER,
            quantity    INTEGER,
            unit_price  REAL,
            order_date  TEXT,
            status      TEXT,
            region      TEXT
        )
    """)

    statuses = ["completed", "pending", "cancelled", "refunded"]
    regions = ["North", "South", "East", "West", None]  # Intentional NULLs
    base_date = datetime(2023, 1, 1)

    orders = []
    for i in range(1, 501):
        order_date = (base_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
        orders.append((
            i,
            f"CUST_{random.randint(1, 100):03d}" if random.random() > 0.05 else None,  # 5% null customer
            random.randint(1, 50),
            random.randint(1, 20) if random.random() > 0.03 else None,                 # 3% null quantity
            round(random.uniform(5.0, 500.0), 2) if random.random() > 0.02 else None,  # 2% null price
            order_date if random.random() > 0.04 else None,                            # 4% null date
            random.choice(statuses),
            random.choice(regions),
        ))

    # Add 10 duplicate rows to simulate DQ issue
    for dup in orders[:10]:
        orders.append(dup)

    cursor.executemany("INSERT OR IGNORE INTO orders VALUES (?,?,?,?,?,?,?,?)", orders)

    # ── PRODUCTS TABLE (has schema issues, missing categories) ──
    cursor.execute("""
        CREATE TABLE products (
            product_id   INTEGER PRIMARY KEY,
            product_name TEXT,
            category     TEXT,
            price        REAL,
            stock_qty    INTEGER,
            supplier_id  TEXT,
            last_updated TEXT
        )
    """)

    categories = ["Electronics", "Clothing", "Food", "Books", None, ""]  # NULLs + empty strings
    products = []
    for i in range(1, 51):
        products.append((
            i,
            f"Product_{i:02d}" if random.random() > 0.02 else None,
            random.choice(categories),
            round(random.uniform(1.0, 1000.0), 2),
            random.randint(0, 500),
            f"SUP_{random.randint(1, 10):02d}",
            (base_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
        ))
    cursor.executemany("INSERT INTO products VALUES (?,?,?,?,?,?,?)", products)

    # ── EMPLOYEES TABLE (timeliness test — some very old last_login dates) ──
    cursor.execute("""
        CREATE TABLE employees (
            emp_id      INTEGER PRIMARY KEY,
            name        TEXT,
            department  TEXT,
            salary      REAL,
            hire_date   TEXT,
            last_login  TEXT,
            is_active   INTEGER
        )
    """)

    departments = ["Engineering", "Sales", "HR", "Finance", None]
    employees = []
    for i in range(1, 101):
        hire_date = (base_date - timedelta(days=random.randint(365, 3650))).strftime("%Y-%m-%d")
        # Some employees have very stale last_login (timeliness issue)
        last_login_days_ago = random.choice([1, 7, 30, 365, 1000, 2000])
        last_login = (datetime.now() - timedelta(days=last_login_days_ago)).strftime("%Y-%m-%d")
        employees.append((
            i,
            f"Employee_{i:03d}",
            random.choice(departments),
            round(random.uniform(30000, 150000), 2) if random.random() > 0.03 else None,
            hire_date,
            last_login,
            random.choice([1, 1, 1, 0]),  # 75% active
        ))
    cursor.executemany("INSERT INTO employees VALUES (?,?,?,?,?,?,?)", employees)

    conn.commit()
    conn.close()
    print(f"✅ SQLite DB created: {db_path}")
    print(f"   Tables: orders (510 rows w/dupes), products (50 rows), employees (100 rows)")


def generate_customers_csv():
    """CSV with nulls, duplicates, format inconsistencies."""
    n = 300
    
    emails = [
        f"user{i}@example.com" if random.random() > 0.08 else None for i in range(n)
    ]
    phones = [
        f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"
        if random.random() > 0.12 else None
        for _ in range(n)
    ]
    
    df = pd.DataFrame({
        "customer_id": [f"CUST_{i:04d}" for i in range(1, n + 1)],
        "first_name": [f"First{i}" if random.random() > 0.05 else None for i in range(n)],
        "last_name": [f"Last{i}" for i in range(n)],
        "email": emails,
        "phone": phones,
        "age": [random.randint(18, 80) if random.random() > 0.07 else None for _ in range(n)],
        "city": [random.choice(["New York", "LA", "Chicago", "Houston", None, ""]) for _ in range(n)],
        "signup_date": [
            (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))).strftime("%Y-%m-%d")
            if random.random() > 0.04 else None
            for _ in range(n)
        ],
        "lifetime_value": [
            round(random.uniform(0, 5000), 2) if random.random() > 0.06 else None
            for _ in range(n)
        ],
    })

    # Add 15 duplicate rows
    dupes = df.sample(15, random_state=42)
    df = pd.concat([df, dupes], ignore_index=True)

    out_path = os.path.join(OUTPUT_DIR, "customers.csv")
    df.to_csv(out_path, index=False)
    print(f"✅ CSV created: {out_path} ({len(df)} rows, intentional nulls + duplicates)")


def generate_transactions_csv():
    """CSV with outliers and inconsistent values."""
    n = 500
    amounts = []
    for _ in range(n):
        if random.random() < 0.03:           # 3% extreme outliers
            amounts.append(round(random.uniform(50000, 999999), 2))
        elif random.random() < 0.02:         # 2% negative amounts (data error)
            amounts.append(round(random.uniform(-500, -1), 2))
        else:
            amounts.append(round(random.uniform(1, 2000), 2))

    df = pd.DataFrame({
        "transaction_id": [f"TXN_{i:05d}" for i in range(1, n + 1)],
        "customer_id": [
            f"CUST_{random.randint(1, 300):04d}" if random.random() > 0.04 else None
            for _ in range(n)
        ],
        "amount": amounts,
        "currency": [random.choice(["USD", "EUR", "GBP", "USD", "USD", None]) for _ in range(n)],
        "transaction_date": [
            (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
            for _ in range(n)
        ],
        "merchant": [f"Merchant_{random.randint(1, 50)}" for _ in range(n)],
        "category": [
            random.choice(["Food", "Travel", "Shopping", "Entertainment", None])
            for _ in range(n)
        ],
        "is_flagged": [1 if abs(a) > 10000 else 0 for a in amounts],
    })

    out_path = os.path.join(OUTPUT_DIR, "transactions.csv")
    df.to_csv(out_path, index=False)
    print(f"✅ CSV created: {out_path} ({len(df)} rows, intentional outliers + negatives)")


if __name__ == "__main__":
    print("🔧 Generating sample data...\n")
    generate_sqlite_db()
    generate_customers_csv()
    generate_transactions_csv()
    print("\n✅ All sample data generated! Run test_connectors.py to verify.")