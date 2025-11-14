import csv
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = BASE_DIR / "ecom.db"

SCHEMAS = {
    "customers": """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            city TEXT,
            state TEXT,
            signup_date TEXT,
            loyalty_tier TEXT
        )
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT,
            price REAL,
            cost REAL,
            currency TEXT,
            stock_status TEXT
        )
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_date TEXT,
            order_status TEXT,
            payment_method TEXT,
            order_total REAL,
            ship_city TEXT,
            ship_state TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """,
    "order_items": """
        CREATE TABLE IF NOT EXISTS order_items (
            order_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            quantity INTEGER,
            item_price REAL,
            item_discount REAL,
            PRIMARY KEY (order_id, product_id),
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """,
    "reviews": """
        CREATE TABLE IF NOT EXISTS reviews (
            review_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            customer_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            rating INTEGER,
            review_text TEXT,
            review_date TEXT,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """,
}

LOAD_SEQUENCE = ["customers", "products", "orders", "order_items", "reviews"]

TABLE_CONFIGS = {
    "customers": {
        "file": "customers.csv",
        "columns": [
            "customer_id",
            "name",
            "email",
            "city",
            "state",
            "signup_date",
            "loyalty_tier",
        ],
        "float_fields": [],
        "int_fields": [],
    },
    "products": {
        "file": "products.csv",
        "columns": [
            "product_id",
            "product_name",
            "category",
            "price",
            "cost",
            "currency",
            "stock_status",
        ],
        "float_fields": ["price", "cost"],
        "int_fields": [],
    },
    "orders": {
        "file": "orders.csv",
        "columns": [
            "order_id",
            "customer_id",
            "order_date",
            "order_status",
            "payment_method",
            "order_total",
            "ship_city",
            "ship_state",
        ],
        "float_fields": ["order_total"],
        "int_fields": [],
    },
    "order_items": {
        "file": "order_items.csv",
        "columns": [
            "order_id",
            "product_id",
            "quantity",
            "item_price",
            "item_discount",
        ],
        "float_fields": ["item_price", "item_discount"],
        "int_fields": ["quantity"],
    },
    "reviews": {
        "file": "reviews.csv",
        "columns": [
            "review_id",
            "order_id",
            "customer_id",
            "product_id",
            "rating",
            "review_text",
            "review_date",
        ],
        "float_fields": [],
        "int_fields": ["rating"],
    },
}


def create_tables(conn: sqlite3.Connection) -> None:
    for ddl in SCHEMAS.values():
        conn.execute(ddl)


def clear_tables(conn: sqlite3.Connection) -> None:
    # Delete in dependency order to satisfy foreign key constraints.
    for table in reversed(LOAD_SEQUENCE):
        conn.execute(f"DELETE FROM {table}")


def read_rows(table_name: str):
    config = TABLE_CONFIGS[table_name]
    path = DATA_DIR / config["file"]
    rows = []
    with path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            rows.append(convert_row(row, config))
    return rows


def convert_row(row: dict, config: dict) -> tuple:
    converted = []
    float_fields = set(config.get("float_fields", []))
    int_fields = set(config.get("int_fields", []))
    for column in config["columns"]:
        value = row.get(column, "").strip()
        if value == "":
            converted.append(None)
            continue
        if column in float_fields:
            converted.append(float(value))
        elif column in int_fields:
            converted.append(int(value))
        else:
            converted.append(value)
    return tuple(converted)


def insert_rows(conn: sqlite3.Connection, table_name: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    placeholders = ", ".join("?" for _ in TABLE_CONFIGS[table_name]["columns"])
    column_list = ", ".join(TABLE_CONFIGS[table_name]["columns"])
    query = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"
    conn.executemany(query, rows)
    return len(rows)


def main() -> None:
    DB_PATH.touch(exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        create_tables(conn)
        clear_tables(conn)
        summary = {}
        for table in LOAD_SEQUENCE:
            rows = read_rows(table)
            with conn:  # start transaction per table
                inserted = insert_rows(conn, table, rows)
            summary[table] = inserted
    for table, count in summary.items():
        print(f"{table}: inserted {count} rows")


if __name__ == "__main__":
    main()

