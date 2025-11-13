import sqlite3
from pathlib import Path

import pandas as pd


def get_data_paths(base_dir: Path) -> dict:
    data_dir = base_dir / "data"
    return {
        "customers": data_dir / "customers.csv",
        "products": data_dir / "products.csv",
        "orders": data_dir / "orders.csv",
        "order_items": data_dir / "order_items.csv",
        "payments": data_dir / "payments.csv",
    }


def create_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    # Ensure foreign keys are enforced (safe even if we didn't add constraints)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def create_tables(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            country TEXT NOT NULL
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_date TEXT NOT NULL,
            total_amount REAL NOT NULL
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            quantity INTEGER NOT NULL
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            payment_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            payment_method TEXT NOT NULL,
            payment_status TEXT NOT NULL
        );
        """
    )

    conn.commit()


def read_csvs(paths: dict) -> dict:
    # Explicit dtypes to keep IDs as text and numbers properly typed
    dfs = {
        "customers": pd.read_csv(
            paths["customers"],
            dtype={
                "customer_id": "string",
                "name": "string",
                "email": "string",
                "country": "string",
            },
        ),
        "products": pd.read_csv(
            paths["products"],
            dtype={
                "product_id": "string",
                "name": "string",
                "category": "string",
                "price": "float",
            },
        ),
        "orders": pd.read_csv(
            paths["orders"],
            dtype={
                "order_id": "string",
                "customer_id": "string",
                "order_date": "string",
                "total_amount": "float",
            },
        ),
        "order_items": pd.read_csv(
            paths["order_items"],
            dtype={
                "order_item_id": "string",
                "order_id": "string",
                "product_id": "string",
                "quantity": "int64",
            },
        ),
        "payments": pd.read_csv(
            paths["payments"],
            dtype={
                "payment_id": "string",
                "order_id": "string",
                "payment_method": "string",
                "payment_status": "string",
            },
        ),
    }

    # Normalize pandas string dtype to plain Python strings when writing
    for name, df in dfs.items():
        object_like_cols = [
            c for c in df.columns if str(df[c].dtype).startswith(("string", "object"))
        ]
        for c in object_like_cols:
            df[c] = df[c].astype(str)
    return dfs


def insert_data(conn: sqlite3.Connection, dfs: dict) -> None:
    # Insert in an order that respects typical foreign key relationships
    load_order = ["customers", "products", "orders", "order_items", "payments"]
    for table_name in load_order:
        dfs[table_name].to_sql(table_name, conn, if_exists="append", index=False)


def main() -> None:
    base_dir = Path(__file__).parent.resolve()
    db_path = base_dir / "ecommerce.db"
    paths = get_data_paths(base_dir)

    conn = create_connection(db_path)
    try:
        create_tables(conn)
        dataframes = read_csvs(paths)
        insert_data(conn, dataframes)
        conn.commit()
    finally:
        conn.close()

    print("Data Ingested Successfully")


if __name__ == "__main__":
    main()

