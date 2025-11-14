import sqlite3
from pathlib import Path

import pandas as pd


QUERY = """
SELECT
  c.name AS customer_name,
  p.name AS product_name,
  oi.quantity,
  p.price,
  o.total_amount,
  pay.payment_method,
  pay.payment_status
FROM orders AS o
JOIN customers AS c
  ON c.customer_id = o.customer_id
JOIN order_items AS oi
  ON oi.order_id = o.order_id
JOIN products AS p
  ON p.product_id = oi.product_id
LEFT JOIN payments AS pay
  ON pay.order_id = o.order_id;
"""


def main() -> None:
    base_dir = Path(__file__).parent.resolve()
    db_path = base_dir / "ecommerce.db"
    output_file = base_dir / "data" / "order_report.csv"

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(QUERY, conn)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"Saved {len(df)} rows to {output_file}")


if __name__ == "__main__":
    main()

