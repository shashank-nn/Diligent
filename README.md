# Diligent Ecommerce Dataset

This repo contains a small synthetic ecommerce dataset plus a loader script that
builds an `ecom.db` SQLite database ready for analysis.

## Contents
- `data/*.csv` – customers, products, orders, order_items, inventory_events, reviews.
- `ingest_to_sqlite.py` – reads the CSV files, creates tables, and inserts data.
- `ecom.db` – generated SQLite database (created after running the ingest script).

## Setup
1. Ensure Python 3.10+ is installed.
2. Install dependencies (only stdlib modules used, so none needed).
3. Run the loader:
   ```
   python ingest_to_sqlite.py
   ```
   This creates/updates `ecom.db` and prints insert counts.

## Schema Overview
- `customers(customer_id, name, email, city, state, signup_date, loyalty_tier)`
- `products(product_id, product_name, category, price, cost, currency, stock_status)`
- `orders(order_id, customer_id, order_date, order_status, payment_method, order_total, ship_city, ship_state)`
- `order_items(order_id, product_id, quantity, item_price, item_discount)`
- `reviews(review_id, order_id, customer_id, product_id, rating, review_text, review_date)`
- `inventory_events(event_id, product_id, event_type, quantity_change, event_date, warehouse)` – data-only table (not currently loaded into SQLite).

## Useful Queries
Run via `sqlite3 ecom.db` or any SQLite client:

**Top 10 products by revenue**
```
SELECT p.product_name,
       ROUND(SUM(oi.quantity * oi.item_price) - SUM(oi.item_discount), 2) AS gross_revenue
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name
ORDER BY gross_revenue DESC
LIMIT 10;
```

**Total revenue per customer**
```
SELECT c.name,
       ROUND(SUM(oi.quantity * oi.item_price) - SUM(oi.item_discount), 2) AS total_revenue
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
JOIN order_items oi ON oi.order_id = o.order_id
GROUP BY c.customer_id, c.name
ORDER BY total_revenue DESC;
```

**Recent orders with product list**
```
SELECT o.order_id,
       o.order_date,
       c.name,
       GROUP_CONCAT(p.product_name, ', ') AS products
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
JOIN order_items oi ON oi.order_id = o.order_id
JOIN products p ON p.product_id = oi.product_id
GROUP BY o.order_id, o.order_date, c.name
ORDER BY o.order_date DESC, o.order_id DESC
LIMIT 10;
```

**Average rating per product**
```
SELECT p.product_name,
       ROUND(AVG(r.rating), 2) AS avg_rating,
       COUNT(r.review_id) AS review_count
FROM products p
JOIN reviews r ON r.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY avg_rating DESC, review_count DESC;
```

## Notes
- All CSV values are fictitious and safe for demos or testing.
- Re-run `python ingest_to_sqlite.py` anytime to refresh the database from CSVs.

