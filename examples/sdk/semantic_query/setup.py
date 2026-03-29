
import random
import sqlite3
from datetime import date, timedelta
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "example.db"

COUNTRIES = [
    ("United Kingdom", "GBP"),
    ("United States", "USD"),
    ("Germany", "EUR"),
    ("Canada", "CAD"),
    ("Australia", "AUD"),
    ("Netherlands", "EUR"),
]

CHANNELS = [
    "Direct",
    "Email",
    "Paid Search",
    "Paid Social",
    "Affiliate",
    "Organic Search",
    "Partner",
]

ISSUE_TYPES = [
    "Shipping",
    "Returns",
    "Sizing",
    "Refund",
    "Order status",
    "Damaged item",
]

SEVERITIES = ["low", "medium", "high"]

PRODUCTS = [
    ("P-100", "Performance Hoodie", "Apparel", "Outerwear", 72.0, 38.0),
    ("P-101", "Trail Sneaker", "Footwear", "Running", 118.0, 61.0),
    ("P-102", "Commuter Backpack", "Accessories", "Bags", 95.0, 44.0),
    ("P-103", "Studio Legging", "Apparel", "Activewear", 84.0, 36.0),
    ("P-104", "Hydration Bottle", "Accessories", "Fitness", 28.0, 9.0),
    ("P-105", "Travel Shell Jacket", "Apparel", "Outerwear", 156.0, 81.0),
    ("P-106", "Cross-Training Short", "Apparel", "Activewear", 58.0, 24.0),
    ("P-107", "Everyday Cap", "Accessories", "Headwear", 26.0, 8.0),
    ("P-108", "Merino Base Layer", "Apparel", "Baselayer", 92.0, 45.0),
    ("P-109", "Recovery Slide", "Footwear", "Recovery", 46.0, 18.0),
]

FIRST_NAMES = [
    "Ava",
    "Liam",
    "Noah",
    "Sophia",
    "Mia",
    "Ethan",
    "Olivia",
    "Lucas",
    "Isla",
    "Mason",
    "Harper",
    "Leo",
    "Ella",
    "Aria",
    "Jack",
    "Nora",
]

LAST_NAMES = [
    "Thompson",
    "Carter",
    "Bennett",
    "Reed",
    "Foster",
    "Ward",
    "Patel",
    "Murphy",
    "Schmidt",
    "Clarke",
    "Hughes",
    "Singh",
]

SEGMENTS = ["Retail", "Retail", "Retail", "Marketplace", "Wholesale"]
LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]


def build_customers() -> list[tuple[object, ...]]:
    customers: list[tuple[object, ...]] = []
    start_date = date(2023, 1, 1)
    for index in range(36):
        customer_id = 1001 + index
        first_name = FIRST_NAMES[index % len(FIRST_NAMES)]
        last_name = LAST_NAMES[(index * 2) % len(LAST_NAMES)]
        full_name = f"{first_name} {last_name}"
        country, _currency = COUNTRIES[index % len(COUNTRIES)]
        loyalty_tier = LOYALTY_TIERS[min(index // 9, len(LOYALTY_TIERS) - 1)]
        segment = SEGMENTS[index % len(SEGMENTS)]
        signup_date = start_date + timedelta(days=index * 19)
        email = (
            f"{first_name.lower()}.{last_name.lower()}{customer_id}@example.com"
        )
        customers.append(
            (
                customer_id,
                full_name,
                email,
                country,
                loyalty_tier,
                segment,
                signup_date.isoformat(),
            )
        )
    return customers


def build_orders(
    customers: list[tuple[object, ...]],
) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]], list[tuple[object, ...]]]:
    rng = random.Random(20250313)
    customer_lookup = {int(row[0]): row for row in customers}
    product_lookup = {product_id: product for product_id, *product in PRODUCTS}

    orders: list[tuple[object, ...]] = []
    order_items: list[tuple[object, ...]] = []
    support_tickets: list[tuple[object, ...]] = []

    ticket_count = 0
    order_count = 0
    start_date = date(2025, 1, 1)

    for month_index in range(9):
        month_start = date(start_date.year, start_date.month + month_index, 1)
        monthly_orders = 14 + month_index * 2
        for intra_month_index in range(monthly_orders):
            order_count += 1
            order_id = f"O-{1000 + order_count}"
            customer_row = customers[(order_count * 7 + intra_month_index) % len(customers)]
            customer_id = int(customer_row[0])
            country = str(customer_row[3])
            currency = next(code for name, code in COUNTRIES if name == country)
            order_date = month_start + timedelta(days=(intra_month_index * 2) % 27)
            order_month = order_date.strftime("%Y-%m")
            acquisition_channel = CHANNELS[(order_count + month_index) % len(CHANNELS)]

            item_count = 1 + ((order_count + month_index) % 3)
            selected_products = [
                PRODUCTS[(order_count + offset * 3) % len(PRODUCTS)]
                for offset in range(item_count)
            ]

            gross_revenue = 0.0
            total_cost = 0.0
            for offset, product in enumerate(selected_products):
                product_id, _name, _category, _subcategory, list_price, unit_cost = product
                quantity = 1 + ((order_count + offset) % 2)
                effective_price = round(list_price * (0.92 + (0.03 * ((month_index + offset) % 3))), 2)
                gross_revenue += quantity * effective_price
                total_cost += quantity * unit_cost
                order_items.append((order_id, product_id, quantity, effective_price))

            loyalty_tier = str(customer_lookup[customer_id][4])
            base_discount = {"Bronze": 0.02, "Silver": 0.04, "Gold": 0.07, "Platinum": 0.1}[loyalty_tier]
            channel_discount = 0.03 if acquisition_channel in {"Paid Social", "Affiliate"} else 0.0
            discount_amount = round(gross_revenue * (base_discount + channel_discount), 2)
            shipping_amount = round(6.5 + item_count * 2.5 + (month_index % 3), 2)

            order_status = "fulfilled"
            if order_count % 19 == 0:
                order_status = "refunded"
            elif order_count % 31 == 0:
                order_status = "partially_refunded"

            if order_status == "refunded":
                net_revenue = round((gross_revenue - discount_amount + shipping_amount) * 0.15, 2)
                gross_margin = round(net_revenue - (total_cost * 0.2), 2)
            elif order_status == "partially_refunded":
                net_revenue = round((gross_revenue - discount_amount + shipping_amount) * 0.62, 2)
                gross_margin = round(net_revenue - (total_cost * 0.65), 2)
            else:
                net_revenue = round(gross_revenue - discount_amount + shipping_amount, 2)
                gross_margin = round(net_revenue - total_cost, 2)

            orders.append(
                (
                    order_id,
                    order_date.isoformat(),
                    order_month,
                    customer_id,
                    country,
                    acquisition_channel,
                    currency,
                    round(gross_revenue, 2),
                    discount_amount,
                    shipping_amount,
                    net_revenue,
                    gross_margin,
                    order_status,
                )
            )

            if order_count % 5 == 0 or order_status != "fulfilled":
                ticket_count += 1
                issue_type = ISSUE_TYPES[(order_count + month_index) % len(ISSUE_TYPES)]
                severity = SEVERITIES[(order_count + ticket_count) % len(SEVERITIES)]
                status = "open" if severity == "high" and order_status != "fulfilled" else "resolved"
                response_minutes = 18 + ((order_count * 7) % 90)
                support_tickets.append(
                    (
                        f"T-{2000 + ticket_count}",
                        (order_date + timedelta(days=(order_count % 4))).isoformat(),
                        customer_id,
                        country,
                        issue_type,
                        severity,
                        status,
                        response_minutes,
                    )
                )

    return orders, order_items, support_tickets


def setup_database(db_path: Path = DB_PATH) -> Path:
    customers = build_customers()
    orders, order_items, support_tickets = build_orders(customers)

    if db_path.exists():
        db_path.unlink()

    connection = sqlite3.connect(str(db_path))
    cursor = connection.cursor()

    cursor.executescript(
        """
        DROP VIEW IF EXISTS orders_enriched;
        DROP TABLE IF EXISTS support_tickets;
        DROP TABLE IF EXISTS order_items;
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS products;
        DROP TABLE IF EXISTS customers;

        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL,
            email TEXT NOT NULL,
            country TEXT NOT NULL,
            loyalty_tier TEXT NOT NULL,
            segment TEXT NOT NULL,
            signup_date TEXT NOT NULL
        );

        CREATE TABLE products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            subcategory TEXT NOT NULL,
            list_price REAL NOT NULL,
            unit_cost REAL NOT NULL
        );

        CREATE TABLE orders (
            order_id TEXT PRIMARY KEY,
            order_date TEXT NOT NULL,
            order_month TEXT NOT NULL,
            customer_id INTEGER NOT NULL,
            country TEXT NOT NULL,
            acquisition_channel TEXT NOT NULL,
            currency TEXT NOT NULL,
            gross_revenue REAL NOT NULL,
            discount_amount REAL NOT NULL,
            shipping_amount REAL NOT NULL,
            net_revenue REAL NOT NULL,
            gross_margin REAL NOT NULL,
            order_status TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );

        CREATE TABLE order_items (
            order_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );

        CREATE TABLE support_tickets (
            ticket_id TEXT PRIMARY KEY,
            created_date TEXT NOT NULL,
            customer_id INTEGER NOT NULL,
            country TEXT NOT NULL,
            issue_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            status TEXT NOT NULL,
            first_response_minutes INTEGER NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );
        """
    )

    cursor.executemany(
        """
        INSERT INTO customers (
            customer_id, customer_name, email, country, loyalty_tier, segment, signup_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        customers,
    )
    cursor.executemany(
        """
        INSERT INTO products (
            product_id, product_name, category, subcategory, list_price, unit_cost
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        PRODUCTS,
    )
    cursor.executemany(
        """
        INSERT INTO orders (
            order_id, order_date, order_month, customer_id, country, acquisition_channel,
            currency, gross_revenue, discount_amount, shipping_amount, net_revenue,
            gross_margin, order_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        orders,
    )
    cursor.executemany(
        """
        INSERT INTO order_items (
            order_id, product_id, quantity, unit_price
        ) VALUES (?, ?, ?, ?)
        """,
        order_items,
    )
    cursor.executemany(
        """
        INSERT INTO support_tickets (
            ticket_id, created_date, customer_id, country, issue_type, severity, status, first_response_minutes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        support_tickets,
    )

    cursor.executescript(
        """
        CREATE VIEW orders_enriched AS
        WITH item_summary AS (
            SELECT
                order_id,
                SUM(quantity) AS item_count
            FROM order_items
            GROUP BY order_id
        )
        SELECT
            o.order_id,
            o.order_date,
            o.order_month,
            o.customer_id,
            c.customer_name,
            c.loyalty_tier,
            c.segment,
            o.country,
            o.acquisition_channel,
            o.currency,
            item_summary.item_count,
            o.gross_revenue,
            o.discount_amount,
            o.shipping_amount,
            o.net_revenue,
            o.gross_margin,
            ROUND(
                CASE
                    WHEN o.gross_revenue = 0 THEN 0
                    ELSE o.discount_amount / o.gross_revenue
                END,
                4
            ) AS discount_rate,
            o.order_status
        FROM orders AS o
        INNER JOIN customers AS c
            ON c.customer_id = o.customer_id
        LEFT JOIN item_summary
            ON item_summary.order_id = o.order_id;
        """
    )

    connection.commit()
    connection.close()
    return db_path


if __name__ == "__main__":
    output = setup_database()
    print(f"Seeded example database at {output}")
