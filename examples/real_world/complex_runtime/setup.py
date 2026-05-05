
import argparse
import csv
import random
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR_NAME = "data"
COMMERCE_DB_NAME = "commerce.db"
GROWTH_DB_NAME = "growth_ops.db"
SPEND_CSV_NAME = "channel_spend_targets.csv"

RNG_SEED = 20260327

CHANNELS = [
    "Paid Search",
    "Paid Social",
    "Email",
    "Affiliate",
    "Partner",
    "Organic Search",
]

SEGMENTS = ["Enterprise", "Mid-Market", "SMB"]
LIFECYCLE_STAGES = ["Lead", "MQL", "SQL", "Customer", "Expansion"]
REGIONS = ["North America", "United Kingdom", "DACH", "ANZ"]
COUNTRIES = {
    "North America": "United States",
    "United Kingdom": "United Kingdom",
    "DACH": "Germany",
    "ANZ": "Australia",
}
ACCOUNT_MANAGERS = ["Ava Reed", "Luca Patel", "Mina Foster", "Noah Singh"]
CAMPAIGN_FAMILY = {
    "Paid Search": ["Q1 Search Capture", "Brand Search Push", "Category Expansion"],
    "Paid Social": ["Spring Social Launch", "Lookalike Acquisition", "Retention Social"],
    "Email": ["Winback Journey", "VIP Launch", "Regional Newsletter"],
    "Affiliate": ["Partner Commerce Push", "Creator Bundle", "Review Site Feature"],
    "Partner": ["Channel Enablement", "Co-Sell Campaign", "Marketplace Boost"],
    "Organic Search": ["SEO Content Cluster", "Product Education Hub", "Comparison Pages"],
}
PRODUCTS = [
    ("SKU-100", "Trail Jacket", "Apparel", "Outerwear", 149.0, 74.0),
    ("SKU-101", "Commuter Pack", "Accessories", "Bags", 119.0, 52.0),
    ("SKU-102", "All Weather Shoe", "Footwear", "Lifestyle", 132.0, 63.0),
    ("SKU-103", "Performance Tee", "Apparel", "Tops", 48.0, 18.0),
    ("SKU-104", "Training Short", "Apparel", "Bottoms", 58.0, 23.0),
    ("SKU-105", "Travel Bottle", "Accessories", "Hydration", 26.0, 7.0),
    ("SKU-106", "Weekender Pant", "Apparel", "Bottoms", 96.0, 41.0),
    ("SKU-107", "Recovery Sandal", "Footwear", "Recovery", 54.0, 20.0),
]
FIRST_NAMES = [
    "Ava",
    "Noah",
    "Mia",
    "Ethan",
    "Isla",
    "Leo",
    "Nora",
    "Mason",
    "Ella",
    "Liam",
    "Sofia",
    "Aria",
]
LAST_NAMES = [
    "Reed",
    "Patel",
    "Foster",
    "Singh",
    "Clarke",
    "Murphy",
    "Ward",
    "Hughes",
    "Bennett",
    "Schmidt",
    "Carter",
    "Thompson",
]
TICKET_TYPES = ["Shipping", "Returns", "Sizing", "Damaged Item", "Billing", "Subscription"]
SEVERITIES = ["low", "medium", "high"]


@dataclass(frozen=True)
class Customer:
    customer_id: int
    customer_name: str
    segment: str
    lifecycle_stage: str
    acquisition_source: str
    region: str
    country: str
    signup_date: str
    account_manager: str


def build_customers() -> list[Customer]:
    customers: list[Customer] = []
    signup_start = date(2024, 4, 1)
    for index in range(48):
        customer_id = 1001 + index
        first_name = FIRST_NAMES[index % len(FIRST_NAMES)]
        last_name = LAST_NAMES[(index * 3) % len(LAST_NAMES)]
        region = REGIONS[index % len(REGIONS)]
        segment = SEGMENTS[index % len(SEGMENTS)]
        lifecycle_stage = LIFECYCLE_STAGES[min(index // 10, len(LIFECYCLE_STAGES) - 1)]
        acquisition_source = CHANNELS[(index * 2) % len(CHANNELS)]
        customers.append(
            Customer(
                customer_id=customer_id,
                customer_name=f"{first_name} {last_name}",
                segment=segment,
                lifecycle_stage=lifecycle_stage,
                acquisition_source=acquisition_source,
                region=region,
                country=COUNTRIES[region],
                signup_date=(signup_start + timedelta(days=index * 9)).isoformat(),
                account_manager=ACCOUNT_MANAGERS[index % len(ACCOUNT_MANAGERS)],
            )
        )
    return customers


def seed_commerce_database(db_path: Path, *, customers: list[Customer]) -> dict[str, list[tuple[object, ...]]]:
    rng = random.Random(RNG_SEED)
    if db_path.exists():
        db_path.unlink()

    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(db_path))
    cursor = connection.cursor()
    cursor.executescript(
        """
        CREATE TABLE orders (
            order_id TEXT PRIMARY KEY,
            order_date DATE NOT NULL,
            customer_id INTEGER NOT NULL,
            order_channel TEXT NOT NULL,
            fulfillment_region TEXT NOT NULL,
            order_status TEXT NOT NULL,
            currency TEXT NOT NULL,
            gross_revenue REAL NOT NULL,
            discount_amount REAL NOT NULL,
            shipping_amount REAL NOT NULL,
            refund_amount REAL NOT NULL,
            net_revenue REAL NOT NULL,
            gross_margin REAL NOT NULL
        );

        CREATE TABLE products (
            product_sku TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            subcategory TEXT NOT NULL,
            list_price REAL NOT NULL,
            unit_cost REAL NOT NULL
        );

        CREATE TABLE order_items (
            line_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            product_sku TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            net_line_revenue REAL NOT NULL,
            line_margin REAL NOT NULL
        );
        """
    )

    cursor.executemany(
        """
        INSERT INTO products (
            product_sku, product_name, category, subcategory, list_price, unit_cost
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        PRODUCTS,
    )

    orders: list[tuple[object, ...]] = []
    order_items: list[tuple[object, ...]] = []
    attribution_by_key: dict[str, list[object]] = {}
    support_rows: list[tuple[object, ...]] = []

    order_count = 0
    line_count = 0
    ticket_count = 0
    month_start = date(2025, 1, 1)

    for month_index in range(9):
        current_month = date(month_start.year, month_start.month + month_index, 1)
        monthly_orders = 18 + (month_index * 3)
        for intra_month_index in range(monthly_orders):
            customer = customers[(month_index * 7 + intra_month_index * 3) % len(customers)]
            order_count += 1
            order_id = f"O-{10000 + order_count}"
            channel = CHANNELS[(customer.customer_id + month_index + intra_month_index) % len(CHANNELS)]
            order_date = current_month + timedelta(days=(intra_month_index * 2) % 26)
            order_status = "fulfilled"
            if order_count % 11 == 0:
                order_status = "partially_refunded"
            elif order_count % 17 == 0:
                order_status = "refunded"

            item_count = 1 + ((order_count + month_index) % 3)
            selected_products = [
                PRODUCTS[(order_count + offset + month_index) % len(PRODUCTS)]
                for offset in range(item_count)
            ]

            gross_revenue = 0.0
            gross_margin = 0.0
            for offset, product in enumerate(selected_products):
                product_sku, _product_name, _category, _subcategory, list_price, unit_cost = product
                quantity = 1 + ((order_count + offset) % 2)
                price_factor = 0.9 + (0.03 * ((month_index + offset) % 4))
                unit_price = round(list_price * price_factor, 2)
                net_line_revenue = round(quantity * unit_price, 2)
                line_margin = round(net_line_revenue - (quantity * unit_cost), 2)
                line_count += 1
                order_items.append(
                    (
                        f"L-{line_count:05d}",
                        order_id,
                        product_sku,
                        quantity,
                        unit_price,
                        net_line_revenue,
                        line_margin,
                    )
                )
                gross_revenue += net_line_revenue
                gross_margin += line_margin

            discount_rate = 0.03 + (0.01 * (month_index % 4))
            if customer.segment == "Enterprise":
                discount_rate += 0.02
            discount_amount = round(gross_revenue * discount_rate, 2)
            shipping_amount = round(6.0 + item_count * 1.75 + (month_index % 3), 2)
            refund_amount = 0.0
            if order_status == "refunded":
                refund_amount = round(gross_revenue * 0.82, 2)
            elif order_status == "partially_refunded":
                refund_amount = round(gross_revenue * 0.28, 2)
            net_revenue = round(gross_revenue - discount_amount + shipping_amount - refund_amount, 2)
            gross_margin = round(gross_margin - (refund_amount * 0.45), 2)

            orders.append(
                (
                    order_id,
                    order_date.isoformat(),
                    customer.customer_id,
                    channel,
                    customer.region,
                    order_status,
                    "USD",
                    round(gross_revenue, 2),
                    discount_amount,
                    shipping_amount,
                    refund_amount,
                    net_revenue,
                    gross_margin,
                )
            )

            customer_month_channel_key = (
                f"{customer.customer_id}|{order_date.strftime('%Y-%m')}|{channel}"
            )
            channel_month_key = f"{order_date.strftime('%Y-%m')}|{channel}"
            campaign_name = CAMPAIGN_FAMILY[channel][(order_count + month_index) % len(CAMPAIGN_FAMILY[channel])]
            attributed_touches = 2 + ((order_count + month_index) % 5)
            influenced_pipeline = round(net_revenue * (1.12 + ((order_count % 4) * 0.08)), 2)
            assisted_signups = 1 + ((customer.customer_id + month_index) % 2)
            if customer_month_channel_key not in attribution_by_key:
                attribution_by_key[customer_month_channel_key] = [
                    customer_month_channel_key,
                    customer.customer_id,
                    date(order_date.year, order_date.month, 1).isoformat(),
                    channel,
                    channel_month_key,
                    campaign_name,
                    "converted" if net_revenue >= 150 else "nurture",
                    attributed_touches,
                    influenced_pipeline,
                    assisted_signups,
                ]
            else:
                existing = attribution_by_key[customer_month_channel_key]
                existing[7] = int(existing[7]) + attributed_touches
                existing[8] = round(float(existing[8]) + influenced_pipeline, 2)
                existing[9] = int(existing[9]) + assisted_signups
                if net_revenue >= 150:
                    existing[6] = "converted"

            ticket_mod = 5 if order_status == "fulfilled" else 2
            if order_count % ticket_mod == 0:
                ticket_count += 1
                opened_date = order_date + timedelta(days=1 + (order_count % 4))
                severity = SEVERITIES[(order_count + month_index) % len(SEVERITIES)]
                support_rows.append(
                    (
                        f"T-{20000 + ticket_count}",
                        customer.customer_id,
                        opened_date.isoformat(),
                        TICKET_TYPES[(order_count + intra_month_index) % len(TICKET_TYPES)],
                        severity,
                        round(5.0 + rng.random() * 30.0 + (6.0 if severity == "high" else 0.0), 2),
                    )
                )

    cursor.executemany(
        """
        INSERT INTO orders (
            order_id, order_date, customer_id, order_channel, fulfillment_region, order_status,
            currency, gross_revenue, discount_amount, shipping_amount, refund_amount,
            net_revenue, gross_margin
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        orders,
    )
    cursor.executemany(
        """
        INSERT INTO order_items (
            line_id, order_id, product_sku, quantity, unit_price, net_line_revenue, line_margin
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        order_items,
    )
    connection.commit()
    connection.close()

    attribution_rows = [tuple(values) for values in attribution_by_key.values()]
    return {
        "campaign_attribution": attribution_rows,
        "support_tickets": support_rows,
    }


def seed_growth_database(
    db_path: Path,
    *,
    customers: list[Customer],
    attribution_rows: list[tuple[object, ...]],
    support_rows: list[tuple[object, ...]],
) -> None:
    if db_path.exists():
        db_path.unlink()

    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(db_path))
    cursor = connection.cursor()
    cursor.executescript(
        """
        CREATE TABLE customer_profiles (
            customer_id INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL,
            segment TEXT NOT NULL,
            lifecycle_stage TEXT NOT NULL,
            acquisition_source TEXT NOT NULL,
            region TEXT NOT NULL,
            country TEXT NOT NULL,
            signup_date DATE NOT NULL,
            account_manager TEXT NOT NULL
        );

        CREATE TABLE campaign_attribution_monthly (
            customer_month_channel_key TEXT PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            attribution_month DATE NOT NULL,
            acquisition_channel TEXT NOT NULL,
            channel_month_key TEXT NOT NULL,
            primary_campaign TEXT NOT NULL,
            funnel_stage TEXT NOT NULL,
            attributed_touches INTEGER NOT NULL,
            influenced_pipeline REAL NOT NULL,
            assisted_signups INTEGER NOT NULL
        );

        CREATE TABLE support_tickets (
            ticket_id TEXT PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            opened_date DATE NOT NULL,
            ticket_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            resolution_hours REAL NOT NULL
        );
        """
    )

    cursor.executemany(
        """
        INSERT INTO customer_profiles (
            customer_id, customer_name, segment, lifecycle_stage, acquisition_source,
            region, country, signup_date, account_manager
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                customer.customer_id,
                customer.customer_name,
                customer.segment,
                customer.lifecycle_stage,
                customer.acquisition_source,
                customer.region,
                customer.country,
                customer.signup_date,
                customer.account_manager,
            )
            for customer in customers
        ],
    )
    cursor.executemany(
        """
        INSERT INTO campaign_attribution_monthly (
            customer_month_channel_key, customer_id, attribution_month, acquisition_channel,
            channel_month_key, primary_campaign, funnel_stage, attributed_touches,
            influenced_pipeline, assisted_signups
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        attribution_rows,
    )
    cursor.executemany(
        """
        INSERT INTO support_tickets (
            ticket_id, customer_id, opened_date, ticket_type, severity, resolution_hours
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        support_rows,
    )
    connection.commit()
    connection.close()


def write_channel_spend_csv(
    csv_path: Path,
    *,
    attribution_rows: list[tuple[object, ...]],
) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    aggregated_customers: dict[tuple[str, str], set[int]] = defaultdict(set)
    for row in attribution_rows:
        _customer_month_channel_key, customer_id, attribution_month, acquisition_channel, _channel_month_key, *_rest = row
        aggregated_customers[(str(attribution_month)[:7], str(acquisition_channel))].add(int(customer_id))

    rows: list[dict[str, object]] = []
    for month in [f"2025-{month:02d}" for month in range(1, 10)]:
        for channel_index, channel in enumerate(CHANNELS):
            key = (month, channel)
            customer_count = len(aggregated_customers.get(key, set()))
            rows.append(
                {
                    "channel_month_key": f"{month}|{channel}",
                    "spend_month": f"{month}-01",
                    "acquisition_channel": channel,
                    "geo_region": "Global",
                    "target_segment": SEGMENTS[channel_index % len(SEGMENTS)],
                    "spend_amount": round(8500 + (channel_index * 1450) + (customer_count * 210), 2),
                    "impressions": 180000 + (channel_index * 22000) + (customer_count * 3500),
                    "clicks": 3600 + (channel_index * 540) + (customer_count * 42),
                    "qualified_leads_target": max(14, customer_count + 10 + channel_index * 3),
                }
            )

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "channel_month_key",
                "spend_month",
                "acquisition_channel",
                "geo_region",
                "target_segment",
                "spend_amount",
                "impressions",
                "clicks",
                "qualified_leads_target",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def seed_example(base_dir: Path) -> dict[str, Path]:
    customers = build_customers()
    data_dir = base_dir / DATA_DIR_NAME
    commerce_db_path = data_dir / COMMERCE_DB_NAME
    growth_db_path = data_dir / GROWTH_DB_NAME
    spend_csv_path = data_dir / SPEND_CSV_NAME

    commerce_payload = seed_commerce_database(commerce_db_path, customers=customers)
    seed_growth_database(
        growth_db_path,
        customers=customers,
        attribution_rows=commerce_payload["campaign_attribution"],
        support_rows=commerce_payload["support_tickets"],
    )
    write_channel_spend_csv(
        spend_csv_path,
        attribution_rows=commerce_payload["campaign_attribution"],
    )
    return {
        "commerce_db": commerce_db_path,
        "growth_db": growth_db_path,
        "spend_csv": spend_csv_path,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed the complex Langbridge runtime example data.")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=BASE_DIR,
        help="Example directory that contains langbridge_config.yml and the data/ output folder.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = seed_example(args.base_dir.resolve())
    for key, value in paths.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
