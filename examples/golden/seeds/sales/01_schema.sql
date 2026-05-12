BEGIN;

DROP TABLE IF EXISTS sales_order_items CASCADE;
DROP TABLE IF EXISTS sales_orders CASCADE;
DROP TABLE IF EXISTS sales_customers CASCADE;

CREATE TABLE sales_customers (
  id BIGINT PRIMARY KEY,
  customer_key TEXT NOT NULL UNIQUE,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  country TEXT NOT NULL,
  region TEXT NOT NULL,
  loyalty_tier TEXT NOT NULL,
  marketing_opt_in BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE sales_orders (
  id BIGINT PRIMARY KEY,
  order_number TEXT NOT NULL UNIQUE,
  customer_id BIGINT NOT NULL REFERENCES sales_customers(id),
  order_ts TIMESTAMPTZ NOT NULL,
  channel TEXT NOT NULL,
  status TEXT NOT NULL,
  fulfillment_location_name TEXT NOT NULL,
  fulfillment_location_country TEXT NOT NULL,
  subtotal NUMERIC(12,2) NOT NULL,
  discount NUMERIC(12,2) NOT NULL DEFAULT 0,
  tax NUMERIC(12,2) NOT NULL DEFAULT 0,
  shipping NUMERIC(12,2) NOT NULL DEFAULT 0,
  total NUMERIC(12,2) NOT NULL
);

CREATE TABLE sales_order_items (
  id BIGINT PRIMARY KEY,
  order_id BIGINT NOT NULL REFERENCES sales_orders(id) ON DELETE CASCADE,
  shopify_product_id BIGINT NOT NULL,
  shopify_variant_id BIGINT NOT NULL,
  sku TEXT,
  quantity INTEGER NOT NULL,
  unit_price NUMERIC(12,2) NOT NULL,
  discount NUMERIC(12,2) NOT NULL DEFAULT 0,
  line_total NUMERIC(12,2) NOT NULL
);

CREATE INDEX idx_sales_orders_customer_id ON sales_orders(customer_id);
CREATE INDEX idx_sales_orders_order_ts ON sales_orders(order_ts);
CREATE INDEX idx_sales_orders_channel ON sales_orders(channel);
CREATE INDEX idx_sales_order_items_order_id ON sales_order_items(order_id);
CREATE INDEX idx_sales_order_items_product_id ON sales_order_items(shopify_product_id);
CREATE INDEX idx_sales_order_items_variant_id ON sales_order_items(shopify_variant_id);

COMMIT;
