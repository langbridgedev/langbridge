BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS shops CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS shipments CASCADE;
DROP TABLE IF EXISTS returns CASCADE;
DROP TABLE IF EXISTS inventory CASCADE;

CREATE TABLE customers (
  id BIGSERIAL PRIMARY KEY,
  -- Shared key for cross-source joins with crm.contacts.contact_external_id
  crm_contact_external_id TEXT UNIQUE,
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  phone TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  marketing_opt_in BOOLEAN NOT NULL DEFAULT false,
  loyalty_tier TEXT NOT NULL DEFAULT 'bronze',
  country TEXT NOT NULL DEFAULT 'US',
  state TEXT,
  city TEXT,
  postal_code TEXT
);

CREATE TABLE shops (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  store_type TEXT NOT NULL,
  city TEXT NOT NULL,
  state TEXT NOT NULL,
  country TEXT NOT NULL DEFAULT 'US',
  opened_at DATE NOT NULL
);

CREATE TABLE products (
  id BIGSERIAL PRIMARY KEY,
  sku TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  category TEXT NOT NULL,
  subcategory TEXT NOT NULL,
  price NUMERIC(10,2) NOT NULL,
  cost NUMERIC(10,2) NOT NULL,
  active BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE orders (
  id BIGSERIAL PRIMARY KEY,
  customer_id BIGINT NOT NULL REFERENCES customers(id),
  shop_id BIGINT REFERENCES shops(id),
  channel TEXT NOT NULL,
  status TEXT NOT NULL,
  order_ts TIMESTAMPTZ NOT NULL,
  subtotal NUMERIC(12,2) NOT NULL DEFAULT 0,
  discount NUMERIC(12,2) NOT NULL DEFAULT 0,
  tax NUMERIC(12,2) NOT NULL DEFAULT 0,
  shipping NUMERIC(12,2) NOT NULL DEFAULT 0,
  total NUMERIC(12,2) NOT NULL DEFAULT 0,
  payment_method TEXT NOT NULL
);

CREATE TABLE order_items (
  id BIGSERIAL PRIMARY KEY,
  order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  product_id BIGINT NOT NULL REFERENCES products(id),
  quantity INT NOT NULL,
  unit_price NUMERIC(10,2) NOT NULL,
  discount NUMERIC(10,2) NOT NULL DEFAULT 0,
  line_total NUMERIC(12,2) NOT NULL
);

CREATE TABLE payments (
  id BIGSERIAL PRIMARY KEY,
  order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  payment_ts TIMESTAMPTZ NOT NULL,
  amount NUMERIC(12,2) NOT NULL,
  method TEXT NOT NULL,
  status TEXT NOT NULL
);

CREATE TABLE shipments (
  id BIGSERIAL PRIMARY KEY,
  order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  carrier TEXT NOT NULL,
  service_level TEXT NOT NULL,
  shipped_ts TIMESTAMPTZ NOT NULL,
  delivered_ts TIMESTAMPTZ,
  shipping_cost NUMERIC(12,2) NOT NULL
);

CREATE TABLE returns (
  id BIGSERIAL PRIMARY KEY,
  order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  return_ts TIMESTAMPTZ NOT NULL,
  reason TEXT NOT NULL,
  refund_amount NUMERIC(12,2) NOT NULL
);

CREATE TABLE inventory (
  id BIGSERIAL PRIMARY KEY,
  shop_id BIGINT NOT NULL REFERENCES shops(id) ON DELETE CASCADE,
  product_id BIGINT NOT NULL REFERENCES products(id),
  on_hand INT NOT NULL,
  reorder_point INT NOT NULL
);

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_customers_crm_contact_external_id ON customers(crm_contact_external_id);
CREATE INDEX idx_orders_shop_id ON orders(shop_id);
CREATE INDEX idx_orders_channel ON orders(channel);
CREATE INDEX idx_orders_order_ts ON orders(order_ts);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_inventory_shop_product ON inventory(shop_id, product_id);
CREATE INDEX idx_shipments_order_id ON shipments(order_id);
CREATE INDEX idx_payments_order_id ON payments(order_id);

COMMIT;
