SELECT setseed(0.94);


-- Customers
WITH params AS (
  SELECT
    ARRAY['Alex','Jordan','Taylor','Morgan','Casey','Riley','Avery','Cameron','Quinn','Hayden','Parker','Drew','Sydney','Elliot','Reese','Logan','Rowan','Emery','Blake','Sawyer','Charlie','Dakota','Jamie','Kendall','Lee','Mason','Harper','Noah','Olivia','Emma'] AS first_names,
    ARRAY['Smith','Johnson','Williams','Brown','Jones','Miller','Davis','Garcia','Rodriguez','Wilson','Martinez','Anderson','Taylor','Thomas','Hernandez','Moore','Martin','Lee','Perez','Thompson','White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson','Walker','Young','Allen'] AS last_names,
    ARRAY['New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','San Jose','Austin','Jacksonville','San Francisco','Columbus','Fort Worth','Charlotte','Seattle','Denver','Boston','Nashville','Atlanta','Miami','Orlando','Portland','Las Vegas'] AS cities,
    ARRAY['NY','CA','IL','TX','AZ','PA','OH','WA','MA','TN','GA','FL','OR','NV','CO','NC'] AS states
)
INSERT INTO customers (
  crm_contact_external_id,
  first_name,
  last_name,
  email,
  phone,
  created_at,
  marketing_opt_in,
  loyalty_tier,
  country,
  state,
  city,
  postal_code
)
SELECT
  format('CRM-%08s', s),
  p.fn,
  p.ln,
  format('%s.%s%05s@example.com', lower(p.fn), lower(p.ln), s),
  format('+1-555-%04s', s),
  now() - (random() * interval '730 days'),
  (random() < 0.55),
  CASE
    WHEN p.r_loyalty < 0.6 THEN 'bronze'
    WHEN p.r_loyalty < 0.85 THEN 'silver'
    WHEN p.r_loyalty < 0.95 THEN 'gold'
    ELSE 'platinum'
  END,
  'US',
  p.state,
  p.city,
  format('%05s', (10000 + floor(random() * 89999))::int)
FROM generate_series(1, 10000) s
CROSS JOIN params
CROSS JOIN LATERAL (
   SELECT
    params.first_names[1 + floor(random() * array_length(params.first_names, 1) + s*0)::int] AS fn,
    params.last_names[1 + floor(random() * array_length(params.last_names, 1) + s*0)::int] AS ln,
    params.cities[1 + floor(random() * array_length(params.cities, 1) + s*0)::int] AS city,
    params.states[1 + floor(random() * array_length(params.states, 1) + s*0)::int] AS state,
    random() AS r_loyalty
) p;

-- Shops
WITH params AS (
  SELECT
    ARRAY['Downtown','Uptown','Central','Market','Riverside','Lakeside','North','South','East','West','City Center','Plaza','Harbor','Valley','Station','Hills'] AS prefixes,
    ARRAY['New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','San Jose','Austin','Jacksonville','San Francisco','Columbus','Fort Worth','Charlotte','Seattle','Denver','Boston','Nashville','Atlanta','Miami','Orlando','Portland','Las Vegas'] AS cities,
    ARRAY['NY','CA','IL','TX','AZ','PA','OH','WA','MA','TN','GA','FL','OR','NV','CO','NC'] AS states,
    ARRAY['mall','street','outlet','kiosk','big_box'] AS store_types
)
INSERT INTO shops (
  name,
  store_type,
  city,
  state,
  country,
  opened_at
)
SELECT
  format('%s %s', p.prefix, p.city),
  p.store_type,
  p.city,
  p.state,
  'US',
  (date '2010-01-01' + (floor(random() * 5000))::int)
FROM generate_series(1, 80) s
CROSS JOIN params
CROSS JOIN LATERAL (
  SELECT 
    params.prefixes[1 + floor(random() * array_length(params.prefixes, 1)) + s*0::int] AS prefix,
    params.cities[1 + floor(random() * array_length(params.cities, 1)) + s*0::int] AS city,
    params.states[1 + floor(random() * array_length(params.states, 1)) + s*0::int] AS state,
    params.store_types[1 + floor(random() * array_length(params.store_types, 1)) + s*0::int] AS store_type
) p;

-- Products
WITH params AS (
  SELECT
    ARRAY['Premium','Classic','Eco','Smart','Essential','Ultra','Pro','Lite','Everyday','Active','Travel','Urban','Comfort','Heritage','Bold','Bright','Sleek','Compact','Fresh','Flex'] AS adjectives,
    ARRAY['Headphones','Backpack','Jacket','Sneakers','Blender','Watch','Tee','Jeans','Lamp','Notebook','Mug','Bottle','Tent','Keyboard','Mouse','Speaker','Camera','Chair','Desk','Gloves','Helmet','Mat','Cookware','Skincare','Vitamin','Charger','Tablet','Monitor','Router','Sunglasses'] AS nouns,
    ARRAY['Electronics','Apparel','Home','Beauty','Sports','Toys','Grocery','Office'] AS categories,
    ARRAY['Standard','Premium','Budget','Eco','Sport','Classic'] AS subcategories
)
INSERT INTO products (
  sku,
  name,
  category,
  subcategory,
  price,
  cost,
  active
)
SELECT
  format('SKU-%06s', s),
  format('%s %s', p.adj, p.noun),
  p.category,
  p.subcategory,
  p.price,
  round(p.price * (0.45 + random() * 0.2)::numeric, 2),
  (random() < 0.97)
FROM generate_series(1, 300) s
CROSS JOIN params
CROSS JOIN LATERAL (
  SELECT
    params.adjectives[1 + floor(random() * array_length(params.adjectives, 1)) + s*0::int] AS adj,
    params.nouns[1 + floor(random() * array_length(params.nouns, 1)) + s*0::int] AS noun,
    params.categories[1 + floor(random() * array_length(params.categories, 1)) + s*0::int] AS category,
    params.subcategories[1 + floor(random() * array_length(params.subcategories, 1)) + s*0::int] AS subcategory,
    round((random() * 400 + 5)::numeric, 2) AS price
) p;

-- Orders
WITH customer_base AS (
  SELECT
    id AS customer_id,
    state,
    loyalty_tier,
    CASE
      WHEN loyalty_tier = 'platinum' THEN (6 + floor(random() * 10))::int
      WHEN loyalty_tier = 'gold' THEN (4 + floor(random() * 8))::int
      WHEN loyalty_tier = 'silver' THEN (2 + floor(random() * 6))::int
      ELSE (1 + floor(random() * 3))::int
    END AS order_count,
    CASE WHEN random() < 0.6 THEN 'online' ELSE 'store' END AS preferred_channel
  FROM customers
),
shops_by_state AS (
  SELECT state, array_agg(id) AS shop_ids
  FROM shops
  GROUP BY state
),
shops_all AS (
  SELECT array_agg(id) AS shop_ids
  FROM shops
),
orders_src AS (
  SELECT
    c.customer_id,
    c.state,
    c.preferred_channel,
    gs.n AS order_n,
    CASE
      WHEN random() < 0.7 THEN c.preferred_channel
      ELSE CASE WHEN c.preferred_channel = 'online' THEN 'store' ELSE 'online' END
    END AS channel,
    random() AS r_status,
    now() - ((random() * random()) * interval '730 days') AS order_ts
  FROM customer_base c
  JOIN LATERAL generate_series(1, c.order_count) gs(n) ON true
)
INSERT INTO orders (
  customer_id,
  shop_id,
  channel,
  status,
  order_ts,
  subtotal,
  discount,
  tax,
  shipping,
  total,
  payment_method
)
SELECT
  o.customer_id,
  CASE
    WHEN o.channel = 'store' THEN
      COALESCE(
        s.shop_ids[1 + floor(random() * array_length(s.shop_ids, 1))::int],
        a.shop_ids[1 + floor(random() * array_length(a.shop_ids, 1))::int]
      )
    ELSE NULL
  END,
  o.channel,
  CASE
    WHEN o.r_status < 0.04 THEN 'cancelled'
    WHEN o.r_status < 0.10 THEN 'returned'
    WHEN o.channel = 'online' AND o.r_status < 0.22 THEN 'shipped'
    WHEN o.channel = 'online' AND o.r_status < 0.32 THEN 'processing'
    ELSE 'completed'
  END,
  o.order_ts,
  0,
  0,
  0,
  0,
  0,
  CASE
    WHEN o.channel = 'store' AND random() < 0.4 THEN 'cash'
    WHEN random() < 0.7 THEN 'card'
    WHEN random() < 0.85 THEN 'gift_card'
    WHEN random() < 0.93 THEN 'paypal'
    ELSE 'klarna'
  END
FROM orders_src o
LEFT JOIN shops_by_state s ON s.state = o.state
CROSS JOIN shops_all a;

-- Order items
WITH line AS (
  SELECT
    o.id AS order_id,
    (floor(random() * 300) + 1)::bigint AS product_id,
    (floor(random() * 3) + 1)::int AS quantity,
    random() AS r_discount
  FROM orders o
  JOIN LATERAL generate_series(1, 1 + floor(random() * 4)::int) g(n) ON true
)
INSERT INTO order_items (
  order_id,
  product_id,
  quantity,
  unit_price,
  discount,
  line_total
)
SELECT
  l.order_id,
  l.product_id,
  l.quantity,
  p.price,
  round(p.price * l.quantity * d.discount_rate, 2),
  round(p.price * l.quantity - round(p.price * l.quantity * d.discount_rate, 2), 2)
FROM line l
JOIN products p ON p.id = l.product_id
CROSS JOIN LATERAL (
  SELECT CASE
    WHEN l.r_discount < 0.7 THEN 0.00
    WHEN l.r_discount < 0.85 THEN 0.05
    WHEN l.r_discount < 0.95 THEN 0.10
    ELSE 0.15
  END AS discount_rate
) d;

-- Update order totals
WITH totals AS (
  SELECT
    order_id,
    round(sum(unit_price * quantity), 2) AS subtotal,
    round(sum(discount), 2) AS discount
  FROM order_items
  GROUP BY order_id
),
shipping AS (
  SELECT
    id AS order_id,
    CASE WHEN channel = 'online' THEN round((random() * 12 + 3)::numeric, 2) ELSE 0 END AS shipping
  FROM orders
)
UPDATE orders o
SET
  subtotal = t.subtotal,
  discount = t.discount,
  tax = round((t.subtotal - t.discount) * 0.07, 2),
  shipping = s.shipping,
  total = round((t.subtotal - t.discount) + round((t.subtotal - t.discount) * 0.07, 2) + s.shipping, 2)
FROM totals t
JOIN shipping s ON s.order_id = t.order_id
WHERE o.id = t.order_id;

UPDATE orders
SET subtotal = 0, discount = 0, tax = 0, shipping = 0, total = 0
WHERE status = 'cancelled';

-- Payments
INSERT INTO payments (
  order_id,
  payment_ts,
  amount,
  method,
  status
)
SELECT
  o.id,
  o.order_ts + interval '1 hour',
  CASE WHEN o.status = 'cancelled' THEN 0 ELSE o.total END,
  o.payment_method,
  CASE
    WHEN o.status = 'cancelled' THEN 'failed'
    WHEN o.status = 'returned' THEN 'refunded'
    ELSE 'captured'
  END
FROM orders o;

-- Shipments for online orders
WITH params AS (
  SELECT
    ARRAY['UPS','FedEx','USPS','DHL'] AS carriers,
    ARRAY['ground','2-day','overnight'] AS levels
)
INSERT INTO shipments (
  order_id,
  carrier,
  service_level,
  shipped_ts,
  delivered_ts,
  shipping_cost
)
SELECT
  o.id,
  params.carriers[1 + floor(random() * array_length(params.carriers, 1))::int],
  params.levels[1 + floor(random() * array_length(params.levels, 1))::int],
  o.order_ts + interval '1 day',
  CASE WHEN o.status IN ('completed','returned') THEN o.order_ts + interval '4 days' ELSE NULL END,
  o.shipping
FROM orders o
CROSS JOIN params
WHERE o.channel = 'online' AND o.status IN ('completed','returned','shipped');

-- Returns
WITH params AS (
  SELECT ARRAY['damaged','wrong_item','late_delivery','changed_mind','size_issue'] AS reasons
)
INSERT INTO returns (
  order_id,
  return_ts,
  reason,
  refund_amount
)
SELECT
  o.id,
  o.order_ts + interval '7 days',
  params.reasons[1 + floor(random() * array_length(params.reasons, 1))::int],
  o.total
FROM orders o
CROSS JOIN params
WHERE o.status = 'returned';

-- Inventory for physical shops
INSERT INTO inventory (
  shop_id,
  product_id,
  on_hand,
  reorder_point
)
SELECT
  s.id,
  p.id,
  (floor(random() * 200) + 5)::int,
  25
FROM shops s
CROSS JOIN products p
WHERE random() < 0.4;
