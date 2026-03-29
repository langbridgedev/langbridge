SELECT setseed(0.37);

-- Accounts
WITH params AS (
  SELECT
    ARRAY['North','South','East','West','Central','Coastal','Metro','Enterprise'] AS regions,
    ARRAY['startup','smb','mid_market','enterprise'] AS segments
)
INSERT INTO accounts (
  account_external_id,
  name,
  segment,
  owner_region,
  status,
  created_at
)
SELECT
  format('ACC-%06s', s),
  format('Account %s', s),
  params.segments[1 + floor(random() * array_length(params.segments, 1))::int],
  params.regions[1 + floor(random() * array_length(params.regions, 1))::int],
  CASE WHEN random() < 0.96 THEN 'active' ELSE 'inactive' END,
  now() - (random() * interval '1500 days')
FROM generate_series(1, 2500) s
CROSS JOIN params;

-- Contacts
WITH params AS (
  SELECT
    ARRAY['Alex','Jordan','Taylor','Morgan','Casey','Riley','Avery','Cameron','Quinn','Hayden','Parker','Drew','Sydney','Elliot','Reese','Logan','Rowan','Emery','Blake','Sawyer','Charlie','Dakota','Jamie','Kendall'] AS first_names,
    ARRAY['Smith','Johnson','Williams','Brown','Jones','Miller','Davis','Garcia','Rodriguez','Wilson','Martinez','Anderson','Taylor','Thomas','Hernandez','Moore','Martin','Lee','Perez','Thompson','White','Harris','Sanchez','Clark'] AS last_names,
    ARRAY['email','phone','chat'] AS channels
)
INSERT INTO contacts (
  contact_external_id,
  account_id,
  first_name,
  last_name,
  email,
  lifecycle_stage,
  lead_score,
  preferred_channel,
  marketing_opt_in,
  last_touch_at
)
SELECT
  format('CRM-%08s', s),
  1 + ((s - 1) % 2500),
  params.first_names[1 + floor(random() * array_length(params.first_names, 1))::int],
  params.last_names[1 + floor(random() * array_length(params.last_names, 1))::int],
  format('crm.contact.%05s@example.com', s),
  CASE
    WHEN random() < 0.10 THEN 'lead'
    WHEN random() < 0.25 THEN 'prospect'
    WHEN random() < 0.85 THEN 'customer'
    ELSE 'churn_risk'
  END,
  floor(10 + random() * 90)::int,
  params.channels[1 + floor(random() * array_length(params.channels, 1))::int],
  random() < 0.65,
  now() - (random() * interval '120 days')
FROM generate_series(1, 10000) s
CROSS JOIN params;

-- Opportunities
INSERT INTO opportunities (
  account_id,
  contact_external_id,
  stage,
  amount,
  expected_close_date,
  is_closed
)
SELECT
  1 + floor(random() * 2500)::int,
  CASE WHEN random() < 0.85 THEN format('CRM-%08s', 1 + floor(random() * 10000)::int) ELSE NULL END,
  CASE
    WHEN random() < 0.20 THEN 'qualification'
    WHEN random() < 0.45 THEN 'discovery'
    WHEN random() < 0.65 THEN 'proposal'
    WHEN random() < 0.80 THEN 'negotiation'
    WHEN random() < 0.90 THEN 'closed_won'
    ELSE 'closed_lost'
  END,
  round((random() * 100000 + 500)::numeric, 2),
  current_date + (floor(random() * 180)::int - 60),
  random() < 0.30
FROM generate_series(1, 4500);

-- Support cases
WITH params AS (
  SELECT
    ARRAY['billing','delivery','returns','account_access','product_quality'] AS categories,
    ARRAY['low','medium','high','urgent'] AS priorities
)
INSERT INTO support_cases (
  contact_external_id,
  opened_at,
  closed_at,
  priority,
  status,
  category,
  csat_score
)
SELECT
  base.contact_external_id,
  base.opened_at,
  CASE WHEN random() < 0.82 THEN base.opened_at + (random() * interval '14 days') ELSE NULL END,
  params.priorities[1 + floor(random() * array_length(params.priorities, 1))::int],
  CASE
    WHEN random() < 0.75 THEN 'closed'
    WHEN random() < 0.92 THEN 'in_progress'
    ELSE 'new'
  END,
  params.categories[1 + floor(random() * array_length(params.categories, 1))::int],
  CASE WHEN random() < 0.70 THEN (1 + floor(random() * 5))::int ELSE NULL END
FROM (
  SELECT
    format('CRM-%08s', 1 + floor(random() * 10000)::int) AS contact_external_id,
    now() - (random() * interval '365 days') AS opened_at
  FROM generate_series(1, 7000)
) base
CROSS JOIN params;
