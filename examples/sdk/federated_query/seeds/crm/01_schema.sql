BEGIN;

DROP TABLE IF EXISTS support_cases CASCADE;
DROP TABLE IF EXISTS opportunities CASCADE;
DROP TABLE IF EXISTS contacts CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;

CREATE TABLE accounts (
  id BIGSERIAL PRIMARY KEY,
  account_external_id TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  segment TEXT NOT NULL,
  owner_region TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE contacts (
  id BIGSERIAL PRIMARY KEY,
  contact_external_id TEXT NOT NULL UNIQUE,
  account_id BIGINT NOT NULL REFERENCES accounts(id),
  first_name TEXT NOT NULL,
  last_name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  lifecycle_stage TEXT NOT NULL,
  lead_score INT NOT NULL,
  preferred_channel TEXT NOT NULL,
  marketing_opt_in BOOLEAN NOT NULL DEFAULT false,
  last_touch_at TIMESTAMPTZ
);

CREATE TABLE opportunities (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id),
  contact_external_id TEXT,
  stage TEXT NOT NULL,
  amount NUMERIC(12,2) NOT NULL,
  expected_close_date DATE,
  is_closed BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE support_cases (
  id BIGSERIAL PRIMARY KEY,
  contact_external_id TEXT NOT NULL,
  opened_at TIMESTAMPTZ NOT NULL,
  closed_at TIMESTAMPTZ,
  priority TEXT NOT NULL,
  status TEXT NOT NULL,
  category TEXT NOT NULL,
  csat_score INT
);

CREATE INDEX idx_contacts_external_id ON contacts(contact_external_id);
CREATE INDEX idx_contacts_account_id ON contacts(account_id);
CREATE INDEX idx_opportunities_account_id ON opportunities(account_id);
CREATE INDEX idx_support_cases_contact_external_id ON support_cases(contact_external_id);
CREATE INDEX idx_support_cases_opened_at ON support_cases(opened_at);

COMMIT;
