import test from "node:test";
import assert from "node:assert/strict";

import {
  buildConnectorConfigValues,
  buildConnectorConnectionPayload,
  buildConnectorEditFormState,
  buildConnectorSubmitPayload,
  buildMetadataPayload,
  buildSecretReferencesPayload,
  connectorFamilyOptions,
  normalizeConnectorTypes,
} from "./connectorFormModel.js";

const postgresSchema = {
  connector_type: "POSTGRES",
  config: [
    { field: "host", label: "Host", type: "string", required: true },
    { field: "port", label: "Port", type: "number", required: true, default: "5432" },
    { field: "ssl", label: "Use SSL", type: "boolean", required: false, default: "false" },
    { field: "password", label: "Password", type: "password", required: true },
  ],
};

test("normalizeConnectorTypes and connectorFamilyOptions normalize catalog values", () => {
  const types = normalizeConnectorTypes([
    { name: "postgres", label: "Postgres", family: "DATABASE" },
    { name: "s3", label: "S3", family: "storage" },
  ]);

  assert.deepEqual(types.map((item) => item.value), ["POSTGRES", "S3"]);
  assert.deepEqual(connectorFamilyOptions(types), [
    { value: "database", label: "Database" },
    { value: "storage", label: "Storage" },
  ]);
});

test("buildConnectorConfigValues initializes from defaults and existing connection values", () => {
  const values = buildConnectorConfigValues(postgresSchema, { host: "db.local", ssl: true });

  assert.deepEqual(values, {
    host: "db.local",
    port: "5432",
    ssl: "true",
    password: "",
  });
});

test("buildConnectorConfigValues preserves connection values outside the published schema", () => {
  const values = buildConnectorConfigValues(postgresSchema, {
    host: "db.local",
    port: 5432,
    custom_option: { mode: "legacy" },
  });
  const payload = buildConnectorConnectionPayload(postgresSchema, {
    ...values,
    password: "secret",
  });

  assert.deepEqual(values.custom_option, { mode: "legacy" });
  assert.deepEqual(payload.custom_option, { mode: "legacy" });
});


test("buildConnectorConnectionPayload validates required fields and coerces values", () => {
  const payload = buildConnectorConnectionPayload(postgresSchema, {
    host: "db.local",
    port: "5433",
    ssl: "true",
    password: "secret",
  });

  assert.deepEqual(payload, {
    host: "db.local",
    port: 5433,
    ssl: true,
    password: "secret",
  });
});

test("buildConnectorConnectionPayload accepts secret references for required fields", () => {
  const payload = buildConnectorConnectionPayload(
    postgresSchema,
    { host: "db.local", port: "5432", ssl: "false", password: "" },
    [{ field: "password", provider_type: "env", identifier: "PG_PASSWORD" }],
  );

  assert.deepEqual(payload, {
    host: "db.local",
    port: 5432,
    ssl: false,
  });
});

test("buildConnectorConnectionPayload reports missing required fields", () => {
  assert.throws(
    () => buildConnectorConnectionPayload(postgresSchema, { host: "", port: "5432" }),
    /Complete the required fields: Host, Password/,
  );
});

test("buildMetadataPayload converts typed metadata rows", () => {
  const payload = buildMetadataPayload([
    { key: "warehouse", value: "analytics", valueType: "string" },
    { key: "max_rows", value: "100", valueType: "number" },
    { key: "enabled", value: "true", valueType: "boolean" },
  ]);

  assert.deepEqual(payload, {
    warehouse: "analytics",
    max_rows: 100,
    enabled: true,
  });
});

test("buildSecretReferencesPayload converts secret reference rows", () => {
  const payload = buildSecretReferencesPayload([
    {
      field: "password",
      provider_type: "env",
      identifier: "PG_PASSWORD",
      key: "",
      version: "",
    },
  ]);

  assert.deepEqual(payload, {
    password: {
      provider_type: "env",
      identifier: "PG_PASSWORD",
    },
  });
});

test("buildSecretReferencesPayload rejects duplicate target fields", () => {
  assert.throws(
    () =>
      buildSecretReferencesPayload([
        { field: "password", provider_type: "env", identifier: "ONE" },
        { field: "password", provider_type: "env", identifier: "TWO" },
      ]),
    /duplicated/,
  );
});

test("buildConnectorEditFormState reads live connector detail", () => {
  const form = buildConnectorEditFormState(
    {
      rawPayload: {
        name: "warehouse",
        connector_type: "POSTGRES",
        connector_family: "database",
        description: "Main warehouse",
        connection: { host: "db.local", port: 5432, ssl: false },
        metadata: { owner: "analytics" },
        secrets: { password: { provider_type: "env", identifier: "PG_PASSWORD" } },
      },
    },
    postgresSchema,
  );

  assert.equal(form.name, "warehouse");
  assert.equal(form.type, "POSTGRES");
  assert.equal(form.configValues.host, "db.local");
  assert.equal(form.metadataRows[0].key, "owner");
  assert.equal(form.secretRows[0].identifier, "PG_PASSWORD");
});

test("buildConnectorSubmitPayload creates API-compatible create and update payloads", () => {
  const createPayload = buildConnectorSubmitPayload({
    mode: "create",
    schema: postgresSchema,
    form: {
      name: "warehouse",
      type: "POSTGRES",
      description: "Main warehouse",
      configValues: { host: "db.local", port: "5432", ssl: "true", password: "" },
      metadataRows: [{ key: "owner", value: "analytics", valueType: "string" }],
      secretRows: [{ field: "password", provider_type: "env", identifier: "PG_PASSWORD" }],
    },
  });
  const updatePayload = buildConnectorSubmitPayload({
    mode: "edit",
    schema: postgresSchema,
    form: {
      description: "",
      configValues: { host: "db.local", port: "5432", ssl: "false", password: "direct" },
      metadataRows: [],
      secretRows: [],
    },
  });

  assert.equal(createPayload.name, "warehouse");
  assert.equal(createPayload.type, "POSTGRES");
  assert.deepEqual(createPayload.secrets.password, {
    provider_type: "env",
    identifier: "PG_PASSWORD",
  });
  assert.equal(updatePayload.name, undefined);
  assert.equal(updatePayload.type, undefined);
  assert.deepEqual(updatePayload.connection, {
    host: "db.local",
    port: 5432,
    ssl: false,
    password: "direct",
  });
});

test("buildConnectorSubmitPayload allows connector types without connection fields", () => {
  const payload = buildConnectorSubmitPayload({
    mode: "create",
    schema: { connector_type: "LOCAL", config: [] },
    form: {
      name: "local_files",
      type: "LOCAL",
      description: "",
      configValues: {},
      metadataRows: [],
      secretRows: [],
    },
  });

  assert.deepEqual(payload, {
    name: "local_files",
    type: "LOCAL",
    description: null,
    connection: {},
    metadata: {},
    secrets: {},
  });
});
