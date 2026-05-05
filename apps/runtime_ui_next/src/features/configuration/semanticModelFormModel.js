import { createLocalId } from "../../lib/runtimeUi.js";

export const SEMANTIC_DIMENSION_TYPES = ["string", "integer", "number", "boolean", "date", "time"];
export const SEMANTIC_MEASURE_TYPES = ["number", "integer", "boolean", "date", "time", "string"];
export const SEMANTIC_MEASURE_AGGREGATIONS = ["sum", "avg", "count", "count_distinct", "min", "max"];
export const SEMANTIC_RELATIONSHIP_TYPES = [
  "many_to_one",
  "one_to_many",
  "one_to_one",
  "many_to_many",
  "left",
  "inner",
];

export function buildSemanticModelCreateFormState() {
  return {
    name: "",
    description: "",
    sqlInstructions: "",
    semanticDatasets: [],
    metrics: [],
    relationships: [],
    unsupported: false,
  };
}

export function buildSemanticModelEditFormState(resource, datasetOptions = []) {
  const raw = resource?.rawPayload || resource || {};
  const model = semanticModelPayload(raw);
  if (isSemanticGraphModel(model)) {
    return {
      ...buildSemanticModelCreateFormState(),
      name: String(raw.name || model?.name || resource?.name || ""),
      description: String(raw.description || model?.description || ""),
      sqlInstructions: extractSqlInstructions(model, raw),
      unsupported: true,
      unsupportedReason: "Semantic graph models are inspect-only in the guided editor.",
    };
  }

  const datasetNames = Array.isArray(raw.dataset_names) ? [...raw.dataset_names] : [];
  const remainingDatasetNames = [...datasetNames];
  const semanticDatasets = Object.entries(readDatasetMap(model)).map(([semanticKey, value]) => {
    const datasetValue = value && typeof value === "object" ? value : {};
    const relationName = String(datasetValue.relation_name || datasetValue.relationName || "").trim();
    let matchedDataset = findDatasetOption(datasetOptions, semanticKey, relationName);
    if (!matchedDataset && remainingDatasetNames.includes(semanticKey)) {
      matchedDataset = findDatasetOption(datasetOptions, semanticKey, "");
    }
    if (!matchedDataset && remainingDatasetNames.length === 1) {
      matchedDataset = findDatasetOption(datasetOptions, remainingDatasetNames[0], "");
    }

    const matchedDatasetName = String(matchedDataset?.name || "").trim();
    if (matchedDatasetName) {
      const matchedIndex = remainingDatasetNames.findIndex(
        (item) => String(item || "").trim() === matchedDatasetName,
      );
      if (matchedIndex >= 0) {
        remainingDatasetNames.splice(matchedIndex, 1);
      }
    }

    return {
      id: createLocalId("semantic-dataset"),
      sourceDatasetName: matchedDatasetName || String(datasetValue.dataset || semanticKey).trim(),
      sourceDatasetLabel: String(matchedDataset?.label || matchedDataset?.name || semanticKey).trim(),
      semanticKey,
      relationName: relationName || String(matchedDataset?.sql_alias || matchedDataset?.name || semanticKey).trim(),
      description: String(datasetValue.description || "").trim(),
      dimensions: normalizeFieldList(datasetValue.dimensions).map((item) => buildDimensionField(item)),
      measures: normalizeFieldList(datasetValue.measures).map((item) => buildMeasureField(item)),
    };
  });

  return {
    name: String(raw.name || model?.name || resource?.name || "").trim(),
    description: String(raw.description || model?.description || "").trim(),
    sqlInstructions: extractSqlInstructions(model, raw),
    semanticDatasets,
    metrics: normalizeMetricList(model.metrics),
    relationships: Array.isArray(model.relationships)
      ? model.relationships.map((item) => buildRelationship(item))
      : [],
    unsupported: false,
  };
}

export function normalizeSemanticDatasetOptions(items) {
  return (Array.isArray(items) ? items : [])
    .map((item) => {
      const name = String(item?.name || item?.id || "").trim();
      if (!name) {
        return null;
      }
      return {
        ...item,
        id: item?.id || name,
        name,
        value: name,
        label: item?.label || name,
        columns: normalizeColumns(item?.columns || item?.schema_hint?.columns || item?.fields),
      };
    })
    .filter(Boolean)
    .sort((left, right) => left.label.localeCompare(right.label));
}

export function buildSemanticDatasetFromRuntimeDataset(datasetDetail, existingKeys = []) {
  const datasetName = String(datasetDetail?.name || "").trim();
  const label = String(datasetDetail?.label || datasetName).trim() || datasetName;
  const relationName = String(datasetDetail?.table_name || datasetDetail?.sql_alias || datasetName).trim() || datasetName;
  const semanticKey = uniqueSemanticKey(datasetName || relationName || label, existingKeys);
  const dimensions = [];
  const measures = [];

  normalizeColumns(datasetDetail?.columns || datasetDetail?.schema_hint?.columns || datasetDetail?.fields).forEach((column) => {
    const columnName = String(column?.name || "").trim();
    if (!columnName) {
      return;
    }
    const primaryKey = inferPrimaryKey(columnName, datasetName);
    const idLike = columnName === "id" || columnName.endsWith("_id");
    const normalizedType = normalizeSemanticFieldType(column?.data_type || column?.type, { preferMeasure: true });
    const numericField = normalizedType === "integer" || normalizedType === "number";

    if (numericField && !primaryKey && !idLike) {
      measures.push({
        id: createLocalId("semantic-measure"),
        name: columnName,
        expression: columnName,
        type: "number",
        aggregation: "sum",
      });
      return;
    }

    dimensions.push({
      id: createLocalId("semantic-dimension"),
      name: columnName,
      expression: columnName,
      type: normalizeSemanticFieldType(column?.data_type || column?.type),
      primaryKey,
    });
  });

  return {
    id: createLocalId("semantic-dataset"),
    sourceDatasetName: datasetName,
    sourceDatasetLabel: label,
    semanticKey,
    relationName,
    description: "",
    dimensions,
    measures,
  };
}

export function createEmptySemanticDimension() {
  return {
    id: createLocalId("semantic-dimension"),
    name: "",
    expression: "",
    type: "string",
    primaryKey: false,
  };
}

export function createEmptySemanticMeasure() {
  return {
    id: createLocalId("semantic-measure"),
    name: "",
    expression: "",
    type: "number",
    aggregation: "sum",
  };
}

export function createEmptySemanticRelationship(datasetKeys = []) {
  const [firstKey = "", secondKey = ""] = Array.isArray(datasetKeys) ? datasetKeys : [];
  return {
    id: createLocalId("semantic-relationship"),
    name: firstKey && secondKey ? `${firstKey}_to_${secondKey}` : "",
    sourceDataset: firstKey,
    sourceField: "",
    targetDataset: secondKey,
    targetField: "",
    type: "many_to_one",
  };
}

export function createEmptySemanticMetric() {
  return {
    id: createLocalId("semantic-metric"),
    name: "",
    expression: "",
    description: "",
  };
}

export function buildSemanticModelDefinition(form) {
  const semanticDatasets = Array.isArray(form?.semanticDatasets) ? form.semanticDatasets : [];
  const metrics = Array.isArray(form?.metrics) ? form.metrics : [];
  const metricsPayload = buildMetricsPayload(metrics);
  const relationships = Array.isArray(form?.relationships) ? form.relationships : [];
  return {
    version: "1",
    name: requiredText(form?.name, "Semantic model name is required."),
    description: nullableText(form?.description) || undefined,
    sql_instructions: nullableText(form?.sqlInstructions) || undefined,
    datasets: Object.fromEntries(
      semanticDatasets.map((dataset) => [
        requiredText(dataset?.semanticKey, "Each semantic dataset requires a semantic key."),
        {
          relation_name:
            requiredText(
              dataset?.relationName || dataset?.sourceDatasetName || dataset?.semanticKey,
              "Each semantic dataset requires a relation name.",
            ),
          description: nullableText(dataset?.description) || undefined,
          dimensions: (Array.isArray(dataset?.dimensions) ? dataset.dimensions : [])
            .filter((field) => String(field?.name || "").trim())
            .map((field) => ({
              name: String(field.name).trim(),
              expression: String(field.expression || field.name).trim(),
              type: String(field.type || "string").trim() || "string",
              primary_key: Boolean(field.primaryKey),
            })),
          measures: (Array.isArray(dataset?.measures) ? dataset.measures : [])
            .filter((field) => String(field?.name || "").trim())
            .map((field) => ({
              name: String(field.name).trim(),
              expression: String(field.expression || field.name).trim(),
              type: String(field.type || "number").trim() || "number",
              aggregation: String(field.aggregation || "sum").trim() || "sum",
            })),
        },
      ]),
    ),
    metrics: Object.keys(metricsPayload).length > 0 ? metricsPayload : undefined,
    relationships: relationships
      .filter(isCompleteRelationship)
      .map((relationship) => ({
        name: String(relationship.name).trim(),
        source_dataset: String(relationship.sourceDataset).trim(),
        source_field: String(relationship.sourceField).trim(),
        target_dataset: String(relationship.targetDataset).trim(),
        target_field: String(relationship.targetField).trim(),
        type: String(relationship.type || "many_to_one").trim() || "many_to_one",
      })),
  };
}

export function buildSemanticModelSubmitPayload({ mode, form }) {
  if (form?.unsupported) {
    throw new Error("This semantic model shape is inspect-only in the guided editor.");
  }
  const isCreate = mode === "create";
  const name = requiredText(form?.name, "Semantic model name is required.");
  const semanticDatasets = Array.isArray(form?.semanticDatasets) ? form.semanticDatasets : [];
  if (semanticDatasets.length === 0) {
    throw new Error("Select at least one dataset for the semantic model.");
  }

  const datasetNames = semanticDatasets
    .map((dataset) => String(dataset?.sourceDatasetName || "").trim())
    .filter(Boolean);
  if (datasetNames.length === 0) {
    throw new Error("Selected semantic datasets must be bound to runtime datasets.");
  }

  const semanticKeys = semanticDatasets.map((dataset) => String(dataset?.semanticKey || "").trim());
  if (semanticKeys.some((key) => !key)) {
    throw new Error("Each semantic dataset requires a semantic key.");
  }
  if (new Set(semanticKeys).size !== semanticKeys.length) {
    throw new Error("Semantic dataset keys must be unique.");
  }

  const metricNames = (Array.isArray(form?.metrics) ? form.metrics : [])
    .map((metric) => String(metric?.name || "").trim())
    .filter(Boolean);
  if (new Set(metricNames).size !== metricNames.length) {
    throw new Error("Metric names must be unique.");
  }

  if ((Array.isArray(form?.relationships) ? form.relationships : []).some(isIncompleteRelationship)) {
    throw new Error("Complete or remove unfinished relationships before saving.");
  }

  const payload = {
    description: nullableText(form?.description),
    datasets: Array.from(new Set(datasetNames)),
    model: buildSemanticModelDefinition({ ...form, name }),
  };
  if (isCreate) {
    payload.name = name;
  }
  return compactPayload(payload);
}

export function semanticModelStats(formOrResource) {
  const form = Array.isArray(formOrResource?.semanticDatasets)
    ? formOrResource
    : buildSemanticModelEditFormState(formOrResource);
  const semanticDatasets = Array.isArray(form.semanticDatasets) ? form.semanticDatasets : [];
  return {
    datasets: semanticDatasets.length,
    dimensions: semanticDatasets.reduce((total, dataset) => total + (dataset.dimensions || []).length, 0),
    measures: semanticDatasets.reduce((total, dataset) => total + (dataset.measures || []).length, 0),
    metrics: (Array.isArray(form.metrics) ? form.metrics : []).length,
    relationships: (Array.isArray(form.relationships) ? form.relationships : []).length,
  };
}

export function isUnsupportedSemanticModel(resource) {
  return isSemanticGraphModel(semanticModelPayload(resource?.rawPayload || resource || {}));
}

export function sanitizeSemanticKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

export function normalizeSemanticFieldType(value, { preferMeasure = false } = {}) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) {
    return preferMeasure ? "number" : "string";
  }
  if (raw.includes("bool")) {
    return "boolean";
  }
  if (raw.includes("date") || raw.includes("time")) {
    return raw.includes("date") && !raw.includes("time") ? "date" : "time";
  }
  if (raw.includes("int")) {
    return "integer";
  }
  if (
    raw.includes("decimal") ||
    raw.includes("numeric") ||
    raw.includes("number") ||
    raw.includes("float") ||
    raw.includes("double") ||
    raw.includes("real") ||
    raw.includes("money")
  ) {
    return "number";
  }
  return preferMeasure ? "number" : "string";
}

function buildDimensionField(field = {}) {
  return {
    id: createLocalId("semantic-dimension"),
    name: String(field.name || "").trim(),
    expression: String(field.expression || "").trim(),
    type: String(field.type || "string").trim() || "string",
    primaryKey: Boolean(field.primary_key ?? field.primaryKey),
  };
}

function buildMeasureField(field = {}) {
  return {
    id: createLocalId("semantic-measure"),
    name: String(field.name || "").trim(),
    expression: String(field.expression || "").trim(),
    type: String(field.type || "number").trim() || "number",
    aggregation: String(field.aggregation || "sum").trim() || "sum",
  };
}

function buildRelationship(relationship = {}) {
  let sourceDataset = String(
    relationship.source_dataset || relationship.sourceDataset || relationship.from || relationship.from_ || "",
  ).trim();
  let sourceField = String(relationship.source_field || relationship.sourceField || "").trim();
  let targetDataset = String(relationship.target_dataset || relationship.targetDataset || relationship.to || "").trim();
  let targetField = String(relationship.target_field || relationship.targetField || "").trim();
  const joinExpression = String(relationship.join_on || relationship.on || "").trim();

  if ((!sourceField || !targetField) && joinExpression.includes("=")) {
    const [left, right] = joinExpression.split("=").map((item) => item.trim());
    const [leftDataset, leftField] = left.split(".");
    const [rightDataset, rightField] = right.split(".");
    sourceDataset ||= String(leftDataset || "").trim();
    sourceField ||= String(leftField || "").trim();
    targetDataset ||= String(rightDataset || "").trim();
    targetField ||= String(rightField || "").trim();
  }

  return {
    id: createLocalId("semantic-relationship"),
    name: String(relationship.name || "").trim(),
    sourceDataset,
    sourceField,
    targetDataset,
    targetField,
    type: String(relationship.type || "many_to_one").trim() || "many_to_one",
  };
}

function buildMetric(metricKey, metric = {}) {
  return {
    id: createLocalId("semantic-metric"),
    name: String(metric.name || metricKey || "").trim(),
    expression: String(metric.expression || "").trim(),
    description: String(metric.description || "").trim(),
  };
}

function normalizeMetricList(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return [];
  }
  return Object.entries(value).map(([key, metric]) =>
    buildMetric(key, metric && typeof metric === "object" ? metric : {}),
  );
}

function buildMetricsPayload(metrics) {
  return Object.fromEntries(
    (Array.isArray(metrics) ? metrics : [])
      .filter((metric) => String(metric?.name || "").trim())
      .map((metric) => [
        String(metric.name).trim(),
        {
          expression: requiredText(metric.expression, "Each metric requires an expression."),
          description: nullableText(metric.description) || undefined,
        },
      ]),
  );
}

function extractSqlInstructions(model = {}, raw = {}) {
  const direct = String(model?.sql_instructions || raw?.sql_instructions || "").trim();
  if (direct) {
    return direct;
  }
  return extractOrchestrationInstructions(model?.orchestration || raw?.orchestration);
}

function extractOrchestrationInstructions(orchestration) {
  if (!orchestration || typeof orchestration !== "object" || Array.isArray(orchestration)) {
    return "";
  }
  return (Array.isArray(orchestration.steps) ? orchestration.steps : [])
    .map((step) => {
      if (!step || typeof step !== "object" || Array.isArray(step)) {
        return "";
      }
      return String(step.instructions || step.sql_instructions || "").trim();
    })
    .filter(Boolean)
    .join("\n\n");
}

function semanticModelPayload(raw) {
  return (
    parseObjectLike(raw?.content_json) ||
    parseObjectLike(raw?.model) ||
    parseObjectLike(raw?.definition) ||
    {}
  );
}

function readDatasetMap(model) {
  if (model?.datasets && typeof model.datasets === "object" && !Array.isArray(model.datasets)) {
    return model.datasets;
  }
  if (model?.tables && typeof model.tables === "object" && !Array.isArray(model.tables)) {
    return model.tables;
  }
  return {};
}

function isSemanticGraphModel(model) {
  return Boolean(model?.source_models || model?.sourceModels || model?.semantic_models);
}

function findDatasetOption(datasetOptions, semanticKey, relationName) {
  const options = Array.isArray(datasetOptions) ? datasetOptions : [];
  const normalizedKey = String(semanticKey || "").trim().toLowerCase();
  const normalizedRelation = String(relationName || "").trim().toLowerCase();
  return (
    options.find((item) => String(item?.name || "").trim().toLowerCase() === normalizedKey) ||
    options.find((item) => String(item?.sql_alias || "").trim().toLowerCase() === normalizedRelation) ||
    options.find((item) => String(item?.name || "").trim().toLowerCase() === normalizedRelation) ||
    null
  );
}

function normalizeFieldList(value) {
  if (Array.isArray(value)) {
    return value.filter(Boolean);
  }
  if (value && typeof value === "object") {
    return Object.entries(value).map(([key, item]) => (item && typeof item === "object" ? { name: key, ...item } : { name: key }));
  }
  return [];
}

function normalizeColumns(value) {
  return (Array.isArray(value) ? value : [])
    .map((column) => {
      if (typeof column === "string") {
        return { name: column };
      }
      if (!column || typeof column !== "object") {
        return null;
      }
      const name = String(column.name || column.label || column.key || column.field || "").trim();
      return name ? { ...column, name } : null;
    })
    .filter(Boolean);
}

function inferPrimaryKey(columnName, datasetName) {
  const datasetRoot = String(datasetName || "").trim().toLowerCase().replace(/[^a-z0-9]/g, "");
  const normalizedColumn = String(columnName || "").trim().toLowerCase();
  return normalizedColumn === "id" || normalizedColumn === `${datasetRoot}id` || normalizedColumn === `${datasetRoot}_id`;
}

function uniqueSemanticKey(value, existingKeys) {
  const root = sanitizeSemanticKey(value) || "dataset";
  let nextValue = root;
  let counter = 2;
  const normalizedExisting = new Set(
    (Array.isArray(existingKeys) ? existingKeys : []).map((item) => sanitizeSemanticKey(item)).filter(Boolean),
  );
  while (normalizedExisting.has(nextValue)) {
    nextValue = `${root}_${counter}`;
    counter += 1;
  }
  return nextValue;
}

function isCompleteRelationship(relationship) {
  return [
    relationship?.name,
    relationship?.sourceDataset,
    relationship?.sourceField,
    relationship?.targetDataset,
    relationship?.targetField,
  ].every((value) => String(value || "").trim());
}

function isIncompleteRelationship(relationship) {
  const values = [
    relationship?.name,
    relationship?.sourceDataset,
    relationship?.sourceField,
    relationship?.targetDataset,
    relationship?.targetField,
  ].map((value) => String(value || "").trim());
  const filled = values.filter(Boolean).length;
  return filled > 0 && filled < values.length;
}

function requiredText(value, message) {
  const text = String(value || "").trim();
  if (!text) {
    throw new Error(message);
  }
  return text;
}

function nullableText(value) {
  const text = String(value || "").trim();
  return text || null;
}

function parseObjectLike(value) {
  if (!value) {
    return null;
  }
  if (typeof value === "object") {
    return value;
  }
  if (typeof value !== "string") {
    return null;
  }
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

function compactPayload(payload) {
  return Object.fromEntries(
    Object.entries(payload).filter(([, value]) => value !== undefined),
  );
}
