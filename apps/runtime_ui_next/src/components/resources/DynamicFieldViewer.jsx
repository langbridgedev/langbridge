const MAX_VISIBLE_ARRAY_ITEMS = 8;

export function DynamicFieldValue({ value, depth = 0 }) {
  const parsedValue = parseJsonLikeValue(value);

  if (parsedValue === undefined || parsedValue === null || parsedValue === "") {
    return <span className="dynamic-field-empty">n/a</span>;
  }

  if (typeof parsedValue === "boolean") {
    return <span className="dynamic-field-badge">{parsedValue ? "Yes" : "No"}</span>;
  }

  if (typeof parsedValue === "number") {
    return <span className="dynamic-field-primitive">{parsedValue.toLocaleString()}</span>;
  }

  if (typeof parsedValue === "string") {
    return (
      <span className={parsedValue.length > 96 ? "dynamic-field-primitive long" : "dynamic-field-primitive"}>
        {parsedValue}
      </span>
    );
  }

  if (Array.isArray(parsedValue)) {
    return <DynamicArrayValue value={parsedValue} depth={depth} />;
  }

  return <DynamicObjectValue value={parsedValue} depth={depth} />;
}

function DynamicObjectValue({ value, depth }) {
  const entries = Object.entries(value || {}).filter(([, entryValue]) => entryValue !== undefined);
  if (entries.length === 0) {
    return <span className="dynamic-field-empty">Empty object</span>;
  }

  if (depth >= 2) {
    return (
      <div className="dynamic-field-compact">
        {entries.slice(0, 6).map(([key, entryValue]) => (
          <span key={key}>
            <strong>{formatLabel(key)}</strong>
            {formatCompactValue(entryValue)}
          </span>
        ))}
      </div>
    );
  }

  return (
    <div className={`dynamic-object-viewer depth-${depth}`}>
      {entries.map(([key, entryValue]) => (
        <div className={`dynamic-object-row depth-${depth}`} key={key}>
          <span>{formatLabel(key)}</span>
          <DynamicFieldValue value={entryValue} depth={depth + 1} />
        </div>
      ))}
    </div>
  );
}

function DynamicArrayValue({ value, depth }) {
  if (value.length === 0) {
    return <span className="dynamic-field-empty">Empty list</span>;
  }

  const primitive = value.every((item) => item === null || ["string", "number", "boolean"].includes(typeof item));
  if (primitive) {
    const hasLongValues = value.some((item) => String(item ?? "").length > 48);
    if (hasLongValues) {
      return (
        <div className="dynamic-list-viewer">
          {value.slice(0, MAX_VISIBLE_ARRAY_ITEMS).map((item, index) => (
            <div className="dynamic-list-row" key={`${String(item)}-${index}`}>
              <span>Item {index + 1}</span>
              <DynamicFieldValue value={item} depth={depth + 1} />
            </div>
          ))}
          {value.length > MAX_VISIBLE_ARRAY_ITEMS ? (
            <span className="dynamic-field-empty">Showing {MAX_VISIBLE_ARRAY_ITEMS} of {value.length} items</span>
          ) : null}
        </div>
      );
    }
    return (
      <div className="dynamic-chip-list">
        {value.slice(0, MAX_VISIBLE_ARRAY_ITEMS).map((item, index) => (
          <span key={`${String(item)}-${index}`}>{formatCompactValue(item)}</span>
        ))}
        {value.length > MAX_VISIBLE_ARRAY_ITEMS ? <span>+{value.length - MAX_VISIBLE_ARRAY_ITEMS} more</span> : null}
      </div>
    );
  }

  return (
    <div className={`dynamic-array-viewer depth-${depth}`}>
      {value.slice(0, MAX_VISIBLE_ARRAY_ITEMS).map((item, index) => (
        <div className="dynamic-array-card" key={index}>
          <strong>Item {index + 1}</strong>
          <DynamicFieldValue value={item} depth={depth + 1} />
        </div>
      ))}
      {value.length > MAX_VISIBLE_ARRAY_ITEMS ? (
        <span className="dynamic-field-empty">Showing {MAX_VISIBLE_ARRAY_ITEMS} of {value.length} items</span>
      ) : null}
    </div>
  );
}

function parseJsonLikeValue(value) {
  if (typeof value !== "string") {
    return value;
  }
  const trimmed = value.trim();
  if (!trimmed || !["{", "["].includes(trimmed[0])) {
    return value;
  }
  try {
    return JSON.parse(trimmed);
  } catch {
    return value;
  }
}

function formatLabel(value) {
  return String(value || "")
    .replace(/[_-]+/g, " ")
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^./, (match) => match.toUpperCase());
}

function formatCompactValue(value) {
  if (value === undefined || value === null || value === "") {
    return "n/a";
  }
  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }
  if (typeof value === "number") {
    return value.toLocaleString();
  }
  if (typeof value === "object") {
    return Array.isArray(value) ? `${value.length} items` : `${Object.keys(value).length} fields`;
  }
  return String(value);
}
