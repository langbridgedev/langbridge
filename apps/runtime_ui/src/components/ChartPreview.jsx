import { resolveChartDataKey } from "../lib/chartFieldMapping";
import { formatValue } from "../lib/format";

function toColumnName(column, index) {
  if (typeof column === "string" && column.trim()) {
    return column;
  }
  if (column && typeof column === "object") {
    const candidate = column.name || column.key || column.label;
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate;
    }
  }
  return `Column ${index + 1}`;
}

function toRecords(result) {
  const columns = Array.isArray(result?.columns)
    ? result.columns.map((column, index) => toColumnName(column, index))
    : [];
  const rows = Array.isArray(result?.rows) ? result.rows : [];
  return rows.map((row) => {
    if (Array.isArray(row)) {
      return columns.reduce((accumulator, column, index) => {
        accumulator[column] = row[index];
        return accumulator;
      }, {});
    }
    if (row && typeof row === "object") {
      return row;
    }
    return columns.length > 0 ? { [columns[0]]: row } : { value: row };
  });
}

function toNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const normalized = value.trim().replaceAll(",", "").replaceAll("%", "");
    if (!normalized) {
      return null;
    }
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function inferDimensionKey({ records, metadata, preferredDimension, excludeKey }) {
  const rowKeys = records[0] ? Object.keys(records[0]) : [];
  const fallbackKey = rowKeys.find((key) => toNumber(records[0]?.[key]) === null) || rowKeys[0];
  return resolveChartDataKey({
    selectedKey: preferredDimension,
    rowKeys,
    metadata,
    fallbackKey,
    excludeKey,
  });
}

function inferMeasureKey({ records, metadata, preferredMeasure }) {
  const rowKeys = records[0] ? Object.keys(records[0]) : [];
  const fallbackKey =
    rowKeys.find((key) => {
      const firstNumeric = toNumber(records[0]?.[key]);
      return firstNumeric !== null;
    }) || rowKeys[0];
  return resolveChartDataKey({
    selectedKey: preferredMeasure,
    rowKeys,
    metadata,
    fallbackKey,
  });
}

function normalizeChartType(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) {
    return "bar";
  }
  if (["bar", "line", "pie"].includes(normalized)) {
    return normalized;
  }
  return "bar";
}

function BarChart({ points, dimensionKey, measureKey }) {
  const maxValue = Math.max(...points.map((point) => point.value), 1);
  return (
    <div className="mini-chart-list">
      {points.map((point) => (
        <div key={point.label} className="mini-chart-item">
          <div className="mini-chart-copy">
            <strong>{point.label}</strong>
            <span>{formatValue(point.value)}</span>
          </div>
          <div className="mini-chart-bar">
            <div
              className="mini-chart-bar-fill"
              style={{ width: `${Math.max(8, (point.value / maxValue) * 100)}%` }}
            />
          </div>
        </div>
      ))}
      <p className="mini-chart-caption">
        {dimensionKey} by {measureKey}
      </p>
    </div>
  );
}

function PieChart({ points, measureKey }) {
  const total = points.reduce((sum, point) => sum + point.value, 0) || 1;
  return (
    <div className="mini-chart-list">
      {points.map((point) => (
        <div key={point.label} className="mini-chart-item">
          <div className="mini-chart-copy">
            <strong>{point.label}</strong>
            <span>
              {formatValue(point.value)} ({Math.round((point.value / total) * 100)}%)
            </span>
          </div>
          <div className="mini-chart-bar">
            <div
              className="mini-chart-bar-fill alt"
              style={{ width: `${Math.max(8, (point.value / total) * 100)}%` }}
            />
          </div>
        </div>
      ))}
      <p className="mini-chart-caption">Distribution by {measureKey}</p>
    </div>
  );
}

function LineChart({ points, dimensionKey, measureKey }) {
  const width = Math.max((points.length - 1) * 36 + 24, 60);
  const height = 78;
  const maxValue = Math.max(...points.map((point) => point.value), 1);
  const plotted = points.map((point, index) => {
    const x = points.length === 1 ? width / 2 : 12 + (index * (width - 24)) / (points.length - 1);
    const y = height - 14 - (point.value / maxValue) * (height - 28);
    return {
      ...point,
      x,
      y,
    };
  });

  return (
    <div className="mini-chart-line-shell">
      <svg className="mini-chart-line" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
        <polyline
          points={plotted.map((point) => `${point.x},${point.y}`).join(" ")}
          fill="none"
          stroke="currentColor"
          strokeWidth="2.5"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        {plotted.map((point) => (
          <circle key={point.label} cx={point.x} cy={point.y} r="3.5" fill="currentColor" />
        ))}
      </svg>
      <div className="mini-chart-line-labels">
        {points.map((point) => (
          <div key={point.label}>
            <strong>{point.label}</strong>
            <span>{formatValue(point.value)}</span>
          </div>
        ))}
      </div>
      <p className="mini-chart-caption">
        {dimensionKey} trend by {measureKey}
      </p>
    </div>
  );
}

export function ChartPreview({
  title,
  result,
  metadata = [],
  visualization = {},
  preferredDimension,
  preferredMeasure,
  themeColors = [],
}) {
  const records = toRecords(result);
  if (records.length === 0) {
    return <div className="empty-box">Run a query to render a lightweight chart preview.</div>;
  }

  const chartType = normalizeChartType(visualization?.chartType || visualization?.chart_type);
  const measureKey = inferMeasureKey({
    records,
    metadata,
    preferredMeasure:
      Array.isArray(visualization?.y) && visualization.y.length > 0
        ? visualization.y[0]
        : visualization?.y || preferredMeasure,
  });
  const dimensionKey = inferDimensionKey({
    records,
    metadata,
    preferredDimension: visualization?.x || preferredDimension,
    excludeKey: measureKey,
  });

  if (!measureKey || !dimensionKey) {
    return <div className="empty-box">This result does not have enough structured fields for a chart preview.</div>;
  }

  const points = records
    .map((record) => {
      const rawValue = toNumber(record[measureKey]);
      const label = record[dimensionKey];
      if (rawValue === null || label === null || label === undefined) {
        return null;
      }
      return {
        label: String(label),
        value: rawValue,
      };
    })
    .filter(Boolean)
    .slice(0, 8);

  if (points.length === 0) {
    return <div className="empty-box">No numeric rows were available for a chart preview.</div>;
  }

  return (
    <div
      className="chart-panel"
      style={{
        "--chart-primary": themeColors[0] || undefined,
        "--chart-secondary": themeColors[1] || undefined,
      }}
    >
      <div className="chart-panel-header">
        <h3>{title || "Chart preview"}</h3>
        <span className="chart-kind">{chartType}</span>
      </div>
      {chartType === "pie" ? (
        <PieChart points={points} measureKey={measureKey} />
      ) : chartType === "line" ? (
        <LineChart points={points} dimensionKey={dimensionKey} measureKey={measureKey} />
      ) : (
        <BarChart points={points} dimensionKey={dimensionKey} measureKey={measureKey} />
      )}
    </div>
  );
}
