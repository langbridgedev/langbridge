import { useId, useState } from "react";

import { resolveChartDataKey } from "../lib/chartFieldMapping";
import { formatValue } from "../lib/format";
import { normalizeChartType } from "../lib/runtimeUi";

const DEFAULT_PALETTE = [
  "#d97706",
  "#dc2626",
  "#7c3aed",
  "#0891b2",
  "#16a34a",
  "#f59e0b",
];

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
    const normalized = value
      .trim()
      .replaceAll(",", "")
      .replaceAll("$", "")
      .replaceAll("£", "")
      .replaceAll("€", "")
      .replaceAll("%", "");
    if (!normalized) {
      return null;
    }
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function truncateLabel(value, limit = 18) {
  const text = String(value || "").trim();
  if (text.length <= limit) {
    return text;
  }
  return `${text.slice(0, Math.max(0, limit - 1)).trimEnd()}…`;
}

function normalizeLabelPhrase(value) {
  return String(value || "")
    .replaceAll("__", " ")
    .replaceAll("_", " ")
    .replaceAll("-", " ")
    .replaceAll(".", " ")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function toDisplayLabel(value, { stripMeasurePrefix = false } = {}) {
  let label = normalizeLabelPhrase(value);
  if (stripMeasurePrefix) {
    label = label.replace(/^(monthly|total|sum|avg|average)\s+/, "");
  }
  return label.replace(/\b\w/g, (match) => match.toUpperCase());
}

function columnMatchPhrases(key) {
  const raw = String(key || "").trim();
  const tail = raw.split("__").pop().split(".").pop();
  const normalized = normalizeLabelPhrase(raw);
  const normalizedTail = normalizeLabelPhrase(tail);
  const strippedTail = normalizedTail.replace(/^(monthly|total|sum|avg|average)\s+/, "");
  return [normalized, normalizedTail, strippedTail].filter(Boolean);
}

function supportsMultipleMeasureChart(chartType) {
  return ["bar", "stacked-bar", "line", "area"].includes(String(chartType || "").trim());
}

function expandRequestedMeasureKeys({
  records,
  measureKeys,
  chartType,
  contextText,
  excludeKeys = [],
}) {
  if (!supportsMultipleMeasureChart(chartType) || !contextText || measureKeys.length === 0) {
    return measureKeys;
  }
  const normalizedContext = ` ${normalizeLabelPhrase(contextText)} `;
  const rowKeys = records[0] ? Object.keys(records[0]) : [];
  const requestedKeys = rowKeys.filter((key) => {
    if (excludeKeys.includes(key) || !records.some((record) => toNumber(record?.[key]) !== null)) {
      return false;
    }
    return columnMatchPhrases(key).some((phrase) => normalizedContext.includes(` ${phrase} `));
  });
  if (requestedKeys.length <= 1) {
    return measureKeys;
  }
  return [...requestedKeys, ...measureKeys].filter(
    (key, index, collection) => collection.indexOf(key) === index,
  );
}

function inferDimensionKey({ records, metadata, preferredDimension, excludeKeys = [] }) {
  const rowKeys = records[0] ? Object.keys(records[0]) : [];
  const fallbackKey =
    rowKeys.find(
      (key) =>
        !excludeKeys.includes(key) &&
        records.some((record) => record?.[key] !== null && record?.[key] !== undefined && toNumber(record?.[key]) === null),
    ) ||
    rowKeys.find((key) => !excludeKeys.includes(key)) ||
    rowKeys[0];
  return resolveChartDataKey({
    selectedKey: preferredDimension,
    rowKeys,
    metadata,
    fallbackKey,
  });
}

function inferMeasureKeys({ records, metadata, preferredMeasures = [], excludeKeys = [] }) {
  const rowKeys = records[0] ? Object.keys(records[0]) : [];
  const resolved = preferredMeasures
    .map((key) =>
      resolveChartDataKey({
        selectedKey: key,
        rowKeys,
        metadata,
        fallbackKey: null,
      }),
    )
    .filter(Boolean)
    .filter((key, index, collection) => collection.indexOf(key) === index)
    .filter((key) => !excludeKeys.includes(key))
    .filter((key) => records.some((record) => toNumber(record?.[key]) !== null));

  if (resolved.length > 0) {
    return resolved;
  }

  const numericKeys = rowKeys.filter(
    (key) =>
      !excludeKeys.includes(key) &&
      records.some((record) => toNumber(record?.[key]) !== null),
  );
  return numericKeys.length > 0 ? [numericKeys[0]] : [];
}

function inferNumericKey({ records, metadata, preferredKey, excludeKeys = [] }) {
  const rowKeys = records[0] ? Object.keys(records[0]) : [];
  const fallbackKey =
    rowKeys.find(
      (key) =>
        !excludeKeys.includes(key) && records.some((record) => toNumber(record?.[key]) !== null),
    ) ||
    rowKeys[0];
  return resolveChartDataKey({
    selectedKey: preferredKey,
    rowKeys,
    metadata,
    fallbackKey,
  });
}

function buildSeriesPalette(themeColors = []) {
  return themeColors.length > 0 ? themeColors : DEFAULT_PALETTE;
}

function toSvgIdFragment(value) {
  return String(value || "chart").replace(/[^a-zA-Z0-9_-]/g, "");
}

function buildChartShellStyle(palette) {
  return {
    "--chart-primary": palette[0],
    "--chart-secondary": palette[1] || palette[0],
    "--chart-tertiary": palette[2] || palette[0],
  };
}

function readChartLimit(chartType) {
  switch (chartType) {
    case "pie":
    case "donut":
      return 8;
    case "scatter":
      return 60;
    case "line":
    case "area":
      return 18;
    default:
      return 12;
  }
}

function aggregateCategorySeries({
  records,
  dimensionKey,
  measureKeys,
  groupKey,
  chartType,
}) {
  const maxCategories = readChartLimit(chartType);
  const categories = [];
  const categorySet = new Set();
  const seriesOrder = [];
  const matrix = new Map();

  const registerCategory = (label) => {
    if (!categorySet.has(label) && categories.length < maxCategories) {
      categorySet.add(label);
      categories.push(label);
      matrix.set(label, {});
    }
    return categorySet.has(label);
  };

  if (measureKeys.length > 1) {
    measureKeys.forEach((measureKey) => {
      if (!seriesOrder.includes(measureKey)) {
        seriesOrder.push(measureKey);
      }
    });
    records.forEach((record) => {
      const label = record?.[dimensionKey];
      if (label === null || label === undefined || !registerCategory(String(label))) {
        return;
      }
      const bucket = matrix.get(String(label));
      measureKeys.forEach((measureKey) => {
        const value = toNumber(record?.[measureKey]);
        if (value === null) {
          return;
        }
        bucket[measureKey] = (bucket[measureKey] || 0) + value;
      });
    });
  } else {
    const measureKey = measureKeys[0];
    records.forEach((record) => {
      const label = record?.[dimensionKey];
      const rawValue = toNumber(record?.[measureKey]);
      if (label === null || label === undefined || rawValue === null) {
        return;
      }
      const categoryLabel = String(label);
      if (!registerCategory(categoryLabel)) {
        return;
      }
      const seriesKey =
        groupKey && record?.[groupKey] !== null && record?.[groupKey] !== undefined
          ? String(record[groupKey])
          : measureKey;
      if (!seriesOrder.includes(seriesKey)) {
        seriesOrder.push(seriesKey);
      }
      const bucket = matrix.get(categoryLabel);
      bucket[seriesKey] = (bucket[seriesKey] || 0) + rawValue;
    });
  }

  const series = seriesOrder.map((key, index) => ({
    key,
    label: toDisplayLabel(key, { stripMeasurePrefix: true }),
    color: index,
    values: categories.map((category) => Number(matrix.get(category)?.[key] || 0)),
  }));

  return { categories, series };
}

function buildPieModel({ records, dimensionKey, measureKey, chartType }) {
  const totals = new Map();
  records.forEach((record) => {
    const label = record?.[dimensionKey];
    const value = toNumber(record?.[measureKey]);
    if (label === null || label === undefined || value === null) {
      return;
    }
    totals.set(String(label), (totals.get(String(label)) || 0) + value);
  });

  return [...totals.entries()]
    .map(([label, value]) => ({ label, value }))
    .sort((left, right) => right.value - left.value)
    .slice(0, readChartLimit(chartType));
}

function buildScatterModel({ records, xKey, yKey, groupKey }) {
  return records
    .map((record) => {
      const x = toNumber(record?.[xKey]);
      const y = toNumber(record?.[yKey]);
      if (x === null || y === null) {
        return null;
      }
      return {
        x,
        y,
        group:
          groupKey && record?.[groupKey] !== null && record?.[groupKey] !== undefined
            ? String(record[groupKey])
            : "Series",
        label:
          record?.label || record?.name || record?.[groupKey] || `${formatValue(x)} / ${formatValue(y)}`,
      };
    })
    .filter(Boolean)
    .slice(0, readChartLimit("scatter"));
}

function buildStatModel({ records, measureKey, dimensionKey }) {
  const firstRecord = records[0];
  if (!firstRecord) {
    return null;
  }
  const value = toNumber(firstRecord?.[measureKey]);
  if (value === null) {
    return null;
  }
  const context =
    dimensionKey && firstRecord?.[dimensionKey] !== null && firstRecord?.[dimensionKey] !== undefined
      ? String(firstRecord[dimensionKey])
      : "";
  return {
    value,
    measureKey,
    context,
  };
}

function describeChartKind(chartType) {
  return truncateLabel(
    String(chartType || "")
      .replaceAll("-", " ")
      .replaceAll("_", " "),
    24,
  );
}

function buildTooltipModel({ x, y, width, height, eyebrow, title, value, details = [], color }) {
  return {
    xPercent: Math.max(8, Math.min(92, (x / width) * 100)),
    yPercent: Math.max(10, Math.min(86, (y / height) * 100)),
    eyebrow: String(eyebrow || "").trim(),
    title: String(title || "").trim(),
    value: String(value || "").trim(),
    details: details.filter(Boolean).map((item) => String(item)),
    color: color || DEFAULT_PALETTE[0],
  };
}

function ChartTooltip({ tooltip }) {
  if (!tooltip) {
    return null;
  }
  return (
    <div
      className="chart-tooltip"
      style={{
        left: `${tooltip.xPercent}%`,
        top: `${tooltip.yPercent}%`,
        "--chart-tooltip-accent": tooltip.color,
      }}
      role="status"
      aria-live="polite"
    >
      {tooltip.eyebrow ? <span className="chart-tooltip-eyebrow">{tooltip.eyebrow}</span> : null}
      {tooltip.title ? <strong>{tooltip.title}</strong> : null}
      {tooltip.value ? <span className="chart-tooltip-value">{tooltip.value}</span> : null}
      {tooltip.details.length > 0 ? (
        <div className="chart-tooltip-details">
          {tooltip.details.map((item) => (
            <span key={item}>{item}</span>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function ChartPanelShell({ title, subtitle, chartType, tooltip, onPointerLeave, style, children }) {
  return (
    <div className="chart-panel" style={style}>
      <div className="chart-panel-header">
        <div>
          <h3>{title || "Chart preview"}</h3>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
        <span className="chart-kind">{describeChartKind(chartType)}</span>
      </div>
      <div className="chart-visual-region" onMouseLeave={onPointerLeave}>
        {children}
        <ChartTooltip tooltip={tooltip} />
      </div>
    </div>
  );
}

function ChartEmpty({ title, chartType, message }) {
  return (
    <ChartPanelShell title={title} chartType={chartType || "table"}>
      <div className="empty-box">{message}</div>
    </ChartPanelShell>
  );
}

function ChartLegend({
  series,
  palette,
  activeKey = "",
  onToggleKey,
  onHoverKey,
  onClearHover,
}) {
  if (!Array.isArray(series) || series.length <= 1) {
    return null;
  }
  return (
    <div className="chart-legend">
      {series.map((item, index) => (
        <button
          key={item.key}
          type="button"
          className={`chart-legend-item ${activeKey && activeKey !== item.key ? "muted" : "active"}`}
          onClick={() => onToggleKey?.(item.key)}
          onMouseEnter={() => onHoverKey?.(item.key)}
          onMouseLeave={() => onClearHover?.()}
          onFocus={() => onHoverKey?.(item.key)}
          onBlur={() => onClearHover?.()}
        >
          <i style={{ backgroundColor: palette[index % palette.length] }} aria-hidden="true" />
          {truncateLabel(item.label, 20)}
        </button>
      ))}
    </div>
  );
}

function AxisLabels({ xLabel, yLabel }) {
  return (
    <div className="chart-axis-labels">
      <span>X: {truncateLabel(toDisplayLabel(xLabel), 40)}</span>
      <span>Y: {truncateLabel(yLabel, 40)}</span>
    </div>
  );
}

function BarLikeChart({
  model,
  palette,
  chartId,
  chartType,
  xLabel,
  yLabel,
  activeSeriesKey = "",
  onDatumHover,
  onDatumLeave,
  onToggleSeries,
  onHoverSeries,
  onClearHoverSeries,
}) {
  const width = 720;
  const height = 320;
  const padding = { top: 24, right: 24, bottom: 76, left: 64 };
  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  const tickValues = [0.25, 0.5, 0.75, 1];
  const maxValue =
    chartType === "stacked-bar"
      ? Math.max(
          ...model.categories.map((_, categoryIndex) =>
            model.series.reduce((sum, series) => sum + (series.values[categoryIndex] || 0), 0),
          ),
          1,
        )
      : Math.max(...model.series.flatMap((series) => series.values), 1);
  const band = chartWidth / Math.max(model.categories.length, 1);
  const groupWidth = band * 0.72;
  const categoryOffset = padding.left + (band - groupWidth) / 2;
  const emphasisKey = String(activeSeriesKey || "").trim();
  const resolvedEmphasisKey = model.series.some((series) => series.key === emphasisKey) ? emphasisKey : "";

  return (
    <div className="chart-canvas-shell">
      <svg className="chart-canvas" viewBox={`0 0 ${width} ${height}`} role="img">
        <defs>
          {model.series.map((series, seriesIndex) => (
            <linearGradient
              key={`${series.key}-bar-gradient`}
              id={`${chartId}-bar-gradient-${seriesIndex}`}
              x1="0%"
              y1="0%"
              x2="0%"
              y2="100%"
            >
              <stop offset="0%" stopColor={palette[seriesIndex % palette.length]} stopOpacity="1" />
              <stop offset="100%" stopColor={palette[seriesIndex % palette.length]} stopOpacity="0.62" />
            </linearGradient>
          ))}
        </defs>
        <rect
          x={padding.left}
          y={padding.top}
          width={chartWidth}
          height={chartHeight}
          rx="24"
          className="chart-plot-backdrop"
        />
        <rect
          x={padding.left}
          y={padding.top}
          width={chartWidth}
          height={chartHeight}
          rx="24"
          className="chart-plot-frame"
        />
        {tickValues.map((tick) => {
          const y = padding.top + chartHeight - chartHeight * tick;
          return (
            <g key={tick}>
              <line x1={padding.left} x2={width - padding.right} y1={y} y2={y} className="chart-grid-line" />
              <text x={padding.left - 12} y={y + 4} textAnchor="end" className="chart-axis-tick">
                {formatValue(maxValue * tick)}
              </text>
            </g>
          );
        })}
        <line
          x1={padding.left}
          x2={width - padding.right}
          y1={padding.top + chartHeight}
          y2={padding.top + chartHeight}
          className="chart-axis-line"
        />
        <line
          x1={padding.left}
          x2={padding.left}
          y1={padding.top}
          y2={padding.top + chartHeight}
          className="chart-axis-line"
        />
        {model.categories.map((category, categoryIndex) => {
          const groupX = categoryOffset + categoryIndex * band;
          const values = model.series.map((series) => series.values[categoryIndex] || 0);

          if (chartType === "stacked-bar") {
            let runningHeight = 0;
            return (
              <g key={category}>
                {values.map((value, seriesIndex) => {
                  const series = model.series[seriesIndex];
                  const barHeight = (Math.max(value, 0) / maxValue) * chartHeight;
                  const y = padding.top + chartHeight - runningHeight - barHeight;
                  runningHeight += barHeight;
                  return (
                    <rect
                      key={`${category}-${seriesIndex}`}
                      x={groupX}
                      y={y}
                      width={groupWidth}
                      height={Math.max(barHeight, 2)}
                      rx="10"
                      fill={`url(#${chartId}-bar-gradient-${seriesIndex})`}
                      stroke={palette[seriesIndex % palette.length]}
                      strokeOpacity="0.24"
                      strokeWidth="1"
                      className="chart-bar-mark"
                      opacity={!resolvedEmphasisKey || resolvedEmphasisKey === series.key ? 0.96 : 0.2}
                      tabIndex={0}
                      onMouseEnter={() =>
                        onDatumHover?.(
                          buildTooltipModel({
                            x: groupX + groupWidth / 2,
                            y,
                            width,
                            height,
                            eyebrow: series.label,
                            title: category,
                            value: formatValue(value),
                            details: [
                              `${toDisplayLabel(xLabel)}: ${category}`,
                              `${series.label}: ${formatValue(value)}`,
                            ],
                            color: palette[seriesIndex % palette.length],
                          }),
                        )
                      }
                      onMouseLeave={onDatumLeave}
                      onFocus={() =>
                        onDatumHover?.(
                          buildTooltipModel({
                            x: groupX + groupWidth / 2,
                            y,
                            width,
                            height,
                            eyebrow: series.label,
                            title: category,
                            value: formatValue(value),
                            details: [
                              `${toDisplayLabel(xLabel)}: ${category}`,
                              `${series.label}: ${formatValue(value)}`,
                            ],
                            color: palette[seriesIndex % palette.length],
                          }),
                        )
                      }
                      onBlur={onDatumLeave}
                    />
                  );
                })}
                <text
                  x={groupX + groupWidth / 2}
                  y={height - 20}
                  textAnchor="middle"
                  className="chart-axis-tick chart-axis-tick--x"
                >
                  {truncateLabel(category, 14)}
                </text>
              </g>
            );
          }

          const barWidth = groupWidth / Math.max(model.series.length, 1);
          return (
            <g key={category}>
              {values.map((value, seriesIndex) => {
                const series = model.series[seriesIndex];
                const barHeight = (Math.max(value, 0) / maxValue) * chartHeight;
                const x = groupX + seriesIndex * barWidth;
                const y = padding.top + chartHeight - barHeight;
                return (
                  <rect
                    key={`${category}-${seriesIndex}`}
                    x={x}
                    y={y}
                    width={Math.max(barWidth - 6, 10)}
                    height={Math.max(barHeight, 2)}
                    rx="10"
                    fill={`url(#${chartId}-bar-gradient-${seriesIndex})`}
                    stroke={palette[seriesIndex % palette.length]}
                    strokeOpacity="0.24"
                    strokeWidth="1"
                    className="chart-bar-mark"
                    opacity={!resolvedEmphasisKey || resolvedEmphasisKey === series.key ? 0.96 : 0.2}
                    tabIndex={0}
                    onMouseEnter={() =>
                      onDatumHover?.(
                        buildTooltipModel({
                          x: x + Math.max(barWidth - 6, 10) / 2,
                          y,
                          width,
                          height,
                          eyebrow: series.label,
                          title: category,
                          value: formatValue(value),
                          details: [
                            `${toDisplayLabel(xLabel)}: ${category}`,
                            `${series.label}: ${formatValue(value)}`,
                          ],
                          color: palette[seriesIndex % palette.length],
                        }),
                      )
                    }
                    onMouseLeave={onDatumLeave}
                    onFocus={() =>
                      onDatumHover?.(
                        buildTooltipModel({
                          x: x + Math.max(barWidth - 6, 10) / 2,
                          y,
                          width,
                          height,
                          eyebrow: series.label,
                          title: category,
                          value: formatValue(value),
                          details: [
                            `${toDisplayLabel(xLabel)}: ${category}`,
                            `${series.label}: ${formatValue(value)}`,
                          ],
                          color: palette[seriesIndex % palette.length],
                        }),
                      )
                    }
                    onBlur={onDatumLeave}
                  />
                );
              })}
              <text
                x={groupX + groupWidth / 2}
                y={height - 20}
                textAnchor="middle"
                className="chart-axis-tick chart-axis-tick--x"
              >
                {truncateLabel(category, 14)}
              </text>
            </g>
          );
        })}
      </svg>
      <ChartLegend
        series={model.series}
        palette={palette}
        activeKey={resolvedEmphasisKey}
        onToggleKey={onToggleSeries}
        onHoverKey={onHoverSeries}
        onClearHover={onClearHoverSeries}
      />
      <AxisLabels xLabel={xLabel} yLabel={yLabel} />
    </div>
  );
}

function buildLinePath(points, { baseline = null } = {}) {
  if (!Array.isArray(points) || points.length === 0) {
    return "";
  }
  const first = points[0];
  const commands = [`M ${first.x} ${first.y}`];
  points.slice(1).forEach((point) => {
    commands.push(`L ${point.x} ${point.y}`);
  });
  if (baseline !== null) {
    const last = points[points.length - 1];
    commands.push(`L ${last.x} ${baseline}`);
    commands.push(`L ${first.x} ${baseline}`);
    commands.push("Z");
  }
  return commands.join(" ");
}

function LineLikeChart({
  model,
  palette,
  chartId,
  chartType,
  xLabel,
  yLabel,
  activeSeriesKey = "",
  onDatumHover,
  onDatumLeave,
  onToggleSeries,
  onHoverSeries,
  onClearHoverSeries,
}) {
  const width = 720;
  const height = 320;
  const padding = { top: 24, right: 24, bottom: 72, left: 64 };
  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  const maxValue = Math.max(...model.series.flatMap((series) => series.values), 1);
  const tickValues = [0.25, 0.5, 0.75, 1];
  const xStep = chartWidth / Math.max(model.categories.length - 1, 1);

  const seriesWithPoints = model.series.map((series) => ({
    ...series,
    points: series.values.map((value, index) => ({
      x:
        model.categories.length === 1
          ? padding.left + chartWidth / 2
          : padding.left + index * xStep,
      y: padding.top + chartHeight - (Math.max(value, 0) / maxValue) * chartHeight,
      value,
      label: model.categories[index],
    })),
  }));
  const emphasisKey = String(activeSeriesKey || "").trim();
  const resolvedEmphasisKey = model.series.some((series) => series.key === emphasisKey) ? emphasisKey : "";

  return (
    <div className="chart-canvas-shell">
      <svg className="chart-canvas" viewBox={`0 0 ${width} ${height}`} role="img">
        <defs>
          {seriesWithPoints.map((series, seriesIndex) => (
            <linearGradient
              key={`${series.key}-area-gradient`}
              id={`${chartId}-area-gradient-${seriesIndex}`}
              x1="0%"
              y1="0%"
              x2="0%"
              y2="100%"
            >
              <stop offset="0%" stopColor={palette[seriesIndex % palette.length]} stopOpacity="0.3" />
              <stop offset="100%" stopColor={palette[seriesIndex % palette.length]} stopOpacity="0.03" />
            </linearGradient>
          ))}
        </defs>
        <rect
          x={padding.left}
          y={padding.top}
          width={chartWidth}
          height={chartHeight}
          rx="24"
          className="chart-plot-backdrop"
        />
        <rect
          x={padding.left}
          y={padding.top}
          width={chartWidth}
          height={chartHeight}
          rx="24"
          className="chart-plot-frame"
        />
        {tickValues.map((tick) => {
          const y = padding.top + chartHeight - chartHeight * tick;
          return (
            <g key={tick}>
              <line x1={padding.left} x2={width - padding.right} y1={y} y2={y} className="chart-grid-line" />
              <text x={padding.left - 12} y={y + 4} textAnchor="end" className="chart-axis-tick">
                {formatValue(maxValue * tick)}
              </text>
            </g>
          );
        })}
        <line
          x1={padding.left}
          x2={width - padding.right}
          y1={padding.top + chartHeight}
          y2={padding.top + chartHeight}
          className="chart-axis-line"
        />
        {model.categories.map((category, index) => {
          const x =
            model.categories.length === 1
              ? padding.left + chartWidth / 2
              : padding.left + index * xStep;
          return (
            <text
              key={category}
              x={x}
              y={height - 18}
              textAnchor="middle"
              className="chart-axis-tick chart-axis-tick--x"
            >
              {truncateLabel(category, 14)}
            </text>
          );
        })}
        {seriesWithPoints.map((series, seriesIndex) => (
          <g key={series.key}>
            {chartType === "area" ? (
              <path
                d={buildLinePath(series.points, { baseline: padding.top + chartHeight })}
                fill={`url(#${chartId}-area-gradient-${seriesIndex})`}
                stroke="none"
                className="chart-area-path"
                opacity={!resolvedEmphasisKey || resolvedEmphasisKey === series.key ? 1 : 0.32}
              />
            ) : null}
            <path
              d={buildLinePath(series.points)}
              fill="none"
              stroke={palette[seriesIndex % palette.length]}
              strokeWidth="8"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="chart-line-underlay"
              opacity={!resolvedEmphasisKey || resolvedEmphasisKey === series.key ? 0.22 : 0.06}
              pointerEvents="none"
            />
            {chartType !== "area" ? (
              <path
                d={buildLinePath(series.points, { baseline: padding.top + chartHeight })}
                fill={`url(#${chartId}-area-gradient-${seriesIndex})`}
                stroke="none"
                className="chart-area-path chart-area-path--line"
                opacity={!resolvedEmphasisKey || resolvedEmphasisKey === series.key ? 0.45 : 0.12}
              />
            ) : null}
            <path
              d={buildLinePath(series.points)}
              fill="none"
              stroke={palette[seriesIndex % palette.length]}
              strokeWidth="3"
              strokeLinecap="round"
              strokeLinejoin="round"
              className="chart-line-path"
              opacity={!resolvedEmphasisKey || resolvedEmphasisKey === series.key ? 1 : 0.22}
              onMouseEnter={() => onHoverSeries?.(series.key)}
              onMouseLeave={() => onClearHoverSeries?.()}
            />
            {series.points.map((point) => (
              <g key={`${series.key}-${point.label}`}>
                <circle
                  cx={point.x}
                  cy={point.y}
                  r={!resolvedEmphasisKey || resolvedEmphasisKey === series.key ? "8" : "6.5"}
                  fill={palette[seriesIndex % palette.length]}
                  fillOpacity={!resolvedEmphasisKey || resolvedEmphasisKey === series.key ? "0.16" : "0.06"}
                  className="chart-point-halo"
                />
                <circle
                  cx={point.x}
                  cy={point.y}
                  r={!resolvedEmphasisKey || resolvedEmphasisKey === series.key ? "4.5" : "3.5"}
                  fill={palette[seriesIndex % palette.length]}
                  stroke="rgba(255, 255, 255, 0.95)"
                  strokeWidth="2"
                  opacity={!resolvedEmphasisKey || resolvedEmphasisKey === series.key ? 1 : 0.28}
                  className="chart-point-mark"
                />
                <circle
                  cx={point.x}
                  cy={point.y}
                  r="12"
                  fill="transparent"
                  tabIndex={0}
                  onMouseEnter={() =>
                    onDatumHover?.(
                      buildTooltipModel({
                        x: point.x,
                        y: point.y,
                        width,
                        height,
                        eyebrow: series.label,
                        title: point.label,
                        value: formatValue(point.value),
                        details: [`${xLabel}: ${point.label}`, `${yLabel}: ${formatValue(point.value)}`],
                        color: palette[seriesIndex % palette.length],
                      }),
                    )
                  }
                  onMouseLeave={onDatumLeave}
                  onFocus={() =>
                    onDatumHover?.(
                      buildTooltipModel({
                        x: point.x,
                        y: point.y,
                        width,
                        height,
                        eyebrow: series.label,
                        title: point.label,
                        value: formatValue(point.value),
                        details: [`${xLabel}: ${point.label}`, `${yLabel}: ${formatValue(point.value)}`],
                        color: palette[seriesIndex % palette.length],
                      }),
                    )
                  }
                  onBlur={onDatumLeave}
                />
              </g>
            ))}
          </g>
        ))}
      </svg>
      <ChartLegend
        series={model.series}
        palette={palette}
        activeKey={resolvedEmphasisKey}
        onToggleKey={onToggleSeries}
        onHoverKey={onHoverSeries}
        onClearHover={onClearHoverSeries}
      />
      <AxisLabels xLabel={xLabel} yLabel={yLabel} />
    </div>
  );
}

function ScatterChart({
  points,
  xLabel,
  yLabel,
  groupKey,
  palette,
  chartId,
  activeGroupKey = "",
  onDatumHover,
  onDatumLeave,
  onToggleSeries,
  onHoverSeries,
  onClearHoverSeries,
}) {
  const width = 720;
  const height = 320;
  const padding = { top: 24, right: 24, bottom: 72, left: 64 };
  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  const maxX = Math.max(...points.map((point) => point.x), 1);
  const maxY = Math.max(...points.map((point) => point.y), 1);
  const groups = [...new Set(points.map((point) => point.group))];
  const ticks = [0.25, 0.5, 0.75, 1];
  const emphasisKey = String(activeGroupKey || "").trim();
  const resolvedEmphasisKey = groups.includes(emphasisKey) ? emphasisKey : "";

  return (
    <div className="chart-canvas-shell">
      <svg className="chart-canvas" viewBox={`0 0 ${width} ${height}`} role="img">
        <defs>
          {groups.map((group, groupIndex) => (
            <radialGradient
              key={`${group}-scatter-gradient`}
              id={`${chartId}-scatter-gradient-${groupIndex}`}
              cx="35%"
              cy="35%"
              r="80%"
            >
              <stop offset="0%" stopColor={palette[groupIndex % palette.length]} stopOpacity="1" />
              <stop offset="100%" stopColor={palette[groupIndex % palette.length]} stopOpacity="0.64" />
            </radialGradient>
          ))}
        </defs>
        <rect
          x={padding.left}
          y={padding.top}
          width={chartWidth}
          height={chartHeight}
          rx="24"
          className="chart-plot-backdrop"
        />
        <rect
          x={padding.left}
          y={padding.top}
          width={chartWidth}
          height={chartHeight}
          rx="24"
          className="chart-plot-frame"
        />
        {ticks.map((tick) => {
          const y = padding.top + chartHeight - chartHeight * tick;
          const x = padding.left + chartWidth * tick;
          return (
            <g key={tick}>
              <line x1={padding.left} x2={width - padding.right} y1={y} y2={y} className="chart-grid-line" />
              <line x1={x} x2={x} y1={padding.top} y2={padding.top + chartHeight} className="chart-grid-line" />
              <text x={padding.left - 12} y={y + 4} textAnchor="end" className="chart-axis-tick">
                {formatValue(maxY * tick)}
              </text>
              <text x={x} y={height - 18} textAnchor="middle" className="chart-axis-tick chart-axis-tick--x">
                {formatValue(maxX * tick)}
              </text>
            </g>
          );
        })}
        <line
          x1={padding.left}
          x2={width - padding.right}
          y1={padding.top + chartHeight}
          y2={padding.top + chartHeight}
          className="chart-axis-line"
        />
        <line
          x1={padding.left}
          x2={padding.left}
          y1={padding.top}
          y2={padding.top + chartHeight}
          className="chart-axis-line"
        />
        {points.map((point, index) => {
          const x = padding.left + (point.x / maxX) * chartWidth;
          const y = padding.top + chartHeight - (point.y / maxY) * chartHeight;
          const groupIndex = Math.max(groups.indexOf(point.group), 0);
          return (
            <g key={`${point.group}-${point.label}-${index}`}>
              <circle
                cx={x}
                cy={y}
                r={!resolvedEmphasisKey || resolvedEmphasisKey === point.group ? "10" : "8"}
                fill={palette[groupIndex % palette.length]}
                fillOpacity={!resolvedEmphasisKey || resolvedEmphasisKey === point.group ? "0.14" : "0.05"}
                className="chart-scatter-halo"
              />
              <circle
                cx={x}
                cy={y}
                r={!resolvedEmphasisKey || resolvedEmphasisKey === point.group ? "6.5" : "5"}
                fill={`url(#${chartId}-scatter-gradient-${groupIndex})`}
                stroke="rgba(255, 255, 255, 0.9)"
                strokeWidth="1.5"
                fillOpacity={!resolvedEmphasisKey || resolvedEmphasisKey === point.group ? "0.9" : "0.22"}
                className="chart-scatter-mark"
              />
              <circle
                cx={x}
                cy={y}
                r="14"
                fill="transparent"
                tabIndex={0}
                onMouseEnter={() =>
                  onDatumHover?.(
                    buildTooltipModel({
                      x,
                      y,
                      width,
                      height,
                      eyebrow: point.group,
                      title: point.label,
                      value: `${formatValue(point.x)} / ${formatValue(point.y)}`,
                      details: [`${xLabel}: ${formatValue(point.x)}`, `${yLabel}: ${formatValue(point.y)}`],
                      color: palette[groupIndex % palette.length],
                    }),
                  )
                }
                onMouseLeave={onDatumLeave}
                onFocus={() =>
                  onDatumHover?.(
                    buildTooltipModel({
                      x,
                      y,
                      width,
                      height,
                      eyebrow: point.group,
                      title: point.label,
                      value: `${formatValue(point.x)} / ${formatValue(point.y)}`,
                      details: [`${xLabel}: ${formatValue(point.x)}`, `${yLabel}: ${formatValue(point.y)}`],
                      color: palette[groupIndex % palette.length],
                    }),
                  )
                }
                onBlur={onDatumLeave}
              />
            </g>
          );
        })}
      </svg>
      <ChartLegend
        series={groups.map((group) => ({ key: group, label: group }))}
        palette={palette}
        activeKey={resolvedEmphasisKey}
        onToggleKey={onToggleSeries}
        onHoverKey={onHoverSeries}
        onClearHover={onClearHoverSeries}
      />
      <AxisLabels
        xLabel={xLabel}
        yLabel={groupKey ? `${yLabel} grouped by ${groupKey}` : yLabel}
      />
    </div>
  );
}

function polarToCartesian(cx, cy, radius, angleInDegrees) {
  const angleInRadians = ((angleInDegrees - 90) * Math.PI) / 180;
  return {
    x: cx + radius * Math.cos(angleInRadians),
    y: cy + radius * Math.sin(angleInRadians),
  };
}

function describeArc(cx, cy, radius, startAngle, endAngle) {
  const start = polarToCartesian(cx, cy, radius, endAngle);
  const end = polarToCartesian(cx, cy, radius, startAngle);
  const largeArcFlag = endAngle - startAngle <= 180 ? "0" : "1";
  return [`M ${start.x} ${start.y}`, `A ${radius} ${radius} 0 ${largeArcFlag} 0 ${end.x} ${end.y}`, `L ${cx} ${cy}`, "Z"].join(" ");
}

function describeDonutArc(cx, cy, outerRadius, innerRadius, startAngle, endAngle) {
  const startOuter = polarToCartesian(cx, cy, outerRadius, endAngle);
  const endOuter = polarToCartesian(cx, cy, outerRadius, startAngle);
  const startInner = polarToCartesian(cx, cy, innerRadius, startAngle);
  const endInner = polarToCartesian(cx, cy, innerRadius, endAngle);
  const largeArcFlag = endAngle - startAngle <= 180 ? "0" : "1";
  return [
    `M ${startOuter.x} ${startOuter.y}`,
    `A ${outerRadius} ${outerRadius} 0 ${largeArcFlag} 0 ${endOuter.x} ${endOuter.y}`,
    `L ${startInner.x} ${startInner.y}`,
    `A ${innerRadius} ${innerRadius} 0 ${largeArcFlag} 1 ${endInner.x} ${endInner.y}`,
    "Z",
  ].join(" ");
}

function PieChart({
  slices,
  palette,
  chartId,
  chartType,
  measureKey,
  title,
  activeSliceKey = "",
  onHoverSliceKey,
  onClearHoverSliceKey,
  onToggleSlice,
  onTooltipChange,
  onTooltipClear,
}) {
  const total = slices.reduce((sum, slice) => sum + slice.value, 0) || 1;
  const cx = 140;
  const cy = 140;
  const outerRadius = 108;
  const innerRadius = chartType === "donut" ? 58 : 0;
  let currentAngle = 0;
  const emphasisKey = String(activeSliceKey || "").trim();
  const resolvedEmphasisKey = slices.some((slice) => slice.label === emphasisKey) ? emphasisKey : "";

  return (
    <div className="chart-pie-layout">
      <div className="chart-pie-shell">
        <svg className="chart-pie" viewBox="0 0 280 280" role="img">
          <defs>
            <radialGradient id={`${chartId}-pie-glow`} cx="50%" cy="44%" r="62%">
              <stop offset="0%" stopColor={palette[0]} stopOpacity="0.18" />
              <stop offset="72%" stopColor={palette[1 % palette.length]} stopOpacity="0.06" />
              <stop offset="100%" stopColor={palette[1 % palette.length]} stopOpacity="0" />
            </radialGradient>
          </defs>
          <circle cx={cx} cy={cy} r="116" fill={`url(#${chartId}-pie-glow)`} className="chart-pie-glow" />
          {chartType === "donut" ? (
            <circle cx={cx} cy={cy} r={innerRadius + 18} className="chart-pie-ring-shell" />
          ) : null}
        {slices.map((slice, index) => {
          const angle = (slice.value / total) * 360;
          const startAngle = currentAngle;
          const endAngle = currentAngle + angle;
          const path =
            chartType === "donut"
              ? describeDonutArc(cx, cy, outerRadius, innerRadius, startAngle, endAngle)
              : describeArc(cx, cy, outerRadius, startAngle, endAngle);
          currentAngle = endAngle;
          const middleAngle = startAngle + angle / 2;
          const outerPoint = polarToCartesian(cx, cy, outerRadius - 12, middleAngle);
          const emphasisOffset = resolvedEmphasisKey === slice.label ? 6 : 0;
          const angleInRadians = ((middleAngle - 90) * Math.PI) / 180;
          const offsetX = Math.cos(angleInRadians) * emphasisOffset;
          const offsetY = Math.sin(angleInRadians) * emphasisOffset;
          return (
            <path
              key={slice.label}
              d={path}
              fill={palette[index % palette.length]}
              opacity={!resolvedEmphasisKey || resolvedEmphasisKey === slice.label ? 0.96 : 0.22}
              className="chart-pie-slice"
              transform={emphasisOffset ? `translate(${offsetX} ${offsetY})` : undefined}
              stroke="rgba(255,255,255,0.92)"
              strokeWidth={!resolvedEmphasisKey || resolvedEmphasisKey === slice.label ? 2 : 1}
              tabIndex={0}
              onMouseEnter={() => {
                onHoverSliceKey?.(slice.label);
                onTooltipChange?.(
                  buildTooltipModel({
                    x: outerPoint.x,
                    y: outerPoint.y,
                    width: 280,
                    height: 280,
                    eyebrow: title || "Breakdown",
                    title: slice.label,
                    value: formatValue(slice.value),
                    details: [
                      `${truncateLabel(measureKey, 28)}: ${formatValue(slice.value)}`,
                      `${Math.round((slice.value / total) * 100)}% of total`,
                    ],
                    color: palette[index % palette.length],
                  }),
                );
              }}
              onMouseLeave={() => {
                onClearHoverSliceKey?.();
                onTooltipClear?.();
              }}
              onFocus={() => {
                onHoverSliceKey?.(slice.label);
                onTooltipChange?.(
                  buildTooltipModel({
                    x: outerPoint.x,
                    y: outerPoint.y,
                    width: 280,
                    height: 280,
                    eyebrow: title || "Breakdown",
                    title: slice.label,
                    value: formatValue(slice.value),
                    details: [
                      `${truncateLabel(measureKey, 28)}: ${formatValue(slice.value)}`,
                      `${Math.round((slice.value / total) * 100)}% of total`,
                    ],
                    color: palette[index % palette.length],
                  }),
                );
              }}
              onBlur={() => {
                onClearHoverSliceKey?.();
                onTooltipClear?.();
              }}
              onClick={() => onToggleSlice?.(slice.label)}
            />
          );
        })}
        {chartType === "donut" ? (
          <g>
            <circle cx={cx} cy={cy} r={innerRadius - 8} className="chart-pie-inner-ring" />
            <text x={cx} y={cy - 8} textAnchor="middle" className="chart-donut-total-label">
              Total
            </text>
            <text x={cx} y={cy + 18} textAnchor="middle" className="chart-donut-total-value">
              {formatValue(total)}
            </text>
          </g>
        ) : null}
        </svg>
      </div>
      <div className="chart-pie-legend">
        <strong>{title || "Breakdown"}</strong>
        <span>{truncateLabel(measureKey, 28)}</span>
        {slices.map((slice, index) => (
          <button
            key={slice.label}
            type="button"
            className={`chart-pie-legend-item ${resolvedEmphasisKey && resolvedEmphasisKey !== slice.label ? "muted" : "active"}`}
            onClick={() => onToggleSlice?.(slice.label)}
            onMouseEnter={() => onHoverSliceKey?.(slice.label)}
            onMouseLeave={() => onClearHoverSliceKey?.()}
            onFocus={() => onHoverSliceKey?.(slice.label)}
            onBlur={() => onClearHoverSliceKey?.()}
          >
            <span className="chart-pie-legend-swatch" style={{ backgroundColor: palette[index % palette.length] }} />
            <div>
              <strong>{truncateLabel(slice.label, 20)}</strong>
              <span>
                {formatValue(slice.value)} ({Math.round((slice.value / total) * 100)}%)
              </span>
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}

function StatCard({ stat, title }) {
  return (
    <div className="chart-stat-card">
      <span className="chart-stat-label">{title || truncateLabel(stat.measureKey, 32)}</span>
      <strong>{formatValue(stat.value)}</strong>
      <p>{truncateLabel(stat.context || stat.measureKey, 48)}</p>
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
  const chartId = toSvgIdFragment(useId());
  const [focusedKey, setFocusedKey] = useState("");
  const [hoveredKey, setHoveredKey] = useState("");
  const [tooltip, setTooltip] = useState(null);
  const records = toRecords(result);
  const palette = buildSeriesPalette(themeColors);
  const chartShellStyle = buildChartShellStyle(palette);
  const normalizedVisualization = visualization?.chartType
    ? visualization
    : {
        ...visualization,
        chartType: normalizeChartType(visualization?.chartType || visualization?.chart_type),
      };
  const baseChartType = normalizeChartType(
    normalizedVisualization?.chartType || normalizedVisualization?.chart_type,
  );
  const chartType = normalizedVisualization?.donut
    ? "donut"
    : normalizedVisualization?.stacked && baseChartType === "bar"
      ? "stacked-bar"
      : baseChartType;
  const emphasisKey = hoveredKey || focusedKey;

  function handleToggleKey(key) {
    const normalized = String(key || "").trim();
    if (!normalized) {
      setFocusedKey("");
      return;
    }
    setFocusedKey((current) => (current === normalized ? "" : normalized));
  }

  function clearHoverState() {
    setHoveredKey("");
  }

  function clearVisualInteraction() {
    setTooltip(null);
    setHoveredKey("");
  }

  if (records.length === 0) {
    return (
      <ChartEmpty
        title={title}
        chartType={chartType}
        message="Run a query to render a chart preview."
      />
    );
  }

  const resolvedMetadata =
    Array.isArray(metadata) && metadata.length > 0
      ? metadata
      : Array.isArray(result?.metadata)
        ? result.metadata
        : [];

  if (chartType === "scatter") {
    const xKey = inferNumericKey({
      records,
      metadata: resolvedMetadata,
      preferredKey: normalizedVisualization?.x || preferredDimension,
    });
    const yKey = inferNumericKey({
      records,
      metadata: resolvedMetadata,
      preferredKey:
        Array.isArray(normalizedVisualization?.y) && normalizedVisualization.y.length > 0
          ? normalizedVisualization.y[0]
          : normalizedVisualization?.y || preferredMeasure,
      excludeKeys: [xKey].filter(Boolean),
    });
    const groupKey = normalizedVisualization?.groupBy
      ? inferDimensionKey({
          records,
          metadata: resolvedMetadata,
          preferredDimension: normalizedVisualization.groupBy,
          excludeKeys: [xKey, yKey].filter(Boolean),
        })
      : "";
    if (!xKey || !yKey) {
      return (
        <ChartEmpty
          title={title}
          chartType={chartType}
          message="This result does not contain the numeric fields needed for a scatter chart."
        />
      );
    }
    const scatterPoints = buildScatterModel({ records, xKey, yKey, groupKey });
    if (scatterPoints.length === 0) {
      return (
        <ChartEmpty
          title={title}
          chartType={chartType}
          message="No numeric points were available for a scatter chart."
        />
      );
    }
    return (
      <ChartPanelShell
        title={title || "Chart preview"}
        subtitle={normalizedVisualization?.subtitle}
        chartType={chartType}
        tooltip={tooltip}
        onPointerLeave={clearVisualInteraction}
        style={chartShellStyle}
      >
        <ScatterChart
          points={scatterPoints}
          xLabel={xKey}
          yLabel={yKey}
          groupKey={groupKey}
          palette={palette}
          chartId={chartId}
          activeGroupKey={emphasisKey}
          onDatumHover={setTooltip}
          onDatumLeave={() => setTooltip(null)}
          onToggleSeries={handleToggleKey}
          onHoverSeries={setHoveredKey}
          onClearHoverSeries={clearHoverState}
        />
      </ChartPanelShell>
    );
  }

  const preferredMeasures =
    Array.isArray(normalizedVisualization?.y) && normalizedVisualization.y.length > 0
      ? normalizedVisualization.y
      : [normalizedVisualization?.y || preferredMeasure].filter(Boolean);
  let measureKeys = inferMeasureKeys({
    records,
    metadata: resolvedMetadata,
    preferredMeasures,
    excludeKeys: [],
  });
  measureKeys = expandRequestedMeasureKeys({
    records,
    measureKeys,
    chartType,
    contextText: [
      normalizedVisualization?.title,
      normalizedVisualization?.subtitle,
      normalizedVisualization?.rationale,
      normalizedVisualization?.options?.rationale,
      normalizedVisualization?.options?.reason,
    ]
      .filter(Boolean)
      .join(" "),
    excludeKeys: [],
  });

  if (measureKeys.length === 0) {
    return (
      <ChartEmpty
        title={title}
        chartType={chartType}
        message="This result does not contain numeric fields that can be charted."
      />
    );
  }

  const dimensionKey = inferDimensionKey({
    records,
    metadata: resolvedMetadata,
    preferredDimension: normalizedVisualization?.x || preferredDimension,
    excludeKeys: measureKeys,
  });
  const groupKey = normalizedVisualization?.groupBy
    ? inferDimensionKey({
        records,
        metadata: resolvedMetadata,
        preferredDimension: normalizedVisualization.groupBy,
        excludeKeys: [dimensionKey, ...measureKeys].filter(Boolean),
      })
    : "";

  if (chartType === "stat") {
    const stat = buildStatModel({
      records,
      measureKey: measureKeys[0],
      dimensionKey,
    });
    if (!stat) {
      return (
        <ChartEmpty
          title={title}
          chartType={chartType}
          message="A single numeric value was not available for this metric card."
        />
      );
    }
    return (
      <ChartPanelShell
        title={title || "Metric"}
        subtitle={normalizedVisualization?.subtitle}
        chartType={chartType}
        style={chartShellStyle}
      >
        <StatCard stat={stat} title={title} />
      </ChartPanelShell>
    );
  }

  if (!dimensionKey) {
    return (
      <ChartEmpty
        title={title}
        chartType={chartType}
        message="This result does not include a usable categorical dimension for charting."
      />
    );
  }

  if (chartType === "pie" || chartType === "donut") {
    const slices = buildPieModel({
      records,
      dimensionKey,
      measureKey: measureKeys[0],
      chartType,
    });
    if (slices.length === 0) {
      return (
        <ChartEmpty
          title={title}
          chartType={chartType}
          message="No categorical values were available for a pie or donut chart."
        />
      );
    }
    return (
      <ChartPanelShell
        title={title || "Chart preview"}
        subtitle={normalizedVisualization?.subtitle}
        chartType={chartType}
        tooltip={tooltip}
        onPointerLeave={clearVisualInteraction}
        style={chartShellStyle}
      >
        <PieChart
          slices={slices}
          palette={palette}
          chartId={chartId}
          chartType={chartType}
          measureKey={measureKeys[0]}
          title={title}
          activeSliceKey={emphasisKey}
          onHoverSliceKey={setHoveredKey}
          onClearHoverSliceKey={clearHoverState}
          onToggleSlice={handleToggleKey}
          onTooltipChange={setTooltip}
          onTooltipClear={() => setTooltip(null)}
        />
      </ChartPanelShell>
    );
  }

  const model = aggregateCategorySeries({
    records,
    dimensionKey,
    measureKeys,
    groupKey,
    chartType: chartType === "stacked-bar" ? "stacked-bar" : chartType,
  });

  if (model.categories.length === 0 || model.series.length === 0) {
    return (
      <ChartEmpty
        title={title}
        chartType={chartType}
        message="The visualization payload did not line up with the returned rows."
      />
    );
  }

  return (
    <ChartPanelShell
      title={title || "Chart preview"}
      subtitle={normalizedVisualization?.subtitle}
      chartType={chartType}
      tooltip={tooltip}
      onPointerLeave={clearVisualInteraction}
      style={chartShellStyle}
    >
      {chartType === "line" || chartType === "area" ? (
        <LineLikeChart
          model={model}
          palette={palette}
          chartId={chartId}
          chartType={chartType}
          xLabel={dimensionKey}
          yLabel={measureKeys.map((key) => toDisplayLabel(key, { stripMeasurePrefix: true })).join(", ")}
          activeSeriesKey={emphasisKey}
          onDatumHover={setTooltip}
          onDatumLeave={() => setTooltip(null)}
          onToggleSeries={handleToggleKey}
          onHoverSeries={setHoveredKey}
          onClearHoverSeries={clearHoverState}
        />
      ) : (
        <BarLikeChart
          model={model}
          palette={palette}
          chartId={chartId}
          chartType={chartType === "stacked-bar" ? "stacked-bar" : "bar"}
          xLabel={dimensionKey}
          yLabel={measureKeys.map((key) => toDisplayLabel(key, { stripMeasurePrefix: true })).join(", ")}
          activeSeriesKey={emphasisKey}
          onDatumHover={setTooltip}
          onDatumLeave={() => setTooltip(null)}
          onToggleSeries={handleToggleKey}
          onHoverSeries={setHoveredKey}
          onClearHoverSeries={clearHoverState}
        />
      )}
    </ChartPanelShell>
  );
}
