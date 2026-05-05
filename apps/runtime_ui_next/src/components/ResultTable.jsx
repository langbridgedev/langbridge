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

function normalizeRow(columns, row) {
  if (Array.isArray(row)) {
    return columns.length > 0 ? columns.map((_, index) => row[index]) : row;
  }
  if (row && typeof row === "object") {
    const record = row;
    return columns.length > 0 ? columns.map((column) => record[column]) : Object.values(record);
  }
  return [row];
}

function formatElapsed(value) {
  if (value === null || value === undefined) {
    return "";
  }
  if (value < 1000) {
    return `${value} ms`;
  }
  return `${(value / 1000).toFixed(2)} s`;
}

function columnFormatting(result, column) {
  const columns = result?.formatting?.columns;
  if (!columns || typeof columns !== "object") {
    return null;
  }
  return columns[column] || columns[String(column || "").trim()] || null;
}

export function ResultTable({ result, maxPreviewRows = 12 }) {
  const columns = Array.isArray(result?.columns)
    ? result.columns.map((column, index) => toColumnName(column, index))
    : [];
  const rows = Array.isArray(result?.rows) ? result.rows : [];
  const previewRows = rows.slice(0, maxPreviewRows);
  const normalizedRows = previewRows.map((row) => normalizeRow(columns, row));
  const visibleColumns =
    columns.length > 0
      ? columns
      : normalizedRows[0]
        ? normalizedRows[0].map((_, index) => `Column ${index + 1}`)
        : [];

  if (visibleColumns.length === 0) {
    return <div className="empty-box">No tabular data is available for this response.</div>;
  }

  return (
    <div className="result-table-shell">
      <div className="table-wrap">
        <table className="result-table">
          <thead>
            <tr>
              {visibleColumns.map((column) => (
                <th key={column}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {normalizedRows.length > 0 ? (
              normalizedRows.map((row, rowIndex) => (
                <tr key={rowIndex}>
                  {row.map((value, valueIndex) => (
                    <td key={`${rowIndex}-${valueIndex}`}>
                      {formatValue(value, columnFormatting(result, visibleColumns[valueIndex]))}
                    </td>
                  ))}
                </tr>
              ))
            ) : (
              <tr>
                <td className="result-table-empty-cell" colSpan={visibleColumns.length}>
                  No preview rows were returned for this result.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <div className="table-meta">
        {result?.rowCount !== undefined && result?.rowCount !== null ? (
          <span>{Number(result.rowCount).toLocaleString()} rows</span>
        ) : null}
        {result?.row_count_preview !== undefined && result?.row_count_preview !== null ? (
          <span>{Number(result.row_count_preview).toLocaleString()} preview rows</span>
        ) : null}
        {result?.duration_ms !== undefined && result?.duration_ms !== null ? (
          <span>{formatElapsed(result.duration_ms)}</span>
        ) : null}
        {rows.length > maxPreviewRows ? (
          <span>{(rows.length - maxPreviewRows).toLocaleString()} more rows hidden</span>
        ) : null}
      </div>
    </div>
  );
}
