import { ResultTable } from "../ResultTable.jsx";

export function AnalyticsResultTable({ result, maxPreviewRows = 12 }) {
  return <ResultTable result={result} maxPreviewRows={maxPreviewRows} />;
}
