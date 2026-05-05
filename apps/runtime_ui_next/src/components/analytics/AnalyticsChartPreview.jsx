import { ChartPreview } from "../ChartPreview.jsx";

export function AnalyticsChartPreview({
  title,
  result,
  metadata,
  visualization,
  preferredDimension,
  preferredMeasure,
  themeColors,
}) {
  return (
    <ChartPreview
      title={title}
      result={result}
      metadata={metadata}
      visualization={visualization}
      preferredDimension={preferredDimension}
      preferredMeasure={preferredMeasure}
      themeColors={themeColors}
    />
  );
}
