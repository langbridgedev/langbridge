import {
  AlertTriangle,
  CheckCircle2,
  CircleDot,
  DatabaseZap,
  SearchCode,
  ShieldAlert,
  Sparkles,
  WandSparkles,
} from "lucide-react";

export function formatStageLabel(stage) {
  return String(stage || "working")
    .replaceAll("_", " ")
    .trim();
}

export function formatStageTitle(stage) {
  const label = formatStageLabel(stage);
  return label ? label.charAt(0).toUpperCase() + label.slice(1) : "Working";
}

export function getProgressIcon(stage, status) {
  if (status === "failed") {
    return AlertTriangle;
  }
  switch (stage) {
    case "planning":
      return WandSparkles;
    case "selecting_asset":
      return CircleDot;
    case "generating_sql":
      return SearchCode;
    case "running_query":
      return DatabaseZap;
    case "access_denied":
      return ShieldAlert;
    case "completed":
    case "empty_result":
      return CheckCircle2;
    default:
      return Sparkles;
  }
}
