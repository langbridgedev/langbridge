import { classNames } from "../../utils/classNames.js";

export function ManagementPill({ mode }) {
  const label = mode === "runtime_managed" ? "Runtime managed" : "Config managed";
  return <span className={classNames("management-pill", mode)}>{label}</span>;
}
