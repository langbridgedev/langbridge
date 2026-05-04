import { resolveAsync } from "./runtimeService.js";
import { dashboardBoard, dashboardProjects, dashboardRecents } from "../mocks/dashboard.mock.js";

export function listDashboardRecents() {
  return resolveAsync(dashboardRecents);
}

export function listDashboardProjects() {
  return resolveAsync(dashboardProjects);
}

export function getDashboardBoard() {
  return resolveAsync(dashboardBoard);
}
