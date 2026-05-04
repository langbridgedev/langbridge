import { resolveAsync } from "./runtimeService.js";
import { exampleQueryResult, queryProjects, queryRecents, queryScopes, sourceConnectors } from "../mocks/query.mock.js";

export function listQueryRecents() {
  return resolveAsync(queryRecents);
}

export function listQueryProjects() {
  return resolveAsync(queryProjects);
}

export function getQueryScopes() {
  return resolveAsync(queryScopes);
}

export function getSourceConnectors() {
  return resolveAsync(sourceConnectors);
}

export function runExampleQuery() {
  return resolveAsync(exampleQueryResult);
}
