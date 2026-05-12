import assert from "node:assert/strict";
import test from "node:test";

import {
  buildUnreferencedPrimaryArtifactIds,
  splitMarkdownArtifacts,
} from "./analyticsArtifacts.js";

test("buildUnreferencedPrimaryArtifactIds falls back to primary chart artifacts", () => {
  const parts = splitMarkdownArtifacts("Here is the answer without an artifact placeholder.");
  const artifacts = [
    { id: "primary_result", type: "table", role: "supporting_result" },
    { id: "primary_visualization", type: "chart", role: "primary_result" },
  ];

  assert.deepEqual(buildUnreferencedPrimaryArtifactIds(parts, artifacts), ["primary_visualization"]);
});

test("buildUnreferencedPrimaryArtifactIds does not duplicate referenced artifacts", () => {
  const parts = splitMarkdownArtifacts("{{artifact:primary_visualization}}");
  const artifacts = [
    { id: "primary_visualization", type: "chart", role: "primary_result" },
    { id: "primary_result", type: "table", role: "supporting_result" },
  ];

  assert.deepEqual(buildUnreferencedPrimaryArtifactIds(parts, artifacts), []);
});

test("buildUnreferencedPrimaryArtifactIds falls back to primary table when no chart exists", () => {
  const parts = splitMarkdownArtifacts("Here is the answer.");
  const artifacts = [
    { id: "generated_sql", type: "sql", role: "supporting_result" },
    { id: "primary_result", type: "table", role: "primary_result" },
  ];

  assert.deepEqual(buildUnreferencedPrimaryArtifactIds(parts, artifacts), ["primary_result"]);
});
