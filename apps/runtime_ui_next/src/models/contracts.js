/*
  Frontend contract guide for the POC.

  ChatResponse:
    id, conversationId, role, status, mode, agent, primaryReply, artifacts,
    metadata, diagnostics, actions.

  Artifact:
    id, type, title, payload fields by type.
    Supported POC types: metric_cards, scatter_plot, table.

  QueryRun:
    scope, connector, sqlCanonical, sqlExecutable, result, diagnostics.

  Dashboard:
    id, title, filters, tiles.
    Tiles should carry source, query contract, layout size, visualization config,
    result metadata, and provenance.

  ConfigResource:
    id, name, subtitle, status, management, owner, lastUpdated, runtimeState,
    configDefinition, relationships, details.
*/

export const MANAGEMENT_MODES = {
  runtimeManaged: "runtime_managed",
  configManaged: "config_managed",
};
