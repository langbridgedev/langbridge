import { useEffect, useState } from "react";

import { getQueryScopes, getSourceConnectors, runExampleQuery } from "../../services/queryService.js";
import { SqlCodeBlock } from "./SqlCodeBlock.jsx";

export function QueryWorkspacePage({ navigate }) {
  const [queryScope, setQueryScope] = useState("semantic");
  const [connector, setConnector] = useState("snowflake");
  const [scopes, setScopes] = useState([]);
  const [connectors, setConnectors] = useState([]);
  const [result, setResult] = useState(null);

  useEffect(() => {
    void getQueryScopes().then(setScopes);
    void getSourceConnectors().then(setConnectors);
    void runExampleQuery().then(setResult);
  }, []);

  return (
    <section className="workspace-page workspace-page--query query-workspace-minimal">
      <div className="query-minimal-bar">
        <div className="query-title-block">
          <strong>Scratchpad</strong>
          <span>Write, run, and inspect SQL without leaving the analyst workspace.</span>
        </div>
        <div className="query-control-strip">
          <label className="query-control">
            Scope
            <select value={queryScope} onChange={(event) => setQueryScope(event.target.value)}>
              {scopes.map((scope) => <option key={scope.value} value={scope.value}>{scope.label}</option>)}
            </select>
          </label>
          {queryScope === "source" ? (
            <label className="query-control">
              Connector
              <select value={connector} onChange={(event) => setConnector(event.target.value)}>
                {connectors.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
              </select>
            </label>
          ) : null}
          <button type="button" onClick={() => navigate("/chat")}>Chat</button>
          <button type="button">Save</button>
          <button className="primary-action" type="button">Run</button>
        </div>
      </div>

      <div className="query-minimal-grid">
        <div className="editor-panel query-scratchpad-panel">
          <div className="editor-toolbar">
            <span>{queryScope === "source" ? `Source SQL - ${connector}` : `${queryScope} SQL`}</span>
            <div>
              <button type="button">Format</button>
              <button type="button">Copy</button>
            </div>
          </div>
          <SqlCodeBlock />
        </div>

        {result ? <QueryResultPreview result={result} queryScope={queryScope} /> : null}
      </div>
    </section>
  );
}

function QueryResultPreview({ result, queryScope }) {
  return (
    <div className="query-result-panel">
      <div className="query-result-toolbar">
        <div>
          <p className="eyebrow">Result</p>
          <h3>{result.title}</h3>
        </div>
        <div className="query-result-actions">
          <button type="button">Copy rows</button>
          <button type="button">Download CSV</button>
          <button type="button">Export JSON</button>
          <button type="button">Chart</button>
          <button type="button">Explain</button>
        </div>
      </div>
      <div className="query-result-summary">
        <span>{result.metadata.rows} rows</span>
        <span>{result.metadata.columns} columns</span>
        <span>Runtime: {result.metadata.runtime}</span>
        <span>Scope: {queryScope}</span>
      </div>
      <table>
        <thead>
          <tr>{result.columns.map((column) => <th key={column}>{column}</th>)}</tr>
        </thead>
        <tbody>
          {result.rows.map((row) => (
            <tr key={row.join(":")}>
              {row.map((cell) => <td key={cell}>{cell}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
      <div className="query-result-footer">
        <span>Previewing the latest successful run.</span>
        <div>
          <button type="button">Open in chat</button>
          <button type="button">Add to dashboard</button>
        </div>
      </div>
    </div>
  );
}
