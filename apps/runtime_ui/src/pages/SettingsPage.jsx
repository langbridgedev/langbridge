import { useAsyncData } from "../hooks/useAsyncData";
import { fetchRuntimeInfo } from "../lib/runtimeApi";
import { formatList, formatValue } from "../lib/format";
import { DetailList, PageEmpty, Panel } from "../components/PagePrimitives";

export function SettingsPage({ authStatus, session }) {
  const { data, loading, error, reload } = useAsyncData(fetchRuntimeInfo);
  const info = data || {};

  return (
    <div className="page-stack">
      <section className="surface-panel product-command-bar">
        <div className="product-command-bar-main">
          <div className="product-command-bar-copy">
            <p className="eyebrow">Settings</p>
            <h2>Runtime identity</h2>
            <div className="product-command-bar-meta">
              <span className="chip">{formatValue(info.runtime_mode || "unknown")}</span>
              <span className="chip">{authStatus?.auth_enabled ? authStatus.auth_mode : "Auth disabled"}</span>
              <span className="chip">{session?.username || "runtime"}</span>
            </div>
          </div>
          <div className="product-command-bar-actions">
            <button className="ghost-button" type="button" onClick={reload} disabled={loading}>
              {loading ? "Refreshing..." : "Refresh runtime info"}
            </button>
          </div>
        </div>
      </section>

      <Panel
        title="Runtime settings"
        className="compact-panel"
      >
        {error ? <div className="error-banner">{error}</div> : null}
        {loading ? (
          <div className="empty-box">Loading runtime info...</div>
        ) : (
          <DetailList
            items={[
              { label: "Runtime mode", value: formatValue(info.runtime_mode) },
              { label: "Config path", value: formatValue(info.config_path) },
              { label: "Workspace ID", value: formatValue(info.workspace_id) },
              { label: "Actor ID", value: formatValue(info.actor_id) },
              { label: "Default semantic model", value: formatValue(info.default_semantic_model) },
              { label: "Default agent", value: formatValue(info.default_agent) },
            ]}
          />
        )}
      </Panel>

      <section className="summary-grid">
        <Panel title="Session" className="compact-panel">
          <DetailList
            items={[
              { label: "Auth enabled", value: formatValue(authStatus?.auth_enabled) },
              { label: "Auth mode", value: formatValue(authStatus?.auth_mode) },
              { label: "Bootstrap required", value: formatValue(authStatus?.bootstrap_required) },
              { label: "Has admin", value: formatValue(authStatus?.has_admin) },
              { label: "Login allowed", value: formatValue(authStatus?.login_allowed) },
              { label: "User", value: formatValue(session?.username || "runtime") },
              { label: "Email", value: formatValue(session?.email) },
              { label: "Roles", value: formatList(session?.roles) },
            ]}
          />
        </Panel>

        <Panel title="Capabilities" className="compact-panel">
          {Array.isArray(info.capabilities) && info.capabilities.length > 0 ? (
            <div className="tag-list">
              {info.capabilities.map((item) => (
                <span key={item} className="tag">
                  {item}
                </span>
              ))}
            </div>
          ) : (
            <PageEmpty
              title="No capabilities"
              message="The runtime did not expose capability metadata."
            />
          )}
        </Panel>
      </section>
    </div>
  );
}
