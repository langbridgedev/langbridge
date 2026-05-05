import { KeyRound, RefreshCw, ShieldCheck, ShieldOff, UserPlus, Users } from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { createActor, fetchActors, resetActorPassword, updateActor } from "../../lib/runtimeApi.js";
import { formatDateTime, getErrorMessage } from "../../lib/format.js";
import {
  RUNTIME_ROLES,
  actorStatusLabel,
  buildCreateActorPayload,
  buildResetPasswordPayload,
  buildSecurityCreateForm,
  buildSecurityPasswordForm,
  buildUpdateActorPayload,
  hasRuntimeAdminRole,
  normalizeActorList,
  roleLabel,
  updateRoleList,
} from "./securityModel.js";

export function SecurityManagementPage({ authStatus, session }) {
  const authEnabled = Boolean(authStatus?.auth_enabled);
  const localLoginAllowed = Boolean(authStatus?.login_allowed);
  const isAdmin = hasRuntimeAdminRole(session?.roles);
  const canManage = authEnabled && localLoginAllowed && isAdmin;
  const [actors, setActors] = useState([]);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState("");
  const [selectedActorId, setSelectedActorId] = useState("");
  const [createForm, setCreateForm] = useState(buildSecurityCreateForm);
  const [accessForm, setAccessForm] = useState({ roles: ["viewer"], status: "active" });
  const [passwordForm, setPasswordForm] = useState(buildSecurityPasswordForm);
  const [createState, setCreateState] = useState(actionState);
  const [accessState, setAccessState] = useState(actionState);
  const [passwordState, setPasswordState] = useState(actionState);

  const selectedActor = useMemo(
    () => actors.find((actor) => actor.id === selectedActorId) || actors[0] || null,
    [actors, selectedActorId],
  );

  useEffect(() => {
    if (!canManage) {
      setActors([]);
      setSelectedActorId("");
      return;
    }
    void loadActors();
  }, [canManage]);

  useEffect(() => {
    if (!selectedActor) {
      setSelectedActorId("");
      setAccessForm({ roles: ["viewer"], status: "active" });
      setPasswordForm(buildSecurityPasswordForm());
      return;
    }
    if (!selectedActorId || !actors.some((actor) => actor.id === selectedActorId)) {
      setSelectedActorId(selectedActor.id);
    }
    setAccessForm({
      roles: selectedActor.roles.length > 0 ? selectedActor.roles : ["viewer"],
      status: selectedActor.status || "active",
    });
    setPasswordForm(buildSecurityPasswordForm());
    setAccessState(actionState());
    setPasswordState(actionState());
  }, [actors, selectedActor, selectedActorId]);

  async function loadActors({ focusActorId = "" } = {}) {
    if (!canManage) {
      return;
    }
    setLoading(true);
    setLoadError("");
    try {
      const payload = await fetchActors();
      const nextActors = normalizeActorList(payload);
      setActors(nextActors);
      setSelectedActorId(
        focusActorId && nextActors.some((actor) => actor.id === focusActorId)
          ? focusActorId
          : nextActors[0]?.id || "",
      );
    } catch (caughtError) {
      setActors([]);
      setLoadError(getErrorMessage(caughtError));
    } finally {
      setLoading(false);
    }
  }

  async function handleCreateActor(event) {
    event.preventDefault();
    setCreateState({ submitting: true, error: "", success: "" });
    try {
      const createdActor = await createActor(buildCreateActorPayload(createForm));
      setCreateForm(buildSecurityCreateForm());
      setCreateState({
        submitting: false,
        error: "",
        success: `Created runtime user ${createdActor.username}.`,
      });
      await loadActors({ focusActorId: String(createdActor.id || "") });
    } catch (caughtError) {
      setCreateState({ submitting: false, error: getErrorMessage(caughtError), success: "" });
    }
  }

  async function handleSaveAccess(event) {
    event.preventDefault();
    if (!selectedActor) {
      return;
    }
    setAccessState({ submitting: true, error: "", success: "" });
    try {
      const updatedActor = await updateActor(selectedActor.id, buildUpdateActorPayload(accessForm));
      setAccessState({
        submitting: false,
        error: "",
        success: `Updated access for ${updatedActor.username}.`,
      });
      await loadActors({ focusActorId: String(updatedActor.id || "") });
    } catch (caughtError) {
      setAccessState({ submitting: false, error: getErrorMessage(caughtError), success: "" });
    }
  }

  async function handleResetPassword(event) {
    event.preventDefault();
    if (!selectedActor) {
      return;
    }
    setPasswordState({ submitting: true, error: "", success: "" });
    try {
      const updatedActor = await resetActorPassword(
        selectedActor.id,
        buildResetPasswordPayload(passwordForm),
      );
      setPasswordForm(buildSecurityPasswordForm());
      setPasswordState({
        submitting: false,
        error: "",
        success: `Reset password for ${updatedActor.username}.`,
      });
      await loadActors({ focusActorId: String(updatedActor.id || "") });
    } catch (caughtError) {
      setPasswordState({ submitting: false, error: getErrorMessage(caughtError), success: "" });
    }
  }

  if (!authEnabled) {
    return (
      <SecurityUnavailable
        title="Runtime auth is disabled"
        message="This runtime is running in single-user mode, so there are no local users to manage."
        authStatus={authStatus}
      />
    );
  }

  if (!localLoginAllowed) {
    return (
      <SecurityUnavailable
        title="Local browser login is disabled"
        message={authStatus?.detail || "This runtime is using a secured auth mode without local operator sessions."}
        authStatus={authStatus}
      />
    );
  }

  if (!isAdmin) {
    return (
      <SecurityUnavailable
        title="Admin access required"
        message="This session can use the runtime UI but cannot manage local runtime users."
        authStatus={authStatus}
      />
    );
  }

  return (
    <div className="security-management">
      <section className="security-summary-grid">
        <SecurityMetric
          icon={<Users className="button-icon" aria-hidden="true" />}
          label="Local users"
          value={actors.length}
          detail="Browser users with runtime access."
        />
        <SecurityMetric
          icon={<ShieldCheck className="button-icon" aria-hidden="true" />}
          label="Active"
          value={actors.filter((actor) => actor.status !== "disabled").length}
          detail="Users currently allowed to sign in."
        />
        <SecurityMetric
          icon={<KeyRound className="button-icon" aria-hidden="true" />}
          label="Auth mode"
          value={authStatus?.auth_mode || "runtime"}
          detail={authStatus?.session_cookie_name || "Local operator session"}
        />
      </section>

      {loadError ? <div className="resource-error">{loadError}</div> : null}

      <section className="security-layout">
        <section className="security-panel">
          <header className="security-panel-head">
            <div>
              <span>Inventory</span>
              <strong>Runtime users</strong>
            </div>
            <button type="button" onClick={() => void loadActors()} disabled={loading}>
              <RefreshCw className="button-icon" aria-hidden="true" />
              {loading ? "Refreshing" : "Refresh"}
            </button>
          </header>

          {loading && actors.length === 0 ? <div className="connector-empty-note">Loading runtime users...</div> : null}
          {!loading && actors.length === 0 ? <div className="connector-empty-note">No runtime users found.</div> : null}
          <div className="security-user-list">
            {actors.map((actor) => (
              <button
                key={actor.id}
                className={`security-user-card ${selectedActor?.id === actor.id ? "active" : ""}`.trim()}
                type="button"
                onClick={() => setSelectedActorId(actor.id)}
              >
                <div>
                  <strong>{actor.display_name || actor.username}</strong>
                  <span>@{actor.username}</span>
                </div>
                <span className={`security-status-pill ${actor.status === "disabled" ? "disabled" : "active"}`.trim()}>
                  {actorStatusLabel(actor.status)}
                </span>
                <small>{actor.email || "No email"}</small>
                <div className="resource-chip-row">
                  {actor.roles.map((role) => <span key={`${actor.id}-${role}`}>{roleLabel(role)}</span>)}
                </div>
                <small>Password updated {formatDateTime(actor.password_updated_at)}</small>
              </button>
            ))}
          </div>
        </section>

        <div className="security-detail-stack">
          <section className="security-panel">
            <header className="security-panel-head">
              <div>
                <span>Local auth</span>
                <strong>Create user</strong>
              </div>
            </header>
            <form className="security-form-grid" onSubmit={handleCreateActor}>
              <TextField
                label="Username"
                value={createForm.username}
                onChange={(value) => setCreateForm((current) => ({ ...current, username: value }))}
                placeholder="analyst-one"
                disabled={createState.submitting}
                required
              />
              <TextField
                label="Email"
                type="email"
                value={createForm.email}
                onChange={(value) => setCreateForm((current) => ({ ...current, email: value }))}
                placeholder="analyst@example.com"
                disabled={createState.submitting}
                required
              />
              <TextField
                label="Display name"
                value={createForm.display_name}
                onChange={(value) => setCreateForm((current) => ({ ...current, display_name: value }))}
                placeholder="Analyst One"
                disabled={createState.submitting}
                full
              />
              <TextField
                label="Temporary password"
                type="password"
                value={createForm.password}
                onChange={(value) => setCreateForm((current) => ({ ...current, password: value }))}
                placeholder="At least 8 characters"
                disabled={createState.submitting}
                autoComplete="new-password"
                required
                full
              />
              <RolePicker
                label="Roles"
                roles={createForm.roles}
                disabled={createState.submitting}
                onToggle={(role) =>
                  setCreateForm((current) => ({ ...current, roles: updateRoleList(current.roles, role) }))
                }
              />
              <ActionState state={createState} />
              <div className="security-form-actions">
                <button type="submit" disabled={createState.submitting}>
                  <UserPlus className="button-icon" aria-hidden="true" />
                  {createState.submitting ? "Creating..." : "Create user"}
                </button>
              </div>
            </form>
          </section>

          <section className="security-panel">
            <header className="security-panel-head">
              <div>
                <span>Access</span>
                <strong>{selectedActor ? `Manage ${selectedActor.username}` : "Select a user"}</strong>
              </div>
            </header>

            {!selectedActor ? (
              <div className="connector-empty-note">Choose a runtime user to update roles, disable access, or reset the password.</div>
            ) : (
              <div className="security-detail-stack">
                <div className="security-user-summary">
                  <strong>{selectedActor.display_name || selectedActor.username}</strong>
                  <span>{selectedActor.email || "No email"}</span>
                  <div className="inline-notes">
                    <span>{selectedActor.actor_type}</span>
                    <span>{actorStatusLabel(selectedActor.status)}</span>
                    <span>Updated {formatDateTime(selectedActor.updated_at)}</span>
                  </div>
                </div>

                <form className="security-detail-stack" onSubmit={handleSaveAccess}>
                  <RolePicker
                    label="Roles"
                    roles={accessForm.roles}
                    disabled={accessState.submitting}
                    onToggle={(role) =>
                      setAccessForm((current) => ({ ...current, roles: updateRoleList(current.roles, role) }))
                    }
                  />
                  <div className="security-inline-actions">
                    <button
                      className={accessForm.status === "disabled" ? "" : "danger"}
                      type="button"
                      disabled={accessState.submitting}
                      onClick={() =>
                        setAccessForm((current) => ({
                          ...current,
                          status: current.status === "disabled" ? "active" : "disabled",
                        }))
                      }
                    >
                      {accessForm.status === "disabled" ? (
                        <>
                          <ShieldCheck className="button-icon" aria-hidden="true" />
                          Enable user
                        </>
                      ) : (
                        <>
                          <ShieldOff className="button-icon" aria-hidden="true" />
                          Disable user
                        </>
                      )}
                    </button>
                    <span className={`security-status-pill ${accessForm.status === "disabled" ? "disabled" : "active"}`.trim()}>
                      {actorStatusLabel(accessForm.status)}
                    </span>
                  </div>
                  <ActionState state={accessState} />
                  <div className="security-form-actions">
                    <button type="submit" disabled={accessState.submitting}>
                      {accessState.submitting ? "Saving..." : "Save access"}
                    </button>
                  </div>
                </form>

                <form className="security-detail-stack" onSubmit={handleResetPassword}>
                  <TextField
                    label="New password"
                    type="password"
                    value={passwordForm.password}
                    onChange={(value) => setPasswordForm((current) => ({ ...current, password: value }))}
                    placeholder="Set a new local password"
                    disabled={passwordState.submitting}
                    autoComplete="new-password"
                    required
                  />
                  <label className="security-checkbox">
                    <input
                      type="checkbox"
                      checked={passwordForm.must_rotate_password}
                      disabled={passwordState.submitting}
                      onChange={(event) =>
                        setPasswordForm((current) => ({
                          ...current,
                          must_rotate_password: event.target.checked,
                        }))
                      }
                    />
                    <span>Require password rotation on next admin review</span>
                  </label>
                  <ActionState state={passwordState} />
                  <div className="security-form-actions">
                    <button type="submit" disabled={passwordState.submitting}>
                      <KeyRound className="button-icon" aria-hidden="true" />
                      {passwordState.submitting ? "Resetting..." : "Reset password"}
                    </button>
                  </div>
                </form>
              </div>
            )}
          </section>
        </div>
      </section>
    </div>
  );
}

function SecurityUnavailable({ title, message, authStatus }) {
  return (
    <section className="security-unavailable">
      <div>
        <span>Security</span>
        <strong>{title}</strong>
        <p>{message}</p>
      </div>
      <dl className="resource-definition-list">
        <div><dt>Auth enabled</dt><dd>{authStatus?.auth_enabled ? "Yes" : "No"}</dd></div>
        <div><dt>Auth mode</dt><dd>{authStatus?.auth_mode || "none"}</dd></div>
        <div><dt>Login allowed</dt><dd>{authStatus?.login_allowed ? "Yes" : "No"}</dd></div>
      </dl>
    </section>
  );
}

function SecurityMetric({ icon, label, value, detail }) {
  return (
    <article className="security-metric-card">
      <span className="security-metric-icon">{icon}</span>
      <div>
        <small>{label}</small>
        <strong>{value}</strong>
        <span>{detail}</span>
      </div>
    </article>
  );
}

function TextField({
  label,
  value,
  onChange,
  type = "text",
  placeholder = "",
  disabled = false,
  required = false,
  full = false,
  autoComplete = "off",
}) {
  return (
    <label className={`security-field ${full ? "security-field--full" : ""}`.trim()}>
      <span>{label}</span>
      <input
        type={type}
        value={value}
        placeholder={placeholder}
        disabled={disabled}
        required={required}
        autoComplete={autoComplete}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  );
}

function RolePicker({ label, roles, disabled, onToggle }) {
  return (
    <div className="security-field security-field--full">
      <span>{label}</span>
      <div className="security-role-list">
        {RUNTIME_ROLES.map((role) => (
          <button
            key={role.value}
            className={roles.includes(role.value) ? "active" : ""}
            type="button"
            disabled={disabled}
            onClick={() => onToggle(role.value)}
          >
            {role.label}
          </button>
        ))}
      </div>
    </div>
  );
}

function ActionState({ state }) {
  if (state.error) {
    return <div className="resource-error security-field--full">{state.error}</div>;
  }
  if (state.success) {
    return <div className="security-success security-field--full">{state.success}</div>;
  }
  return null;
}

function actionState() {
  return { submitting: false, error: "", success: "" };
}
