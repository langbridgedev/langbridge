import { useState } from "react";
import { Box, KeyRound, LoaderCircle, ShieldAlert, ShieldCheck, UserRoundPlus } from "lucide-react";

function AuthLayout({ eyebrow, title, description, children, footer, highlights = [], notice, icon: Icon = Box }) {
  return (
    <main className="auth-shell">
      <section className="auth-panel auth-panel-shell">
        <div className="auth-panel-intro">
          <div className="auth-brand">
            <span className="auth-brand-mark" aria-hidden="true">
              <Box className="auth-brand-icon" />
            </span>
            <div>
              <p className="auth-brand-label">Langbridge Runtime</p>
              <strong>Runtime access</strong>
            </div>
          </div>
          <span className="auth-kicker">
            <Icon className="auth-kicker-icon" aria-hidden="true" />
            {eyebrow}
          </span>
          <div className="auth-intro-copy">
            <h1>{title}</h1>
            <p className="auth-copy">{description}</p>
          </div>
          {highlights.length > 0 ? (
            <div className="auth-highlight-grid">
              {highlights.map((item) => (
                <div key={`${item.label}-${item.value}`} className="auth-highlight-card">
                  <span>{item.label}</span>
                  <strong>{item.value}</strong>
                </div>
              ))}
            </div>
          ) : null}
        </div>

        <div className="auth-panel-main">
          {notice ? <div className="callout auth-notice">{notice}</div> : null}
          {children}
          {footer ? <div className="auth-footer">{footer}</div> : null}
        </div>
      </section>
    </main>
  );
}

export function LoadingScreen({ title = "Loading runtime UI...", description = "Checking runtime session and bootstrap status." }) {
  return (
    <AuthLayout
      eyebrow="Langbridge Runtime"
      title={title}
      description={description}
      icon={LoaderCircle}
      highlights={[
        { label: "Stage", value: "Session handshake" },
        { label: "Scope", value: "Single runtime workspace" },
        { label: "Flow", value: "Bootstrap then login" },
      ]}
      footer={<div className="spinner" aria-hidden="true" />}
    >
      <div className="empty-box auth-empty-state">
        <strong>Preparing the runtime-first shell</strong>
        <span>Validating the runtime auth mode and any existing local operator session.</span>
      </div>
    </AuthLayout>
  );
}

export function ErrorScreen({ error, onRetry }) {
  return (
    <AuthLayout
      eyebrow="Runtime Error"
      title="Unable to load runtime auth state"
      description="The UI could not verify bootstrap or session status against the local runtime API."
      icon={ShieldAlert}
      highlights={[
        { label: "Area", value: "Runtime auth status" },
        { label: "Action", value: "Retry after API recovery" },
      ]}
      footer={
        <button className="primary-button" type="button" onClick={onRetry}>
          Retry
        </button>
      }
    >
      <div className="error-banner">{error}</div>
    </AuthLayout>
  );
}

export function BootstrapScreen({ error, submitting, onSubmit }) {
  const [form, setForm] = useState({
    username: "",
    email: "",
    password: "",
  });

  function updateField(event) {
    const { name, value } = event.target;
    setForm((current) => ({
      ...current,
      [name]: value,
    }));
  }

  function handleSubmit(event) {
    event.preventDefault();
    onSubmit(form);
  }

  return (
    <AuthLayout
      eyebrow="Bootstrap Admin"
      title="Create the first runtime administrator"
      description="This secured self-hosted runtime needs its first local operator account. Bootstrap a single runtime admin, then the UI will sign in automatically."
      icon={UserRoundPlus}
      highlights={[
        { label: "Role", value: "Initial runtime admin" },
        { label: "Mode", value: "Local operator session" },
        { label: "Next", value: "Automatic sign-in" },
      ]}
      notice={
        <>
          <strong>One-time setup</strong>
          <span>This bootstrap flow is available only until the first local administrator account is created.</span>
        </>
      }
    >
      <form className="form-grid auth-form" onSubmit={handleSubmit}>
        <label className="field">
          <span>Username</span>
          <input
            className="text-input"
            name="username"
            type="text"
            autoComplete="username"
            value={form.username}
            onChange={updateField}
            placeholder="runtime-admin"
            disabled={submitting}
            required
          />
        </label>
        <label className="field">
          <span>Email</span>
          <input
            className="text-input"
            name="email"
            type="email"
            autoComplete="email"
            value={form.email}
            onChange={updateField}
            placeholder="admin@example.com"
            disabled={submitting}
            required
          />
        </label>
        <label className="field">
          <span>Password</span>
          <input
            className="text-input"
            name="password"
            type="password"
            autoComplete="new-password"
            value={form.password}
            onChange={updateField}
            placeholder="At least 8 characters"
            disabled={submitting}
            required
          />
        </label>
        {error ? <div className="error-banner">{error}</div> : null}
        <div className="auth-action-row">
          <button className="primary-button" type="submit" disabled={submitting}>
            {submitting ? "Creating admin..." : "Create admin account"}
          </button>
        </div>
      </form>
    </AuthLayout>
  );
}

export function LoginScreen({ error, submitting, onSubmit }) {
  const [form, setForm] = useState({
    identifier: "",
    password: "",
  });

  function updateField(event) {
    const { name, value } = event.target;
    setForm((current) => ({
      ...current,
      [name]: value,
    }));
  }

  function handleSubmit(event) {
    event.preventDefault();
    onSubmit(form);
  }

  return (
    <AuthLayout
      eyebrow="Runtime Login"
      title="Sign in to the local runtime"
      description="Use the bootstrapped local operator account to access this single-workspace runtime UI."
      icon={KeyRound}
      highlights={[
        { label: "Access", value: "Local operator session" },
        { label: "Workspace", value: "Runtime-first UI" },
        { label: "Auth", value: "Browser sign-in enabled" },
      ]}
    >
      <form className="form-grid auth-form" onSubmit={handleSubmit}>
        <label className="field field-full">
          <span>Username or email</span>
          <input
            className="text-input"
            name="identifier"
            type="text"
            autoComplete="username"
            value={form.identifier}
            onChange={updateField}
            placeholder="runtime-admin or admin@example.com"
            disabled={submitting}
            required
          />
        </label>
        <label className="field">
          <span>Password</span>
          <input
            className="text-input"
            name="password"
            type="password"
            autoComplete="current-password"
            value={form.password}
            onChange={updateField}
            placeholder="Your runtime password"
            disabled={submitting}
            required
          />
        </label>
        {error ? <div className="error-banner">{error}</div> : null}
        <div className="auth-action-row">
          <button className="primary-button" type="submit" disabled={submitting}>
            {submitting ? "Signing in..." : "Sign in"}
          </button>
        </div>
      </form>
    </AuthLayout>
  );
}

export function UnsupportedAuthScreen({ authStatus, onRetry }) {
  return (
    <AuthLayout
      eyebrow="Bearer-Only Auth"
      title="This runtime does not allow local browser login"
      description="The runtime UI can bootstrap and sign in only when local operator sessions are enabled alongside the configured runtime auth mode."
      icon={ShieldCheck}
      highlights={[
        { label: "Configured mode", value: authStatus?.auth_mode || "unknown" },
        { label: "Browser login", value: authStatus?.login_allowed ? "Enabled" : "Disabled" },
      ]}
      footer={
        <button className="ghost-button" type="button" onClick={onRetry}>
          Retry
        </button>
      }
    >
      <dl className="meta-grid auth-meta-grid">
        <div>
          <dt>Configured auth mode</dt>
          <dd>{authStatus?.auth_mode || "unknown"}</dd>
        </div>
        <div>
          <dt>Browser login support</dt>
          <dd>{authStatus?.login_allowed ? "enabled" : "disabled"}</dd>
        </div>
        <div>
          <dt>Runtime note</dt>
          <dd>{authStatus?.detail || "Use a bearer token or enable local operator sessions for browser access."}</dd>
        </div>
      </dl>
    </AuthLayout>
  );
}
