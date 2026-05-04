import { useState } from "react";

function AuthLayout({ eyebrow, title, description, children, footer, error }) {
  return (
    <main className="auth-shell">
      <section className="auth-panel">
        <div className="auth-panel-intro">
          <div className="auth-brand">
            <span className="auth-brand-mark">L</span>
            <div>
              <p className="auth-brand-label">Langbridge Runtime</p>
              <strong>Analyst workspace</strong>
            </div>
          </div>
          <span className="auth-kicker">{eyebrow}</span>
          <div className="auth-intro-copy">
            <h1>{title}</h1>
            <p>{description}</p>
          </div>
          <div className="auth-highlight-grid">
            <div><span>Surface</span><strong>Chat-first</strong></div>
            <div><span>Data</span><strong>Governed</strong></div>
            <div><span>Mode</span><strong>Runtime API</strong></div>
          </div>
        </div>

        <div className="auth-panel-main">
          {error ? <div className="auth-error">{error}</div> : null}
          {children}
          {footer ? <div className="auth-footer">{footer}</div> : null}
        </div>
      </section>
    </main>
  );
}

export function LoadingScreen() {
  return (
    <AuthLayout
      eyebrow="Runtime session"
      title="Checking access"
      description="Verifying the Langbridge runtime auth mode and browser session."
    >
      <div className="auth-empty-state">Loading runtime auth state...</div>
    </AuthLayout>
  );
}

export function ErrorScreen({ error, onRetry }) {
  return (
    <AuthLayout
      eyebrow="Connection"
      title="Unable to reach the runtime"
      description="Start the Langbridge runtime and retry. In local development, /api requests are proxied to the runtime."
      error={error}
      footer={<button type="button" onClick={onRetry}>Retry</button>}
    >
      <div className="auth-empty-state">The UI will use real API resources when the runtime is reachable.</div>
    </AuthLayout>
  );
}

export function UnsupportedAuthScreen({ authStatus, onRetry }) {
  return (
    <AuthLayout
      eyebrow="Auth mode"
      title="Browser login is not enabled"
      description="This runtime does not allow local browser login. Enable local operator sessions or run with auth disabled."
      footer={<button type="button" onClick={onRetry}>Retry</button>}
    >
      <div className="auth-empty-state">Configured mode: {authStatus?.auth_mode || "unknown"}</div>
    </AuthLayout>
  );
}

export function LoginScreen({ error, submitting, onSubmit }) {
  const [form, setForm] = useState({ identifier: "", password: "" });

  function updateField(event) {
    setForm((current) => ({ ...current, [event.target.name]: event.target.value }));
  }

  function handleSubmit(event) {
    event.preventDefault();
    onSubmit(form);
  }

  return (
    <AuthLayout
      eyebrow="Runtime login"
      title="Sign in to Langbridge"
      description="Use a local runtime operator account to access configured resources and live API data."
      error={error}
    >
      <form className="auth-form" onSubmit={handleSubmit}>
        <label>
          Username or email
          <input name="identifier" value={form.identifier} onChange={updateField} autoComplete="username" required />
        </label>
        <label>
          Password
          <input name="password" type="password" value={form.password} onChange={updateField} autoComplete="current-password" required />
        </label>
        <button type="submit" disabled={submitting}>{submitting ? "Signing in..." : "Sign in"}</button>
      </form>
    </AuthLayout>
  );
}

export function BootstrapScreen({ error, submitting, onSubmit }) {
  const [form, setForm] = useState({ username: "", email: "", password: "" });

  function updateField(event) {
    setForm((current) => ({ ...current, [event.target.name]: event.target.value }));
  }

  function handleSubmit(event) {
    event.preventDefault();
    onSubmit(form);
  }

  return (
    <AuthLayout
      eyebrow="Bootstrap"
      title="Create the first runtime administrator"
      description="This runtime needs an initial local operator account before browser login can be used."
      error={error}
    >
      <form className="auth-form" onSubmit={handleSubmit}>
        <label>
          Username
          <input name="username" value={form.username} onChange={updateField} autoComplete="username" required />
        </label>
        <label>
          Email
          <input name="email" type="email" value={form.email} onChange={updateField} autoComplete="email" required />
        </label>
        <label>
          Password
          <input name="password" type="password" value={form.password} onChange={updateField} autoComplete="new-password" required />
        </label>
        <button type="submit" disabled={submitting}>{submitting ? "Creating..." : "Create admin"}</button>
      </form>
    </AuthLayout>
  );
}
