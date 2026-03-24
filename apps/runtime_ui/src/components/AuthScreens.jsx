import { useState } from "react";

function AuthLayout({ eyebrow, title, description, children, footer }) {
  return (
    <main className="auth-shell">
      <section className="auth-panel">
        <p className="eyebrow">{eyebrow}</p>
        <h1>{title}</h1>
        <p className="auth-copy">{description}</p>
        {children}
        {footer ? <div className="auth-footer">{footer}</div> : null}
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
      footer={<div className="spinner" aria-hidden="true" />}
    >
      <div className="empty-box">Preparing the runtime-first shell.</div>
    </AuthLayout>
  );
}

export function ErrorScreen({ error, onRetry }) {
  return (
    <AuthLayout
      eyebrow="Runtime Error"
      title="Unable to load runtime auth state"
      description="The UI could not verify bootstrap or session status against the local runtime API."
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
      description="This runtime is self-hosted and local auth is enabled. Bootstrap a single admin account, then the UI will sign in automatically."
    >
      <form className="form-grid" onSubmit={handleSubmit}>
        <label className="field">
          <span>Username</span>
          <input
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
        <button className="primary-button" type="submit" disabled={submitting}>
          {submitting ? "Creating admin..." : "Create admin account"}
        </button>
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
      description="Local auth is enabled for this runtime. Use the bootstrapped administrator account to access the runtime-first UI."
    >
      <form className="form-grid" onSubmit={handleSubmit}>
        <label className="field">
          <span>Username or email</span>
          <input
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
        <button className="primary-button" type="submit" disabled={submitting}>
          {submitting ? "Signing in..." : "Sign in"}
        </button>
      </form>
    </AuthLayout>
  );
}

export function UnsupportedAuthScreen({ authStatus, onRetry }) {
  return (
    <AuthLayout
      eyebrow="Unsupported Browser Auth"
      title="This runtime is not using local session auth"
      description="The runtime UI can bootstrap and sign in only when auth is disabled or when the runtime uses local browser-managed sessions."
      footer={
        <button className="ghost-button" type="button" onClick={onRetry}>
          Retry
        </button>
      }
    >
      <dl className="meta-grid">
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
          <dd>{authStatus?.detail || "Provide a bearer token or JWT outside the browser session flow."}</dd>
        </div>
      </dl>
    </AuthLayout>
  );
}
