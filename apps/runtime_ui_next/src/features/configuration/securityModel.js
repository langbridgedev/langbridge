export const RUNTIME_ROLES = [
  { value: "admin", label: "Admin" },
  { value: "builder", label: "Builder" },
  { value: "analyst", label: "Analyst" },
  { value: "viewer", label: "Viewer" },
];

export function buildSecurityCreateForm() {
  return {
    username: "",
    email: "",
    display_name: "",
    password: "",
    roles: ["viewer"],
  };
}

export function buildSecurityPasswordForm() {
  return {
    password: "",
    must_rotate_password: false,
  };
}

export function buildCreateActorPayload(form) {
  return {
    username: cleanString(form?.username),
    email: cleanString(form?.email),
    display_name: cleanString(form?.display_name),
    password: String(form?.password || ""),
    roles: normalizeRoles(form?.roles),
  };
}

export function buildUpdateActorPayload(form) {
  return {
    roles: normalizeRoles(form?.roles),
    status: actorStatusValue(form?.status),
  };
}

export function buildResetPasswordPayload(form) {
  return {
    password: String(form?.password || ""),
    must_rotate_password: Boolean(form?.must_rotate_password),
  };
}

export function hasRuntimeAdminRole(roles) {
  const normalizedRoles = new Set(
    (Array.isArray(roles) ? roles : [])
      .map((role) => String(role || "").trim().toLowerCase())
      .filter(Boolean),
  );
  return normalizedRoles.has("admin") || normalizedRoles.has("runtime:admin");
}

export function updateRoleList(list, role) {
  const items = Array.isArray(list) ? list : [];
  const normalizedRole = String(role || "").trim();
  if (!normalizedRole) {
    return items;
  }
  if (items.includes(normalizedRole)) {
    const next = items.filter((item) => item !== normalizedRole);
    return next.length > 0 ? next : items;
  }
  return [...items, normalizedRole];
}

export function actorStatusLabel(status) {
  return String(status || "").trim().toLowerCase() === "disabled" ? "Disabled" : "Active";
}

export function roleLabel(role) {
  return RUNTIME_ROLES.find((item) => item.value === role)?.label || String(role || "");
}

export function normalizeActorList(payload) {
  return (Array.isArray(payload?.items) ? payload.items : Array.isArray(payload) ? payload : [])
    .filter(Boolean)
    .map((actor) => ({
      ...actor,
      id: String(actor.id || ""),
      username: String(actor.username || actor.subject || "").trim(),
      display_name: String(actor.display_name || actor.username || actor.subject || "").trim(),
      email: String(actor.email || "").trim(),
      status: String(actor.status || "active").trim() || "active",
      roles: Array.isArray(actor.roles) ? actor.roles.filter(Boolean).map(String) : [],
    }));
}

function normalizeRoles(roles) {
  const normalized = Array.isArray(roles)
    ? roles.map(cleanString).filter(Boolean)
    : [];
  return normalized.length > 0 ? normalized : ["viewer"];
}

function actorStatusValue(status) {
  return cleanString(status).toLowerCase() === "disabled" ? "disabled" : "active";
}

function cleanString(value) {
  return String(value || "").trim();
}
