const STORAGE_KEYS = {
  theme: "langbridge-poc:theme",
  sidebarCollapsed: "langbridge-poc:sidebar-collapsed",
};

function canUseStorage() {
  return typeof window !== "undefined" && Boolean(window.localStorage);
}

export function readThemePreference() {
  if (!canUseStorage()) {
    return "light";
  }
  const value = window.localStorage.getItem(STORAGE_KEYS.theme);
  return value === "dark" ? "dark" : "light";
}

export function writeThemePreference(theme) {
  if (!canUseStorage()) {
    return;
  }
  window.localStorage.setItem(STORAGE_KEYS.theme, theme === "dark" ? "dark" : "light");
}

export function readSidebarPreference() {
  if (!canUseStorage()) {
    return false;
  }
  return window.localStorage.getItem(STORAGE_KEYS.sidebarCollapsed) === "true";
}

export function writeSidebarPreference(collapsed) {
  if (!canUseStorage()) {
    return;
  }
  window.localStorage.setItem(STORAGE_KEYS.sidebarCollapsed, collapsed ? "true" : "false");
}
