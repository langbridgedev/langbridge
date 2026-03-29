import { MoonStar, SunMedium } from "lucide-react";

import { useTheme } from "./ThemeProvider";

export function ThemeToggle({ className = "", onToggle }) {
  const { theme, toggleTheme } = useTheme();

  return (
    <button
      type="button"
      onClick={() => {
        toggleTheme();
        onToggle?.();
      }}
      className={`ghost-button theme-toggle-button ${className}`.trim()}
      aria-label={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}
      title={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}
    >
      {theme === "dark" ? (
        <>
          <SunMedium className="button-icon" aria-hidden="true" />
          Light mode
        </>
      ) : (
        <>
          <MoonStar className="button-icon" aria-hidden="true" />
          Dark mode
        </>
      )}
    </button>
  );
}
