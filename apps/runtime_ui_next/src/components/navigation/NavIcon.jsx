export function NavIcon({ name }) {
  const common = {
    viewBox: "0 0 24 24",
    fill: "none",
    stroke: "currentColor",
    strokeWidth: "1.8",
    strokeLinecap: "round",
    strokeLinejoin: "round",
    "aria-hidden": "true",
  };

  switch (name) {
    case "chat":
      return (
        <svg {...common}>
          <path d="M5.5 6.5h13a2 2 0 0 1 2 2v6.7a2 2 0 0 1-2 2H10l-4.5 3v-3a2 2 0 0 1-2-2V8.5a2 2 0 0 1 2-2Z" />
          <path d="M8 10.5h8" />
          <path d="M8 13.5h5.5" />
        </svg>
      );
    case "terminal":
      return (
        <svg {...common}>
          <rect x="3.5" y="5" width="17" height="14" rx="2.2" />
          <path d="m7.5 10 2.5 2-2.5 2" />
          <path d="M12.5 14h4" />
        </svg>
      );
    case "dashboard":
      return (
        <svg {...common}>
          <rect x="4" y="4" width="7" height="7" rx="1.6" />
          <rect x="13" y="4" width="7" height="5" rx="1.6" />
          <rect x="13" y="11" width="7" height="9" rx="1.6" />
          <rect x="4" y="13" width="7" height="7" rx="1.6" />
        </svg>
      );
    case "settings":
      return (
        <svg {...common}>
          <path d="M12 8.2a3.8 3.8 0 1 0 0 7.6 3.8 3.8 0 0 0 0-7.6Z" />
          <path d="M18.5 12a6.6 6.6 0 0 0-.1-1l2-1.5-2-3.4-2.4 1a7 7 0 0 0-1.7-1L14 3.5h-4l-.3 2.6a7 7 0 0 0-1.7 1l-2.4-1-2 3.4 2 1.5a6.6 6.6 0 0 0 0 2l-2 1.5 2 3.4 2.4-1a7 7 0 0 0 1.7 1l.3 2.6h4l.3-2.6a7 7 0 0 0 1.7-1l2.4 1 2-3.4-2-1.5c.1-.3.1-.7.1-1Z" />
        </svg>
      );
    case "sun":
      return (
        <svg {...common}>
          <circle cx="12" cy="12" r="4" />
          <path d="M12 2.8v2" />
          <path d="M12 19.2v2" />
          <path d="m4.5 4.5 1.4 1.4" />
          <path d="m18.1 18.1 1.4 1.4" />
          <path d="M2.8 12h2" />
          <path d="M19.2 12h2" />
          <path d="m4.5 19.5 1.4-1.4" />
          <path d="m18.1 5.9 1.4-1.4" />
        </svg>
      );
    case "moon":
      return (
        <svg {...common}>
          <path d="M20 14.6A7.7 7.7 0 0 1 9.4 4a8 8 0 1 0 10.6 10.6Z" />
        </svg>
      );
    case "plus":
      return (
        <svg {...common}>
          <path d="M12 5v14" />
          <path d="M5 12h14" />
        </svg>
      );
    case "menu":
      return (
        <svg {...common}>
          <path d="M5 7h14" />
          <path d="M5 12h14" />
          <path d="M5 17h14" />
        </svg>
      );
    case "logout":
      return (
        <svg {...common}>
          <path d="M10 5H6.5A2.5 2.5 0 0 0 4 7.5v9A2.5 2.5 0 0 0 6.5 19H10" />
          <path d="M14 8l4 4-4 4" />
          <path d="M18 12H9" />
        </svg>
      );
    default:
      return null;
  }
}
