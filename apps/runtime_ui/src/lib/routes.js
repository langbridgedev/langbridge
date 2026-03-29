import {
  Blocks,
  Bot,
  Cable,
  Database,
  LayoutDashboard,
  MessageSquareText,
  Settings2,
  Sparkles,
  Table2,
} from "lucide-react";

export const NAV_SECTIONS = [
  {
    label: "Command",
    items: [
      {
        to: "/",
        label: "Overview",
        icon: LayoutDashboard,
        description: "Runtime command center, recent activity, and quick actions.",
      },
    ],
  },
  {
    label: "Model",
    items: [
      {
        to: "/connectors",
        label: "Connectors",
        icon: Cable,
        description: "Connector inventory, sync resources, and runtime state.",
      },
      {
        to: "/datasets",
        label: "Datasets",
        icon: Database,
        description: "Dataset bindings, schema detail, and runtime previews.",
      },
      {
        to: "/semantic-models",
        label: "Semantic Models",
        icon: Blocks,
        description: "Semantic field libraries, YAML detail, and model structure.",
      },
    ],
  },
  {
    label: "Operate",
    items: [
      {
        to: "/sql",
        label: "SQL Workspace",
        icon: Table2,
        description: "Federated and direct SQL with local history and saved workbench state.",
      },
      {
        to: "/bi",
        label: "Dashboard Studio",
        icon: Sparkles,
        description: "Runtime BI studio with local dashboards and semantic query widgets.",
      },
      {
        to: "/agents",
        label: "Agents",
        icon: Bot,
        description: "Agent definitions, access policy, and runtime quick runs.",
      },
      {
        to: "/chat",
        label: "Threads",
        icon: MessageSquareText,
        description: "Threaded runtime chat with local agent selection and history.",
      },
    ],
  },
  {
    label: "System",
    items: [
      {
        to: "/settings",
        label: "Settings",
        icon: Settings2,
        description: "Runtime auth posture, host info, and capabilities.",
      },
    ],
  },
];

export function matchNav(pathname, target) {
  if (target === "/") {
    return pathname === "/";
  }
  return pathname === target || pathname.startsWith(`${target}/`);
}

export function resolveActiveNav(pathname) {
  const items = NAV_SECTIONS.flatMap((section) => section.items);
  return items.find((item) => matchNav(pathname, item.to)) || items[0];
}
