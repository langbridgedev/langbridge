import {
  Activity,
  Blocks,
  Bot,
  Cable,
  Database,
  LayoutGrid,
  LayoutDashboard,
  MessageSquareText,
  Settings2,
  Table2,
  Workflow,
} from "lucide-react";

export const NAV_SECTIONS = [
  {
    label: "Core",
    items: [
      {
        to: "/",
        label: "Command Center",
        icon: LayoutDashboard,
        description: "Ask-first runtime home with recent activity, entry points, and operating signals.",
      },
      {
        to: "/chat",
        label: "Ask",
        icon: MessageSquareText,
        description: "Question-first analysis with integrated threads and agent execution context.",
      },
      {
        to: "/runs",
        label: "Runs",
        icon: Activity,
        description: "Recent runtime executions across threads, queries, and dashboard widgets.",
      },
    ],
  },
  {
    label: "Build",
    items: [
      {
        to: "/semantic-models",
        label: "Semantic Models",
        icon: Blocks,
        description: "Define the runtime semantic layer that powers governed analysis.",
      },
      {
        to: "/datasets",
        label: "Datasets",
        icon: Database,
        description: "Runtime datasets, bindings, previews, and execution-ready context.",
      },
      {
        to: "/connectors",
        label: "Connectors",
        icon: Cable,
        description: "Connector inventory, sync resources, and runtime state.",
      },
    ],
  },
  {
    label: "Advanced",
    items: [
      {
        to: "/query-workspace",
        label: "Query Workspace",
        icon: Workflow,
        description: "Semantic-first query workspace with dataset and source SQL for power users.",
        aliases: ["/sql"],
      },
      {
        to: "/dashboards",
        label: "Dashboard Builder",
        icon: LayoutGrid,
        description: "Runtime-local dashboards backed by semantic queries without leading the main product story.",
        mobile: false,
      },
      {
        to: "/agents",
        label: "Agent Library",
        icon: Bot,
        description: "Inspect runtime agent profiles, tools, and execution posture.",
        mobile: false,
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

function listMatchTargets(item) {
  return [item.to, ...(Array.isArray(item.aliases) ? item.aliases : [])];
}

export function matchNav(pathname, item) {
  const targets = typeof item === "string" ? [item] : listMatchTargets(item);
  if (targets.includes("/")) {
    return pathname === "/";
  }
  return targets.some((target) => pathname === target || pathname.startsWith(`${target}/`));
}

export function resolveActiveNav(pathname) {
  const items = NAV_SECTIONS.flatMap((section) => section.items);
  return items.find((item) => matchNav(pathname, item)) || items[0];
}
