import { listChatProjects, listChatThreads } from "./chatService.js";
import { listDashboardProjects, listDashboardRecents } from "./dashboardService.js";
import { listQueryProjects, listQueryRecents } from "./queryService.js";

export function getEmptyNavigationState() {
  return { recents: null, projects: null };
}

export async function getNavigationState(workspace) {
  if (workspace === "query") {
    const [recents, projects] = await Promise.all([listQueryRecents(), listQueryProjects()]);
    return {
      recents: { label: "Queries", items: recents.map((item) => ({ ...item, path: "/query-workspace" })) },
      projects: { label: "Query Projects", items: projects.map((item) => ({ ...item, path: "/query-workspace" })) },
    };
  }

  if (workspace === "dashboards") {
    const [recents, projects] = await Promise.all([listDashboardRecents(), listDashboardProjects()]);
    return {
      recents: {
        label: "Dashboards",
        items: recents.map((item) => ({ ...item, path: item.path || `/dashboards/${item.id}` })),
      },
      projects: {
        label: "Dashboard Projects",
        items: projects.map((item) => ({ ...item, path: item.path || "/dashboards" })),
      },
    };
  }

  if (workspace === "configure") {
    return { recents: null, projects: null };
  }

  const [threads, projects] = await Promise.all([
    safeNavigationItems(listChatThreads),
    safeNavigationItems(listChatProjects),
  ]);

  return {
    recents: {
      label: "Chats",
      items: threads,
    },
    projects: projects.length > 0 ? {
      label: "Chat Projects",
      items: projects.map((item) => ({ ...item, path: item.path || "/chat" })),
    } : null,
  };
}

async function safeNavigationItems(loader) {
  try {
    const items = await loader();
    return Array.isArray(items) ? items : [];
  } catch {
    return [];
  }
}
