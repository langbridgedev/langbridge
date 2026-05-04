import { chatProjects } from "../mocks/chat.mock.js";
import { dashboardProjects, dashboardRecents } from "../mocks/dashboard.mock.js";
import { queryProjects, queryRecents } from "../mocks/query.mock.js";
import { listChatThreads } from "./chatService.js";

export function getEmptyNavigationState() {
  return { recents: null, projects: null };
}

export async function getNavigationState(workspace) {
  if (workspace === "query") {
    return {
      recents: { label: "Queries", items: queryRecents.map((item) => ({ ...item, path: "/query-workspace" })) },
      projects: { label: "Query Projects", items: queryProjects.map((item) => ({ ...item, path: "/query-workspace" })) },
    };
  }

  if (workspace === "dashboards") {
    return {
      recents: {
        label: "Dashboards",
        items: dashboardRecents.map((item) => ({ ...item, path: `/dashboards/${item.id}` })),
      },
      projects: {
        label: "Dashboard Projects",
        items: dashboardProjects.map((item) => ({ ...item, path: "/dashboards" })),
      },
    };
  }

  if (workspace === "configure") {
    return { recents: null, projects: null };
  }

  return {
    recents: {
      label: "Chats",
      items: await listChatThreads(),
    },
    projects: {
      label: "Chat Projects",
      items: chatProjects.map((item) => ({ ...item, path: "/chat" })),
    },
  };
}
