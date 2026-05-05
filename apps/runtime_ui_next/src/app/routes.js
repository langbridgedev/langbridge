export const routes = [
  { id: "chat", path: "/chat", label: "Chats", workspace: "chat" },
  { id: "chatThread", path: "/chat/:threadId", label: "Chats", workspace: "chat" },
  { id: "query", path: "/query-workspace", label: "Query", workspace: "query" },
  { id: "dashboard", path: "/dashboards", label: "Dashboards", workspace: "dashboards" },
  { id: "dashboardBoard", path: "/dashboards/:dashboardId", label: "Dashboards", workspace: "dashboards" },
  { id: "configure", path: "/configure/:section", label: "Configure", workspace: "configure" },
];

export const defaultRoute = routes[0];

const routeMatchers = routes.map((route) => {
  const keys = [];
  const pattern = route.path
    .replace(/:[^/]+/g, (segment) => {
      keys.push(segment.slice(1));
      return "([^/]+)";
    })
    .replace(/\//g, "\\/");
  return {
    route,
    keys,
    regex: new RegExp(`^${pattern}\\/?$`),
  };
});

export function matchRoute(path) {
  const normalized = path === "/" ? "/chat" : path;
  for (const matcher of routeMatchers) {
    const match = normalized.match(matcher.regex);
    if (!match) {
      continue;
    }
    const params = Object.fromEntries(
      matcher.keys.map((key, index) => [key, decodeURIComponent(match[index + 1] || "")]),
    );
    return { ...matcher.route, params };
  }
  return null;
}
