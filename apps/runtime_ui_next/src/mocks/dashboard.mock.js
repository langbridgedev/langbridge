export const dashboardProjects = [
  { id: "leadership-boards", name: "Leadership boards", meta: "3 dashboards" },
  { id: "weekly-trading", name: "Weekly trading", meta: "2 dashboards" },
  { id: "ops-monitoring", name: "Ops monitoring", meta: "4 dashboards" },
];

export const dashboardRecents = [
  { id: "growth-leadership-view", title: "Growth leadership view" },
  { id: "commerce-weekly-readout", title: "Commerce weekly readout" },
  { id: "support-health-monitor", title: "Support health monitor" },
  { id: "marketing-efficiency-board", title: "Marketing efficiency board" },
  { id: "revenue-operations-cockpit", title: "Revenue operations cockpit" },
];

export const dashboardBoard = {
  id: "growth-leadership-view",
  title: "Growth leadership view",
  filters: ["Date: Q3 2025", "Region: All", "Channel: All"],
  tiles: [
    {
      id: "channel-revenue-margin",
      title: "Q3 channel revenue and gross margin",
      type: "Grouped bar",
      size: "wide",
      source: "growth_performance",
      rows: 3,
      lastRun: "just now",
    },
    {
      id: "net-revenue-kpi",
      title: "$25.5k net revenue",
      type: "KPI",
      description: "Across the top three returned channels in Q3 2025.",
    },
    {
      id: "analyst-note",
      title: "Paid Social leads both revenue and margin.",
      type: "Analyst note",
      description: "Organic Search and Affiliate are close enough to monitor in the next trading period.",
    },
    {
      id: "ranked-rows",
      title: "Ranked rows",
      type: "Table",
      description: "Channel, net revenue, gross margin, generated from governed runtime data.",
    },
  ],
};
