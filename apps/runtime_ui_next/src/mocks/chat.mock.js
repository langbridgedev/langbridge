export const chatProjects = [
  { id: "growth-analysis", name: "Growth analysis", meta: "12 chats" },
  { id: "commerce-reporting", name: "Commerce reporting", meta: "8 chats" },
  { id: "support-operations", name: "Support operations", meta: "5 chats" },
];

export const chatThreads = [
  { id: "q3-channel-performance", title: "Q3 channel performance" },
  { id: "support-load-vs-efficiency", title: "Support load vs efficiency" },
  { id: "orders-slowing-2026", title: "Orders slowing in 2026" },
  { id: "gross-margin-region", title: "Gross margin by region" },
  { id: "marketing-spend-anomaly", title: "Marketing spend anomaly" },
];

export const suggestedPrompts = [
  {
    label: "Rank performance",
    prompt: "Which order channels drove the highest net revenue and gross margin in Q3 2025?",
  },
  {
    label: "Investigate relationship",
    prompt: "Do regions with higher support load also underperform on marketing efficiency?",
  },
  {
    label: "Create a chart",
    prompt: "Show monthly orders and net revenue for 2024 as a bar chart.",
  },
];

export const exampleChatResponse = {
  id: "run_support_efficiency",
  conversationId: "support-load-vs-efficiency",
  createdAt: "2026-04-27T10:24:00Z",
  role: "assistant",
  status: "completed",
  mode: "research",
  agent: {
    id: "analyst.growth_analyst",
    label: "Growth analyst",
  },
  primaryReply: `## Higher support load does not clearly predict weaker marketing efficiency.

The relationship is mixed across regions. DACH has the highest support load but also the strongest revenue per marketing dollar, so it does not support the hypothesis. ANZ is the clearest risk area: it combines elevated ticket load with the weakest marketing efficiency in the returned result.

{artifact:regional_summary_cards}

{artifact:support_efficiency_scatter}

{artifact:regional_metrics_table}

> Note: marketing spend is modeled as global spend attributed by regional revenue share. If spend is available by region, I would rerun this with direct regional spend before treating ANZ as a confirmed operational issue.`,
  artifacts: [
    {
      id: "regional_summary_cards",
      type: "metric_cards",
      title: "Regional readout",
      items: [
        { label: "Highest support load", value: "DACH", detail: "1.17 avg tickets per customer" },
        { label: "Weakest efficiency", value: "ANZ", detail: "$0.082 revenue per marketing dollar" },
        { label: "Overall pattern", value: "Mixed", detail: "No strong negative relationship" },
      ],
    },
    {
      id: "support_efficiency_scatter",
      type: "scatter_plot",
      title: "Support load vs marketing efficiency",
      eyebrow: "Visual",
      xLabel: "Higher support load",
      yLabel: "Higher marketing efficiency",
      points: [
        { label: "DACH", left: 72, bottom: 78 },
        { label: "ANZ", left: 63, bottom: 28 },
        { label: "NA", left: 42, bottom: 58 },
        { label: "UK", left: 24, bottom: 52 },
      ],
    },
    {
      id: "regional_metrics_table",
      type: "table",
      title: "Underlying regional metrics",
      columns: ["Region", "Support load", "Efficiency"],
      rows: [
        ["DACH", "1.17", "$0.085"],
        ["ANZ", "1.11", "$0.082"],
        ["North America", "0.94", "$0.084"],
        ["United Kingdom", "0.72", "$0.084"],
      ],
    },
  ],
  metadata: {
    displayChips: ["Used governed data", "3 SQL checks", "Chart created"],
    queryScope: "dataset",
    rowCount: 4,
    confidence: "medium",
  },
};
