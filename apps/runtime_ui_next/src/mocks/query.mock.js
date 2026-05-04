export const queryProjects = [
  { id: "revenue-sql-pack", name: "Revenue SQL pack", meta: "7 queries" },
  { id: "marketing-efficiency", name: "Marketing efficiency", meta: "4 queries" },
  { id: "support-load-model", name: "Support load model", meta: "3 queries" },
];

export const queryRecents = [
  { id: "q3-channel-revenue-sql", title: "Q3 channel revenue SQL" },
  { id: "support-load-correlation", title: "Support load correlation" },
  { id: "monthly-order-trend", title: "Monthly order trend" },
  { id: "marketing-efficiency-region", title: "Marketing efficiency by region" },
  { id: "customer-revenue-scan", title: "Customer revenue scan" },
];

export const queryScopes = [
  { value: "semantic", label: "Semantic" },
  { value: "dataset", label: "Dataset" },
  { value: "source", label: "Source" },
];

export const sourceConnectors = [
  { value: "snowflake", label: "Snowflake warehouse" },
  { value: "postgres", label: "Postgres replica" },
  { value: "bigquery", label: "BigQuery analytics" },
];

export const exampleQueryResult = {
  title: "Channel ranking",
  columns: ["Channel", "Net revenue", "Gross margin"],
  rows: [
    ["Paid Social", "$9.1k", "$4.9k"],
    ["Organic Search", "$8.2k", "$4.7k"],
    ["Affiliate", "$8.1k", "$4.5k"],
  ],
  metadata: {
    rows: 3,
    columns: 3,
    runtime: "142ms",
  },
};
