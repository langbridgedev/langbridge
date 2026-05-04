export function SqlCodeBlock() {
  return (
    <pre className="sql-code" aria-label="Generated SQL query">
      <code>
        <span className="sql-keyword">SELECT</span> order_channel,{"\n"}
        {"       "}<span className="sql-function">SUM</span>(monthly_net_revenue) <span className="sql-keyword">AS</span> net_revenue,{"\n"}
        {"       "}<span className="sql-function">SUM</span>(monthly_gross_margin) <span className="sql-keyword">AS</span> gross_margin{"\n"}
        <span className="sql-keyword">FROM</span> growth_performance{"\n"}
        <span className="sql-keyword">WHERE</span> revenue_month &gt;= <span className="sql-type">DATE</span> <span className="sql-string">'2025-07-01'</span>{"\n"}
        {"  "}<span className="sql-keyword">AND</span> revenue_month &lt; <span className="sql-type">DATE</span> <span className="sql-string">'2025-10-01'</span>{"\n"}
        <span className="sql-keyword">GROUP BY</span> order_channel{"\n"}
        <span className="sql-keyword">ORDER BY</span> net_revenue <span className="sql-keyword">DESC</span>;
      </code>
    </pre>
  );
}
