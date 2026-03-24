import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";

import App from "./App";
import "./styles.css";

if (window.location.pathname === "/ui" || window.location.pathname.startsWith("/ui/")) {
  const nextPath = window.location.pathname.slice(3) || "/";
  window.history.replaceState(window.history.state, "", `${nextPath}${window.location.search}${window.location.hash}`);
}

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>,
);
