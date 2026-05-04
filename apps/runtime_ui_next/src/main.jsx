import React from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";

import { App } from "./App.jsx";
import "./styles/index.css";

if (window.location.pathname === "/ui-next" || window.location.pathname.startsWith("/ui-next/")) {
  const nextPath = window.location.pathname.slice(8) || "/";
  window.history.replaceState(window.history.state, "", `${nextPath}${window.location.search}${window.location.hash}`);
}

createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>,
);
