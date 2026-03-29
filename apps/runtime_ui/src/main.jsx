import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";

import App from "./App";
import { ThemeProvider, initializeTheme } from "./components/ThemeProvider";
import "./styles.css";

if (window.location.pathname === "/ui" || window.location.pathname.startsWith("/ui/")) {
  const nextPath = window.location.pathname.slice(3) || "/";
  window.history.replaceState(window.history.state, "", `${nextPath}${window.location.search}${window.location.hash}`);
}

initializeTheme();

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <ThemeProvider>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </ThemeProvider>
  </React.StrictMode>,
);
