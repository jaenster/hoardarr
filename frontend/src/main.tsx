import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App";
import "./styles.css";

// Vite bakes a sentinel base path ("/__HOARDARR_BASE__/") into
// import.meta.env.BASE_URL at build time. The Go server replaces the
// sentinel with the configured runtime base before serving any JS,
// HTML, or CSS file. By the time this code runs in the browser,
// BASE_URL already holds the real prefix.
//
// BrowserRouter expects a basename WITHOUT a trailing slash, so we
// trim it. Empty string disables the basename (root deployment).
const baseURL = (import.meta.env.BASE_URL ?? "/").replace(/\/+$/, "");

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter basename={baseURL || undefined}>
      <App />
    </BrowserRouter>
  </React.StrictMode>,
);
