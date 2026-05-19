import React from "react";
import ReactDOM from "react-dom/client";
import AuthGate from "./components/AuthGate";
import "./styles.css";

// The router and the rest of the app live behind AuthGate, which
// only mounts <App/> after a successful whoami. That keeps the
// pre-login bundle to just react, react-dom, the two auth forms,
// and the api client — no react-router, no pages, no recharts.
ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <AuthGate />
  </React.StrictMode>,
);
