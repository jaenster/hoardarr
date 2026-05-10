import type { ReactNode } from "react";
import AuthGate from "./AuthGate";
import Sidebar from "./Sidebar";
import TopBar from "./TopBar";

type AppShellProps = {
  children: ReactNode;
};

export default function AppShell({ children }: AppShellProps) {
  return (
    <AuthGate>
      <div className="app-shell">
        <TopBar />
        <div className="app-body">
          <Sidebar />
          <main className="content">{children}</main>
        </div>
      </div>
    </AuthGate>
  );
}
