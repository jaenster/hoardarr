import type { ReactNode } from "react";
import HealthBanner from "./HealthBanner";
import Sidebar from "./Sidebar";
import { ToastProvider } from "./Toasts";
import TopBar from "./TopBar";

type AppShellProps = {
  children: ReactNode;
};

export default function AppShell({ children }: AppShellProps) {
  return (
    <ToastProvider>
      <div className="app-shell">
        <TopBar />
        <div className="app-body">
          <Sidebar />
          <main className="content">
            <HealthBanner />
            {children}
          </main>
        </div>
      </div>
    </ToastProvider>
  );
}
