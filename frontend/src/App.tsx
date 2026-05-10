import { Navigate, Route, Routes } from "react-router-dom";
import Sidebar from "./components/Sidebar";
import Activity from "./pages/Activity";
import History from "./pages/History";
import Settings from "./pages/Settings";
import System from "./pages/System";

export default function App() {
  return (
    <div className="app">
      <Sidebar />
      <main className="content">
        <Routes>
          <Route path="/" element={<Navigate to="/activity" replace />} />
          <Route path="/activity" element={<Activity />} />
          <Route path="/history" element={<History />} />
          <Route path="/settings/*" element={<Settings />} />
          <Route path="/system" element={<System />} />
          <Route path="*" element={<Navigate to="/activity" replace />} />
        </Routes>
      </main>
    </div>
  );
}
