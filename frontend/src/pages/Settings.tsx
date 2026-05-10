import { Settings2, Save } from "lucide-react";
import Page from "../components/Page";

type Section = {
  id: string;
  title: string;
  description: string;
};

const sections: Section[] = [
  {
    id: "servers",
    title: "Usenet Servers",
    description:
      "Provider hosts, credentials, TLS, connection limits and priority order",
  },
  {
    id: "categories",
    title: "Categories",
    description:
      "Per-category output directories and post-processing routing for downloaded content",
  },
  {
    id: "paths",
    title: "Paths",
    description:
      "Incomplete and complete directories, scratch space and watched folders",
  },
  {
    id: "post-processing",
    title: "Post-Processing",
    description:
      "Verification, repair, extract and cleanup behaviour after download completes",
  },
  {
    id: "bandwidth",
    title: "Bandwidth",
    description:
      "Global and per-server speed limits and scheduling windows",
  },
  {
    id: "connect",
    title: "Connect",
    description:
      "Webhooks and notification providers (Discord, Pushover, Notifiarr) for job events",
  },
  {
    id: "general",
    title: "General",
    description:
      "Listen address, data directory, API key and log level",
  },
  {
    id: "authentication",
    title: "Authentication",
    description:
      "API key for *arr clients and SAB consumers; web UI auth (forms, basic)",
  },
  {
    id: "compatibility",
    title: "SABnzbd Compatibility",
    description:
      "SAB API surface mounted at /sabnzbd/api so Sonarr / Radarr / Lidarr / Readarr / Prowlarr drop in",
  },
  {
    id: "ui",
    title: "UI",
    description: "Theme and display preferences",
  },
];

export default function Settings() {
  return (
    <Page title="Settings" subtitle="Configure servers, paths and categories">
      <div className="tool-buttons" role="toolbar" aria-label="Settings actions">
        <button type="button" className="tool-button" disabled>
          <span className="tool-button-icon" aria-hidden="true">
            <Settings2 size={22} strokeWidth={2} />
          </span>
          <span className="tool-button-label">Show Advanced</span>
        </button>
        <button type="button" className="tool-button" disabled>
          <span className="tool-button-icon is-dim" aria-hidden="true">
            <Save size={22} strokeWidth={2} />
          </span>
          <span className="tool-button-label">No Changes</span>
        </button>
      </div>

      <div className="section-list">
        {sections.map((section) => (
          <a
            key={section.id}
            href={`#${section.id}`}
            className="section-list-item"
          >
            <h2 className="section-list-title">{section.title}</h2>
            <p className="section-list-description">{section.description}</p>
          </a>
        ))}
      </div>
    </Page>
  );
}
