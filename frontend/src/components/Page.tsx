import type { ReactNode } from "react";

type PageProps = {
  title: string;
  subtitle?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
};

export default function Page({ title, subtitle, actions, children }: PageProps) {
  return (
    <div className="page">
      <header className="page-header">
        <div className="page-title">
          <h1>{title}</h1>
          {subtitle && <div className="page-subtitle">{subtitle}</div>}
        </div>
        {actions && <div className="page-actions">{actions}</div>}
      </header>
      <div className="page-body">{children}</div>
    </div>
  );
}
