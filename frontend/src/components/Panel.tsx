import type { ReactNode } from "react";

type PanelProps = {
  title?: string;
  meta?: ReactNode;
  actions?: ReactNode;
  /** Render children without the default inner padding (for tables, code blocks). */
  flush?: boolean;
  className?: string;
  children: ReactNode;
};

export default function Panel({
  title,
  meta,
  actions,
  flush,
  className,
  children,
}: PanelProps) {
  const hasHeader = title || meta || actions;
  return (
    <section className={"panel" + (className ? " " + className : "")}>
      {hasHeader && (
        <div className="panel-header">
          <div className="panel-header-left">
            {title && <h2>{title}</h2>}
            {meta && <span className="panel-meta">{meta}</span>}
          </div>
          {actions && <div className="panel-actions">{actions}</div>}
        </div>
      )}
      <div className={"panel-body" + (flush ? " is-flush" : "")}>{children}</div>
    </section>
  );
}
