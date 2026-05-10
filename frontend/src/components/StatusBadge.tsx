import type { ReactNode } from "react";

export type BadgeTone = "ok" | "warn" | "err" | "info" | "neutral";

type StatusBadgeProps = {
  tone?: BadgeTone;
  dot?: boolean;
  children: ReactNode;
};

export default function StatusBadge({
  tone = "neutral",
  dot = false,
  children,
}: StatusBadgeProps) {
  return (
    <span className={"badge badge-" + tone}>
      {dot && <span className="badge-dot" />}
      {children}
    </span>
  );
}
