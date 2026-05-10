import type { ButtonHTMLAttributes, ReactNode } from "react";

type Variant = "primary" | "secondary" | "ghost" | "danger";

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: Variant;
  icon?: ReactNode;
};

export default function Button({
  variant = "secondary",
  icon,
  className,
  children,
  type,
  ...rest
}: ButtonProps) {
  const classes =
    "btn btn-" + variant + (className ? " " + className : "");
  return (
    <button type={type ?? "button"} className={classes} {...rest}>
      {icon && <span className="btn-icon">{icon}</span>}
      {children && <span className="btn-label">{children}</span>}
    </button>
  );
}
