import type { ComponentProps } from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "../../lib/utils";

export const badgeVariants = cva(
  "inline-flex min-h-5 shrink-0 items-center justify-center gap-1 whitespace-nowrap rounded-full border px-2 text-[10px] font-bold leading-none",
  {
    variants: {
      variant: {
        default: "border-blue-200 bg-blue-50 text-blue-700 dark:border-blue-800 dark:bg-blue-950 dark:text-blue-300",
        outline: "border-slate-300 bg-transparent text-slate-600 dark:border-slate-600 dark:text-slate-300",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  },
);

/** Shared shadcn-style badge primitive for compact state labels. */
export function Badge({
  className,
  variant,
  ...props
}: ComponentProps<"span"> & VariantProps<typeof badgeVariants>) {
  return (
    <span
      data-slot="badge"
      className={cn(badgeVariants({ variant }), className)}
      {...props}
    />
  );
}
