import type { ComponentProps } from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "../../lib/utils";

export const badgeVariants = cva(
  "inline-flex w-fit shrink-0 items-center justify-center gap-1 whitespace-nowrap rounded-md border text-xs font-medium",
  {
    variants: {
      variant: {
        default: "border-transparent",
        outline: "border-current bg-transparent",
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
