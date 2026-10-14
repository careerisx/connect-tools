import type { ComponentProps } from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "../../lib/utils";

export const buttonVariants = cva(
  "inline-flex shrink-0 cursor-pointer items-center justify-center gap-1.5 whitespace-nowrap rounded-lg border border-transparent font-semibold leading-none transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-500 disabled:cursor-not-allowed disabled:opacity-50",
  {
    variants: {
      variant: {
        default: "bg-blue-600 text-white shadow-sm hover:bg-blue-700 dark:bg-blue-500 dark:hover:bg-blue-400",
        outline: "border-slate-300 bg-white text-slate-700 shadow-sm hover:bg-slate-50 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-700",
        ghost: "bg-transparent text-slate-600 hover:bg-slate-100 hover:text-slate-900 dark:text-slate-300 dark:hover:bg-slate-800 dark:hover:text-white",
        unstyled: "rounded-none border-0 bg-transparent p-0 font-[inherit] text-inherit shadow-none",
      },
      size: {
        default: "min-h-9.5 px-4 text-[13px]",
        sm: "min-h-8 px-2.5 text-xs",
        icon: "size-9 p-0",
        unstyled: "min-h-0 p-0",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  },
);

/** Shared shadcn-style button primitive for application actions. */
export function Button({
  className,
  variant,
  size,
  type = "button",
  ...props
}: ComponentProps<"button"> & VariantProps<typeof buttonVariants>) {
  return (
    <button
      type={type}
      data-slot="button"
      className={cn(buttonVariants({ variant, size }), className)}
      {...props}
    />
  );
}
