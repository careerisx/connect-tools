import { Activity, CircleAlert, Radio, Server, ShieldOff } from "lucide-react";

import { cn } from "../../lib/utils";
import { useCoreStore } from "../../store/core";

function formatStartedAt(seconds: bigint, nanos: number): string {
  const milliseconds = Number(seconds) * 1_000 + nanos / 1_000_000;
  const date = new Date(milliseconds);

  if (Number.isNaN(date.getTime())) {
    return "不可用";
  }

  return new Intl.DateTimeFormat("zh-CN", {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(date);
}

/** Displays only status fields reported by CoreService.GetStatus. */
export function CoreStatus() {
  const status = useCoreStore((state) => state.status);
  const connected = useCoreStore((state) => state.connected);
  const watching = useCoreStore((state) => state.watching);
  const error = useCoreStore((state) => state.error);
  const lastSequence = useCoreStore((state) => state.lastSequence);

  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm dark:border-slate-700 dark:bg-slate-900" aria-labelledby="core-status-heading">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="mb-1 text-[10px] font-bold tracking-[0.09em] text-blue-600 dark:text-blue-400">运行状态</p>
          <h2 className="text-base font-bold" id="core-status-heading">核心服务状态</h2>
        </div>
        <span className={cn("inline-flex items-center gap-2 rounded-full border px-2.5 py-1 text-xs font-semibold", connected ? "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-300" : "border-red-200 bg-red-50 text-red-700 dark:border-red-800 dark:bg-red-950 dark:text-red-300")}>
          <span className={cn("size-2 rounded-full", connected ? "bg-emerald-500" : "bg-red-500")} aria-hidden="true" />
          {connected ? "已连接" : "未连接"}
        </span>
      </div>

      {error !== undefined && (
        <div className="mt-4 flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 dark:border-red-800 dark:bg-red-950 dark:text-red-300" role="status">
          <CircleAlert size={15} aria-hidden="true" />
          <span>{error}</span>
        </div>
      )}

      <dl className="mt-4 grid gap-px overflow-hidden rounded-xl border border-slate-200 bg-slate-200 dark:border-slate-700 dark:bg-slate-700">
        <div className="bg-white p-3 dark:bg-slate-900">
          <dt className="flex items-center gap-2 text-[11px] text-slate-400"><Server size={14} aria-hidden="true" />监听地址</dt>
          <dd className="mt-1 text-xs font-semibold">{status?.listenAddress || "不可用"}</dd>
        </div>
        <div className="bg-white p-3 dark:bg-slate-900">
          <dt className="flex items-center gap-2 text-[11px] text-slate-400"><Activity size={14} aria-hidden="true" />启动时间</dt>
          <dd className="mt-1 text-xs font-semibold">
            {status?.startedAt
              ? formatStartedAt(status.startedAt.seconds, status.startedAt.nanos)
              : "不可用"}
          </dd>
        </div>
        <div className="bg-white p-3 dark:bg-slate-900">
          <dt className="flex items-center gap-2 text-[11px] text-slate-400"><Radio size={14} aria-hidden="true" />事件流</dt>
          <dd className="mt-1 text-xs font-semibold">{watching ? `正在监听序号 ${lastSequence} 之后的事件` : "正在重新连接"}</dd>
        </div>
        <div className="bg-white p-3 dark:bg-slate-900">
          <dt className="flex items-center gap-2 text-[11px] text-slate-400"><ShieldOff size={14} aria-hidden="true" />MCP</dt>
          <dd className="mt-1 text-xs font-semibold">{status?.mcpEnabled ? "已启用" : "已禁用"}</dd>
        </div>
      </dl>

      {status?.instanceId && (
        <div className="mt-4 flex min-w-0 items-center gap-2 text-[11px] text-slate-400">
          <span className="shrink-0">实例</span>
          <code className="min-w-0 overflow-hidden text-ellipsis whitespace-nowrap rounded bg-slate-100 px-2 py-1 font-mono text-slate-600 dark:bg-slate-800 dark:text-slate-300">{status.instanceId}</code>
        </div>
      )}
    </section>
  );
}
