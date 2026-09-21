import { Activity, CircleAlert, Radio, Server, ShieldOff } from "lucide-react";

import { useCoreStore } from "../../store/core";

function formatStartedAt(seconds: bigint, nanos: number): string {
  const milliseconds = Number(seconds) * 1_000 + nanos / 1_000_000;
  const date = new Date(milliseconds);

  if (Number.isNaN(date.getTime())) {
    return "Unavailable";
  }

  return new Intl.DateTimeFormat(undefined, {
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
    <section className="core-status" aria-labelledby="core-status-heading">
      <div className="section-heading-row">
        <div>
          <p className="eyebrow">Runtime</p>
          <h2 id="core-status-heading">Core status</h2>
        </div>
        <span className={`state-pill ${connected ? "is-online" : "is-offline"}`}>
          <span className="status-dot" aria-hidden="true" />
          {connected ? "Connected" : "Disconnected"}
        </span>
      </div>

      {error !== undefined && (
        <div className="status-alert" role="status">
          <CircleAlert size={15} aria-hidden="true" />
          <span>{error}</span>
        </div>
      )}

      <dl className="status-details">
        <div>
          <dt><Server size={14} aria-hidden="true" />Address</dt>
          <dd>{status?.listenAddress || "Unavailable"}</dd>
        </div>
        <div>
          <dt><Activity size={14} aria-hidden="true" />Started</dt>
          <dd>
            {status?.startedAt
              ? formatStartedAt(status.startedAt.seconds, status.startedAt.nanos)
              : "Unavailable"}
          </dd>
        </div>
        <div>
          <dt><Radio size={14} aria-hidden="true" />Event stream</dt>
          <dd>{watching ? `Watching after ${lastSequence}` : "Reconnecting"}</dd>
        </div>
        <div>
          <dt><ShieldOff size={14} aria-hidden="true" />MCP</dt>
          <dd>{status?.mcpEnabled ? "Enabled" : "Disabled"}</dd>
        </div>
      </dl>

      {status?.instanceId && (
        <div className="instance-id">
          <span>Instance</span>
          <code>{status.instanceId}</code>
        </div>
      )}
    </section>
  );
}
