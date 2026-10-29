import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";

import { CoreService } from "../gen/connecttools/v1/core_pb";
import { MQTTService } from "../gen/connecttools/v1/mqtt_pb";
import { ProfileService } from "../gen/connecttools/v1/profile_pb";
import { RecordService } from "../gen/connecttools/v1/records_pb";
import { SessionService } from "../gen/connecttools/v1/session_pb";
import { TCPService } from "../gen/connecttools/v1/tcp_pb";

/** Keeps browser requests on the current application's credential boundary. */
const sameOriginFetch: typeof globalThis.fetch = (input, init) =>
  globalThis.fetch(input, {
    ...init,
    credentials: "same-origin",
  });

/** Event bridge address supplied only by the native desktop asset server. */
let desktopEventURL: string | undefined;

/** Loads the native event bridge before connecting, while preserving browser access. */
export async function prepareDesktopTransport(signal?: AbortSignal): Promise<void> {
  const isDesktop = window.location.protocol === "wails:" ||
    window.location.hostname === "wails.localhost" || "runtime" in window;
  if (!isDesktop || desktopEventURL !== undefined) return;

  const response = await sameOriginFetch(new URL("/desktop/transport", window.location.href), { signal });
  if (!response.ok) throw new Error("无法获取桌面连接信息");
  const configuration: unknown = await response.json();
  if (configuration === null || typeof configuration !== "object" ||
    !("eventURL" in configuration) || typeof configuration.eventURL !== "string") {
    throw new Error("桌面连接信息无效");
  }
  const eventURL = new URL(configuration.eventURL);
  if (eventURL.protocol !== "ws:" || eventURL.hostname !== "127.0.0.1" ||
    eventURL.pathname !== "/ws/events" || !eventURL.searchParams.get("token")) {
    throw new Error("桌面事件地址无效");
  }
  desktopEventURL = eventURL.toString();
}

/** Uses the native event bridge or the existing same-origin browser WebSocket. */
export function coreEventWebSocketURL(afterSequence: bigint): string {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const url = desktopEventURL === undefined
    ? new URL("/ws/events", `${protocol}//${window.location.host}`)
    : new URL(desktopEventURL);
  url.searchParams.set("after", afterSequence.toString());
  return url.toString();
}

/** Resolves custom Wails schemes without relying on their opaque location.origin. */
const transport = createConnectTransport({
    baseUrl: new URL("/", window.location.href).toString(),
    fetch: sameOriginFetch,
});

export const coreClient = createClient(CoreService, transport);
export const profileClient = createClient(ProfileService, transport);
export const sessionClient = createClient(SessionService, transport);
export const recordClient = createClient(RecordService, transport);
export const tcpClient = createClient(TCPService, transport);
export const mqttClient = createClient(MQTTService, transport);
