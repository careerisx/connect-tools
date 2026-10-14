import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";

import { CoreService } from "../gen/connecttools/v1/core_pb";
import { MQTTService } from "../gen/connecttools/v1/mqtt_pb";
import { ProfileService } from "../gen/connecttools/v1/profile_pb";
import { RecordService } from "../gen/connecttools/v1/records_pb";
import { SessionService } from "../gen/connecttools/v1/session_pb";
import { TCPService } from "../gen/connecttools/v1/tcp_pb";

const sameOriginFetch: typeof globalThis.fetch = (input, init) =>
  globalThis.fetch(input, {
    ...init,
    credentials: "same-origin",
  });

/** Client for the local Connect services exposed by the current origin. */
const transport = createConnectTransport({
    baseUrl: window.location.origin,
    fetch: sameOriginFetch,
});

export const coreClient = createClient(CoreService, transport);
export const profileClient = createClient(ProfileService, transport);
export const sessionClient = createClient(SessionService, transport);
export const recordClient = createClient(RecordService, transport);
export const tcpClient = createClient(TCPService, transport);
export const mqttClient = createClient(MQTTService, transport);
