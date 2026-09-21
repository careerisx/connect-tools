import { create } from "zustand";

import { mqttClient, profileClient, recordClient, sessionClient, tcpClient } from "../api/client";
import type { Profile } from "../gen/connecttools/v1/profile_pb";
import type { DebugRecord } from "../gen/connecttools/v1/records_pb";
import type { Session } from "../gen/connecttools/v1/session_pb";
import { MQTTQoS } from "../gen/connecttools/v1/mqtt_pb";
import { PayloadEncoding, SessionKind } from "../gen/connecttools/v1/session_pb";

type WorkbenchState = {
  profiles: Profile[];
  sessions: Session[];
  records: DebugRecord[];
  selectedSessionId?: string;
  selectedRecord?: DebugRecord;
  loading: boolean;
  error?: string;
  refresh(): Promise<void>;
  selectSession(id: string): Promise<void>;
  start(profileId: string): Promise<void>;
  stop(sessionId: string): Promise<void>;
  send(session: Session, text: string, topic: string, qos: number, retain: boolean): Promise<void>;
  clear(): Promise<void>;
  subscribe(sessionId: string, filter: string, qos: number): Promise<void>;
  unsubscribe(sessionId: string, filter: string): Promise<void>;
  selectRecord(record: DebugRecord): void;
};

const errorText = (error: unknown) => error instanceof Error ? error.message : "Operation failed";

export const useWorkbenchStore = create<WorkbenchState>((set, get) => ({
  profiles: [], sessions: [], records: [], loading: false,
  refresh: async () => {
    set({ loading: true });
    try {
      const [profiles, sessions] = await Promise.all([
        profileClient.listProfiles({ pageSize: 500 }),
        sessionClient.listSessions({ pageSize: 500 }),
      ]);
      set({ profiles: profiles.profiles, sessions: sessions.sessions, loading: false, error: undefined });
      const selected = get().selectedSessionId;
      if (selected) await get().selectSession(selected);
    } catch (error) { set({ loading: false, error: errorText(error) }); }
  },
  selectSession: async (id) => {
    set({ selectedSessionId: id, loading: true });
    try {
      const response = await recordClient.listRecords({ filter: { sessionIds: [id] }, pageSize: 500 });
      set({ records: response.records, loading: false, error: undefined });
    } catch (error) { set({ loading: false, error: errorText(error) }); }
  },
  start: async (profileId) => {
    set({ loading: true });
    try { const response = await sessionClient.startSession({ profileId }); await get().refresh(); if (response.session) await get().selectSession(response.session.id); }
    catch (error) { set({ loading: false, error: errorText(error) }); }
  },
  stop: async (sessionId) => {
    try { await sessionClient.stopSession({ sessionId }); await get().refresh(); }
    catch (error) { set({ error: errorText(error) }); }
  },
  send: async (session, text, topic, qos, retain) => {
    const payload = { encoding: PayloadEncoding.TEXT, data: text, charset: "utf-8", appendDelimiter: new Uint8Array() };
    try {
      if (session.kind === SessionKind.MQTT_CLIENT) {
        const mappedQoS = qos === 2 ? MQTTQoS.MQTT_QOS_EXACTLY_ONCE : qos === 1 ? MQTTQoS.MQTT_QOS_AT_LEAST_ONCE : MQTTQoS.MQTT_QOS_AT_MOST_ONCE;
        await mqttClient.publish({ sessionId: session.id, topic, qos: mappedQoS, retain, payload });
      } else {
        await tcpClient.send({ targetSessionId: session.id, payload });
      }
      await get().selectSession(session.id);
    } catch (error) { set({ error: errorText(error) }); }
  },
  clear: async () => {
    const id = get().selectedSessionId; if (!id) return;
    try { await recordClient.clearRecords({ filter: { sessionIds: [id] } }); set({ records: [], selectedRecord: undefined }); }
    catch (error) { set({ error: errorText(error) }); }
  },
  subscribe: async (sessionId, filter, qos) => {
    const mappedQoS = qos === 2 ? MQTTQoS.MQTT_QOS_EXACTLY_ONCE : qos === 1 ? MQTTQoS.MQTT_QOS_AT_LEAST_ONCE : MQTTQoS.MQTT_QOS_AT_MOST_ONCE;
    try { await mqttClient.subscribe({ sessionId, subscriptions: [{ filter, qos: mappedQoS, noLocal: false, retainAsPublished: false, retainHandling: 0 }] }); }
    catch (error) { set({ error: errorText(error) }); }
  },
  unsubscribe: async (sessionId, filter) => {
    try { await mqttClient.unsubscribe({ sessionId, filters: [filter] }); }
    catch (error) { set({ error: errorText(error) }); }
  },
  selectRecord: (record) => set({ selectedRecord: record }),
}));
