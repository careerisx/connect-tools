import { create } from "zustand";

import { mqttClient, profileClient, recordClient, sessionClient, tcpClient } from "../api/client";
import type { CoreEvent } from "../gen/connecttools/v1/core_pb";
import type { Profile } from "../gen/connecttools/v1/profile_pb";
import type { DebugRecord } from "../gen/connecttools/v1/records_pb";
import type { Session } from "../gen/connecttools/v1/session_pb";
import { MQTTQoS } from "../gen/connecttools/v1/mqtt_pb";
import { RecordEventType } from "../gen/connecttools/v1/records_pb";
import { PayloadEncoding, SessionEventType, SessionKind, SessionState } from "../gen/connecttools/v1/session_pb";

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
  start(profileId: string, displayName?: string): Promise<boolean>;
  stop(sessionId: string): Promise<void>;
  send(session: Session, data: string, encoding: PayloadEncoding, topic: string, qos: number, retain: boolean): Promise<void>;
  clear(): Promise<void>;
  subscribe(sessionId: string, filter: string, qos: number): Promise<void>;
  unsubscribe(sessionId: string, filter: string): Promise<void>;
  /** Applies one ordered Core event received from the WebSocket stream. */
  applyEvent(event: CoreEvent): Promise<void>;
  selectRecord(record: DebugRecord): void;
};

const errorText = (error: unknown) => error instanceof Error ? error.message : "操作失败";

/** 判断会话是否仍应显示在活动会话列表中。 */
const isActiveSession = (session: Session) => (
  session.state === SessionState.STARTING ||
  session.state === SessionState.RUNNING ||
  session.state === SessionState.STOPPING
);

/** 返回当前工作区需要加载的记录会话 ID，TCP 服务端同时包含全部接入端。 */
const recordSessionIds = (sessionId: string, sessions: Session[]) => {
  const session = sessions.find((value) => value.id === sessionId);
  if (session?.kind !== SessionKind.TCP_SERVER) return [sessionId];
  return [sessionId, ...sessions
    .filter((value) => value.kind === SessionKind.TCP_PEER && value.parentId === sessionId)
    .map((value) => value.id)];
};

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
      if (selected && sessions.sessions.some((session) => session.id === selected && isActiveSession(session))) {
        await get().selectSession(selected);
      } else if (selected) {
        set({ selectedSessionId: undefined, selectedRecord: undefined, records: [] });
      }
    } catch (error) { set({ loading: false, error: errorText(error) }); }
  },
  selectSession: async (id) => {
    set({ selectedSessionId: id, loading: true });
    try {
      const sessionIds = recordSessionIds(id, get().sessions);
      const response = await recordClient.listRecords({ filter: { sessionIds }, pageSize: 500 });
      if (get().selectedSessionId !== id) return;
      set((state) => ({
        records: response.records,
        selectedRecord: state.selectedRecord && response.records.some((record) => record.sequence === state.selectedRecord?.sequence)
          ? state.selectedRecord
          : undefined,
        loading: false,
        error: undefined,
      }));
    } catch (error) {
      if (get().selectedSessionId === id) set({ loading: false, error: errorText(error) });
    }
  },
  start: async (profileId, displayName) => {
    const existing = get().sessions.find((session) => (
      !session.parentId && session.profileId === profileId && isActiveSession(session)
    ));
    if (existing) {
      await get().selectSession(existing.id);
      return true;
    }
    set({ loading: true });
    try {
      const response = await sessionClient.startSession({ profileId, displayName: displayName?.trim() || undefined });
      if (response.session) {
        set((state) => ({
          sessions: state.sessions.some((session) => session.id === response.session?.id)
            ? state.sessions
            : [...state.sessions, response.session!],
          selectedSessionId: response.session!.id,
          selectedRecord: undefined,
          records: [],
          loading: false,
          error: undefined,
        }));
        return true;
      } else {
        set({ loading: false, error: undefined });
        return false;
      }
    }
    catch (error) {
      set({ loading: false, error: errorText(error) });
      return false;
    }
  },
  stop: async (sessionId) => {
    set({ loading: true });
    try {
      const response = await sessionClient.stopSession({ sessionId });
      set((state) => ({
        sessions: response.session
          ? state.sessions.map((session) => session.id === sessionId ? response.session! : session)
          : state.sessions,
        selectedSessionId: state.selectedSessionId === sessionId ? undefined : state.selectedSessionId,
        selectedRecord: state.selectedSessionId === sessionId ? undefined : state.selectedRecord,
        records: state.selectedSessionId === sessionId ? [] : state.records,
        loading: false,
        error: undefined,
      }));
    }
    catch (error) { set({ loading: false, error: errorText(error) }); }
  },
  send: async (session, data, encoding, topic, qos, retain) => {
    const payload = { encoding, data, charset: encoding === PayloadEncoding.TEXT ? "utf-8" : "", appendDelimiter: new Uint8Array() };
    try {
      if (session.kind === SessionKind.MQTT_CLIENT) {
        const mappedQoS = qos === 2 ? MQTTQoS.MQTT_QOS_EXACTLY_ONCE : qos === 1 ? MQTTQoS.MQTT_QOS_AT_LEAST_ONCE : MQTTQoS.MQTT_QOS_AT_MOST_ONCE;
        await mqttClient.publish({ sessionId: session.id, topic, qos: mappedQoS, retain, payload });
      } else {
        await tcpClient.send({ targetSessionId: session.id, payload });
      }
    } catch (error) { set({ error: errorText(error) }); }
  },
  clear: async () => {
    const id = get().selectedSessionId; if (!id) return;
    const sessionIds = recordSessionIds(id, get().sessions);
    try { await recordClient.clearRecords({ filter: { sessionIds } }); }
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
  applyEvent: async (event) => {
    switch (event.eventBody.case) {
      case "profileEvent": {
        try {
          const response = await profileClient.listProfiles({ pageSize: 500 });
          set({ profiles: response.profiles, error: undefined });
        } catch (error) {
          set({ error: errorText(error) });
          throw error;
        }
        return;
      }
      case "sessionEvent": {
        const sessionEvent = event.eventBody.value;
        set((state) => {
          if (sessionEvent.type === SessionEventType.REMOVED) {
            const selectedRemoved = state.selectedSessionId === sessionEvent.sessionId;
            return {
              sessions: state.sessions.filter((session) => session.id !== sessionEvent.sessionId),
              selectedSessionId: selectedRemoved ? undefined : state.selectedSessionId,
              records: selectedRemoved ? [] : state.records,
              selectedRecord: selectedRemoved ? undefined : state.selectedRecord,
            };
          }
          if (sessionEvent.session === undefined) return state;
          const index = state.sessions.findIndex((session) => session.id === sessionEvent.sessionId);
          if (index < 0) return { sessions: [...state.sessions, sessionEvent.session] };
          const sessions = [...state.sessions];
          sessions[index] = sessionEvent.session;
          const selectedStopped = state.selectedSessionId === sessionEvent.sessionId && !isActiveSession(sessionEvent.session);
          return {
            sessions,
            selectedSessionId: selectedStopped ? undefined : state.selectedSessionId,
            records: selectedStopped ? [] : state.records,
            selectedRecord: selectedStopped ? undefined : state.selectedRecord,
          };
        });
        return;
      }
      case "recordEvent": {
        const recordEvent = event.eventBody.value;
        const selectedSessionId = get().selectedSessionId;
        if (!selectedSessionId) return;
        const sessionIds = recordSessionIds(selectedSessionId, get().sessions);
        if (recordEvent.type === RecordEventType.APPENDED && recordEvent.record && sessionIds.includes(recordEvent.record.sessionId)) {
          if (event.payloadTruncated) {
            await get().selectSession(selectedSessionId);
            return;
          }
          set((state) => {
            if (state.records.some((record) => record.sequence === recordEvent.record?.sequence)) return state;
            return { records: [...state.records.slice(-499), recordEvent.record!] };
          });
          return;
        }
        const selectedAffected = recordEvent.sessionIds.length === 0 || recordEvent.sessionIds.some((id) => sessionIds.includes(id));
        if (selectedAffected && (
          recordEvent.type === RecordEventType.CLEARED ||
          recordEvent.type === RecordEventType.RETENTION_LIMIT_REACHED
        )) {
          await get().selectSession(selectedSessionId);
        }
        return;
      }
      default:
        return;
    }
  },
  selectRecord: (record) => set({ selectedRecord: record }),
}));
