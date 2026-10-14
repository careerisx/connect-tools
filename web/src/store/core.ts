import { fromBinary } from "@bufbuild/protobuf";
import { create } from "zustand";

import { coreClient } from "../api/client";
import { CoreEventSchema, type CoreEvent, type CoreStatus } from "../gen/connecttools/v1/core_pb";

type CoreEventHandler = (event: CoreEvent) => Promise<void> | void;

/** Core connection state shared by the application shell. */
export type CoreState = {
  status?: CoreStatus;
  connected: boolean;
  watching: boolean;
  error?: string;
  lastSequence: bigint;
  load(signal?: AbortSignal): Promise<void>;
  watch(signal: AbortSignal, onEvent: CoreEventHandler): Promise<void>;
};

type ActiveWatch = {
  token: symbol;
  signal: AbortSignal;
  promise: Promise<void>;
};

let activeWatch: ActiveWatch | undefined;

/** Returns a Chinese message suitable for the connection status banner. */
function errorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim() !== "") {
    return error.message;
  }
  return "无法连接到连接工具服务";
}

/** Builds the same-origin WebSocket URL with the last processed event cursor. */
function eventWebSocketURL(afterSequence: bigint): string {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const url = new URL("/ws/events", `${protocol}//${window.location.host}`);
  url.searchParams.set("after", afterSequence.toString());
  return url.toString();
}

/** Opens one WebSocket session and processes binary CoreEvent frames in order. */
function watchWebSocket(
  signal: AbortSignal,
  afterSequence: bigint,
  onEvent: CoreEventHandler,
): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      resolve();
      return;
    }

    const socket = new WebSocket(eventWebSocketURL(afterSequence));
    socket.binaryType = "arraybuffer";
    let settled = false;
    let eventQueue = Promise.resolve();

    const finish = (error?: unknown) => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", abort);
      if (error === undefined) resolve();
      else reject(error);
    };
    const abort = () => {
      socket.close(1000, "页面已关闭");
      finish();
    };

    signal.addEventListener("abort", abort, { once: true });
    socket.addEventListener("message", (message) => {
      if (!(message.data instanceof ArrayBuffer)) {
        socket.close(1003, "仅支持二进制事件");
        finish(new Error("收到不支持的事件格式"));
        return;
      }

      let event: CoreEvent;
      try {
        event = fromBinary(CoreEventSchema, new Uint8Array(message.data));
      } catch (error) {
        socket.close(1007, "事件数据无效");
        finish(error);
        return;
      }

      eventQueue = eventQueue.then(() => onEvent(event));
      void eventQueue.catch((error) => {
        socket.close(1011, "事件处理失败");
        finish(error);
      });
    });
    socket.addEventListener("close", (event) => {
      void eventQueue.then(
        () => finish(signal.aborted || event.code === 1000 ? undefined : new Error("WebSocket 事件流已断开")),
        finish,
      );
    });
    socket.addEventListener("error", () => {
      // The close event carries the terminal connection state.
    });
  });
}

/** Holds the Core status and maintains the browser WebSocket event stream. */
export const useCoreStore = create<CoreState>((set, get) => ({
  connected: false,
  watching: false,
  lastSequence: 0n,

  load: async (signal) => {
    try {
      const status = await coreClient.getStatus({}, { signal });
      if (signal?.aborted) return;
      set((state) => {
        const instanceChanged =
          state.status?.instanceId !== undefined &&
          state.status.instanceId !== "" &&
          status.instanceId !== "" &&
          state.status.instanceId !== status.instanceId;
        return {
          status,
          connected: true,
          error: undefined,
          lastSequence: instanceChanged ? 0n : state.lastSequence,
        };
      });
    } catch (error) {
      if (signal?.aborted) return;
      set({ connected: false, error: errorMessage(error) });
      throw error;
    }
  },

  watch: (signal, onEvent) => {
    if (activeWatch !== undefined && !activeWatch.signal.aborted) {
      return activeWatch.promise;
    }

    const token = Symbol("core-websocket-watch");
    const promise = (async () => {
      set({ watching: true });
      try {
        await watchWebSocket(signal, get().lastSequence, async (event) => {
          const lifecycle = event.eventBody.case === "coreLifecycleEvent"
            ? event.eventBody.value
            : undefined;
          set((state) => ({
            status: lifecycle?.status ?? state.status,
            connected: lifecycle === undefined,
            error: lifecycle === undefined ? undefined : "连接工具服务正在停止",
            lastSequence: event.sequence > state.lastSequence ? event.sequence : state.lastSequence,
          }));
          await onEvent(event);
        });

        if (!signal.aborted) throw new Error("WebSocket 事件流已断开");
      } catch (error) {
        if (signal.aborted) return;
        set({ connected: false, error: errorMessage(error) });
        throw error;
      } finally {
        if (activeWatch?.token === token) set({ watching: false });
      }
    })();

    activeWatch = { token, signal, promise };
    void promise.finally(() => {
      if (activeWatch?.promise === promise) activeWatch = undefined;
    }).catch(() => undefined);
    return promise;
  },
}));
