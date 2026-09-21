import { create } from "zustand";

import { coreClient } from "../api/client";
import type { CoreStatus } from "../gen/connecttools/v1/core_pb";

/** Core connection state shared by the shell and future feature reducers. */
export type CoreState = {
  status?: CoreStatus;
  connected: boolean;
  watching: boolean;
  error?: string;
  lastSequence: bigint;
  load(signal?: AbortSignal): Promise<void>;
  watch(signal: AbortSignal): Promise<void>;
};

type ActiveWatch = {
  token: symbol;
  signal: AbortSignal;
  promise: Promise<void>;
};

let activeWatch: ActiveWatch | undefined;

function errorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim() !== "") {
    return error.message;
  }
  return "Unable to reach the Connect Tools service";
}

/**
 * Holds the authoritative Core status and the live event cursor.
 */
export const useCoreStore = create<CoreState>((set, get) => ({
  connected: false,
  watching: false,
  lastSequence: 0n,

  load: async (signal) => {
    try {
      const status = await coreClient.getStatus({}, { signal });
      if (signal?.aborted) {
        return;
      }
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
      if (signal?.aborted) {
        return;
      }
      set({ connected: false, error: errorMessage(error) });
      throw error;
    }
  },

  watch: (signal) => {
    if (activeWatch !== undefined && !activeWatch.signal.aborted) {
      return activeWatch.promise;
    }

    const token = Symbol("core-watch");
    const promise = (async () => {
      set({ watching: true });

      try {
        const events = coreClient.watchEvents(
          { afterSequence: get().lastSequence },
          { signal },
        );

        for await (const event of events) {
          set((state) => ({
            connected: event.eventBody.case === "coreLifecycleEvent" ? false : true,
            error:
              event.eventBody.case === "coreLifecycleEvent"
                ? "The Connect Tools service is stopping"
                : undefined,
            lastSequence:
              event.sequence > state.lastSequence
                ? event.sequence
                : state.lastSequence,
          }));
        }

        if (!signal.aborted) {
          const error = new Error("The live event stream disconnected");
          set({ connected: false, error: error.message });
          throw error;
        }
      } catch (error) {
        if (signal.aborted) {
          return;
        }
        set({ connected: false, error: errorMessage(error) });
        throw error;
      } finally {
        if (activeWatch?.token === token) {
          set({ watching: false });
        }
      }
    })();

    activeWatch = { token, signal, promise };
    void promise.then(
      () => {
        if (activeWatch?.promise === promise) {
          activeWatch = undefined;
        }
      },
      () => {
        if (activeWatch?.promise === promise) {
          activeWatch = undefined;
        }
      },
    );

    return promise;
  },
}));
