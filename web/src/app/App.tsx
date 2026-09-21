import { useEffect } from "react";

import { AppShell } from "../components/layout/AppShell";
import { useCoreStore } from "../store/core";

const INITIAL_RETRY_DELAY_MS = 250;
const MAX_RETRY_DELAY_MS = 5_000;
const STABLE_CONNECTION_MS = 10_000;

function waitForRetry(delayMs: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    const timeout = window.setTimeout(finish, delayMs);

    function finish() {
      window.clearTimeout(timeout);
      signal.removeEventListener("abort", finish);
      resolve();
    }

    signal.addEventListener("abort", finish, { once: true });
  });
}

/** Starts the Core snapshot and event stream for the lifetime of the shell. */
export function App() {
  const load = useCoreStore((state) => state.load);
  const watch = useCoreStore((state) => state.watch);

  useEffect(() => {
    const controller = new AbortController();

    const maintainConnection = async () => {
      let retryDelay = INITIAL_RETRY_DELAY_MS;

      while (!controller.signal.aborted) {
        const attemptStartedAt = Date.now();

        try {
          await load(controller.signal);
          await watch(controller.signal);
        } catch {
          // Store actions publish the user-facing error before retrying.
        }

        if (controller.signal.aborted) {
          break;
        }

        await waitForRetry(retryDelay, controller.signal);
        retryDelay =
          Date.now() - attemptStartedAt >= STABLE_CONNECTION_MS
            ? INITIAL_RETRY_DELAY_MS
            : Math.min(retryDelay * 2, MAX_RETRY_DELAY_MS);
      }
    };

    void maintainConnection();
    return () => controller.abort();
  }, [load, watch]);

  return <AppShell />;
}
