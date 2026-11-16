import { useEffect, useRef, useState } from "react";
import type { Alert, Snapshot } from "@/api/types";
import { apiBaseUrl } from "@/config";
import { LiveClient, type LiveStatus, type WebSocketLike } from "./LiveClient";

export interface LiveState {
  status: LiveStatus;
  attempt: number;
  latest: Snapshot | null;
  history: Snapshot[];
  lastAlertEvent: { alert: Alert; at: number } | null;
}

const HISTORY = 60;

export function wsUrl(base: string, token: string, resume: { lastSeq: number; epoch: number }): string {
  const u = base.replace(/^http/, "ws");
  const q = new URLSearchParams({ access_token: token });
  if (resume.lastSeq > 0) {
    q.set("last_seq", String(resume.lastSeq));
    q.set("epoch", String(resume.epoch));
  }
  return `${u}/v1/live?${q.toString()}`;
}

/** Live snapshots every 2 s with automatic reconnect + resume. `getToken` must return a fresh access token. */
export function useLive(getToken: () => Promise<string>, onAuthProblem?: () => void, enabled = true): LiveState {
  const [state, setState] = useState<LiveState>({ status: "connecting", attempt: 0, latest: null, history: [], lastAlertEvent: null });
  const tokenRef = useRef(getToken);
  const authRef = useRef(onAuthProblem);
  useEffect(() => {
    tokenRef.current = getToken;
    authRef.current = onAuthProblem;
  });

  useEffect(() => {
    if (!enabled) return;
    const client = new LiveClient({
      buildUrl: async (resume) => wsUrl(apiBaseUrl(), await tokenRef.current(), resume),
      createSocket: (url) => new WebSocket(url) as unknown as WebSocketLike,
      onSnapshot: (s) => setState((p) => ({ ...p, latest: s, history: [...p.history, s].slice(-HISTORY) })),
      onAlert: (alert) => setState((p) => ({ ...p, lastAlertEvent: { alert, at: Date.now() } })),
      onStatus: (status, info) => setState((p) => ({ ...p, status, attempt: info.attempt })),
      onAuthProblem: () => authRef.current?.(),
    });
    void client.start();
    return () => client.stop();
  }, [enabled]);

  return state;
}
