import type { Alert, AlertFrame, Frame, Snapshot } from "@/api/types";

export type LiveStatus = "connecting" | "open" | "reconnecting" | "closed";

export interface WebSocketLike {
  onopen: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onclose: ((ev: { code?: number; reason?: string }) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  close(code?: number, reason?: string): void;
}

export interface LiveOptions {
  /** Builds the ws:// URL; called for every (re)connection with the resume parameters. */
  buildUrl: (resume: { lastSeq: number; epoch: number }) => Promise<string>;
  createSocket: (url: string) => WebSocketLike;
  onSnapshot: (s: Snapshot) => void;
  onAlert: (a: Alert, event: AlertFrame["event"]) => void;
  onStatus: (s: LiveStatus, info: { attempt: number }) => void;
  /** Called when the server closes the socket with an auth failure so the caller can refresh the token. */
  onAuthProblem?: () => void;
  staleMs?: number;
  backoffMs?: number[];
  setTimer?: (fn: () => void, ms: number) => unknown;
  clearTimer?: (h: unknown) => void;
  now?: () => number;
}

const DEFAULT_BACKOFF = [500, 1000, 2000, 4000, 8000];

/**
 * WebSocket client with resume. It remembers the last `seq` and the server `epoch` and sends them when it
 * reconnects, so the server replays the alerts it missed plus the latest snapshot and the client never shows a gap.
 * Frames whose seq is not newer than the last seen one are ignored (no duplicates after a resume).
 */
export class LiveClient {
  private socket: WebSocketLike | null = null;
  private lastSeq = 0;
  private epoch = 0;
  private attempt = 0;
  private manuallyClosed = false;
  private retryTimer: unknown = null;
  private staleTimer: unknown = null;
  private generation = 0;

  constructor(private readonly o: LiveOptions) {}

  get resume(): { lastSeq: number; epoch: number } {
    return { lastSeq: this.lastSeq, epoch: this.epoch };
  }

  private set(fn: () => void, ms: number): unknown {
    return (this.o.setTimer ?? setTimeout)(fn, ms);
  }
  private clear(h: unknown): void {
    if (h !== null) (this.o.clearTimer ?? ((x) => clearTimeout(x as ReturnType<typeof setTimeout>)))(h);
  }

  async start(): Promise<void> {
    this.manuallyClosed = false;
    await this.connect();
  }

  stop(): void {
    this.manuallyClosed = true;
    this.generation++;
    this.clear(this.retryTimer);
    this.clear(this.staleTimer);
    this.retryTimer = this.staleTimer = null;
    const s = this.socket;
    this.socket = null;
    if (s) {
      s.onclose = s.onmessage = s.onerror = s.onopen = null;
      s.close(1000, "client closed");
    }
    this.o.onStatus("closed", { attempt: this.attempt });
  }

  private async connect(): Promise<void> {
    const gen = ++this.generation;
    this.o.onStatus(this.attempt === 0 ? "connecting" : "reconnecting", { attempt: this.attempt });
    let url: string;
    try {
      url = await this.o.buildUrl({ lastSeq: this.lastSeq, epoch: this.epoch });
    } catch {
      this.scheduleReconnect();
      return;
    }
    if (gen !== this.generation || this.manuallyClosed) return;
    const sock = this.o.createSocket(url);
    this.socket = sock;
    sock.onopen = () => {
      if (gen !== this.generation) return;
      this.attempt = 0;
      this.o.onStatus("open", { attempt: 0 });
      this.armStale();
    };
    sock.onmessage = (ev) => {
      if (gen !== this.generation) return;
      this.armStale();
      this.handle(ev.data);
    };
    sock.onerror = () => {
      /* a close event always follows; reconnect logic lives there */
    };
    sock.onclose = (ev) => {
      if (gen !== this.generation || this.manuallyClosed) return;
      this.clear(this.staleTimer);
      this.socket = null;
      if (ev.code === 1008 || ev.code === 4401) this.o.onAuthProblem?.();
      this.scheduleReconnect();
    };
  }

  private armStale(): void {
    this.clear(this.staleTimer);
    this.staleTimer = this.set(() => {
      // no frame for a few intervals: the socket is dead without having closed. Drop it and reconnect with resume.
      const s = this.socket;
      if (s) s.close(4000, "stale");
      if (this.socket === s) {
        this.socket = null;
        this.scheduleReconnect();
      }
    }, this.o.staleMs ?? 7000);
  }

  private scheduleReconnect(): void {
    if (this.manuallyClosed) return;
    const table = this.o.backoffMs ?? DEFAULT_BACKOFF;
    const delay = table[Math.min(this.attempt, table.length - 1)] ?? 1000;
    this.attempt++;
    this.o.onStatus("reconnecting", { attempt: this.attempt });
    this.generation++; // invalidate the dead socket's callbacks
    this.clear(this.retryTimer);
    this.retryTimer = this.set(() => {
      void this.connect();
    }, delay);
  }

  private handle(raw: unknown): void {
    if (typeof raw !== "string") return;
    let f: Frame;
    try {
      f = JSON.parse(raw) as Frame;
    } catch {
      return;
    }
    if (f.type === "hello") {
      if (f.epoch !== this.epoch) {
        // a new server run: its seq starts over, so our remembered position means nothing
        this.epoch = f.epoch;
        if (!f.resumed) this.lastSeq = 0;
      }
      return;
    }
    if (f.seq <= this.lastSeq) return; // already seen (or older): resume must never duplicate
    this.lastSeq = f.seq;
    if (f.type === "snapshot") this.o.onSnapshot(f.data);
    else this.o.onAlert(f.data, f.event);
  }
}
