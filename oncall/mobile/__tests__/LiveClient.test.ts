import type { Alert, Snapshot } from "@/api/types";
import { LiveClient, type LiveStatus, type WebSocketLike } from "@/live/LiveClient";
import { wsUrl } from "@/live/useLive";

class FakeSocket implements WebSocketLike {
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: { code?: number; reason?: string }) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  closed: { code?: number; reason?: string } | null = null;
  constructor(readonly url: string) {}
  close(code?: number, reason?: string): void {
    this.closed = { code, reason };
  }
  open(): void { this.onopen?.({}); }
  send(frame: unknown): void { this.onmessage?.({ data: JSON.stringify(frame) }); }
  drop(code = 1006): void { this.onclose?.({ code }); }
}

const snap = (seq: number): Snapshot => ({ seq, lag_total: seq * 10 } as unknown as Snapshot);
const frame = (seq: number) => ({ type: "snapshot", seq, data: snap(seq) });
const alertFrame = (seq: number, event: string) => ({ type: "alert", seq, event, data: { id: seq, rule: "consumer_lag_high", state: event } as unknown as Alert });
const hello = (epoch: number, resumed = false) => ({ type: "hello", seq: 0, epoch, resumed, missed_alerts: 0 });

function setup(overrides: Partial<ConstructorParameters<typeof LiveClient>[0]> = {}) {
  const sockets: FakeSocket[] = [];
  const snaps: number[] = [];
  const alerts: string[] = [];
  const statuses: LiveStatus[] = [];
  const timers: { fn: () => void; ms: number; id: number; live: boolean }[] = [];
  let nextId = 1;
  const client = new LiveClient({
    buildUrl: async (r) => `ws://x/v1/live?last_seq=${String(r.lastSeq)}&epoch=${String(r.epoch)}`,
    createSocket: (url) => { const s = new FakeSocket(url); sockets.push(s); return s; },
    onSnapshot: (s) => snaps.push(s.seq),
    onAlert: (a, ev) => alerts.push(`${ev}:${String(a.id)}`),
    onStatus: (s) => statuses.push(s),
    setTimer: (fn, ms) => { const t = { fn, ms, id: nextId++, live: true }; timers.push(t); return t.id; },
    clearTimer: (h) => { const t = timers.find((x) => x.id === h); if (t) t.live = false; },
    ...overrides,
  });
  const fire = (pred: (t: { ms: number }) => boolean = () => true): void => {
    const t = timers.filter((x) => x.live && pred(x)).pop();
    if (!t) throw new Error("no live timer");
    t.live = false;
    t.fn();
  };
  const flush = (): Promise<void> => new Promise((r) => setImmediate(r));
  return { client, sockets, snaps, alerts, statuses, timers, fire, flush };
}

describe("LiveClient", () => {
  it("connects, reports open and delivers snapshots and alerts in order", async () => {
    const t = setup();
    await t.client.start();
    const s = t.sockets[0]!;
    expect(s.url).toBe("ws://x/v1/live?last_seq=0&epoch=0");
    s.open();
    s.send(hello(111));
    s.send(frame(1));
    s.send(alertFrame(2, "firing"));
    s.send(frame(3));
    expect(t.statuses).toEqual(["connecting", "open"]);
    expect(t.snaps).toEqual([1, 3]);
    expect(t.alerts).toEqual(["firing:2"]);
    expect(t.client.resume).toEqual({ lastSeq: 3, epoch: 111 });
  });

  it("reconnects after a drop and resumes from the last seq and epoch", async () => {
    const t = setup();
    await t.client.start();
    const s1 = t.sockets[0]!;
    s1.open();
    s1.send(hello(111));
    s1.send(frame(1));
    s1.send(frame(2));
    s1.drop();
    expect(t.statuses.at(-1)).toBe("reconnecting");
    t.fire((x) => x.ms === 500); // first backoff
    await t.flush();
    const s2 = t.sockets[1]!;
    expect(s2.url).toBe("ws://x/v1/live?last_seq=2&epoch=111");
    s2.open();
    s2.send(hello(111, true));
    s2.send(alertFrame(3, "firing")); // missed while offline
    s2.send(frame(4)); // the latest snapshot
    expect(t.snaps).toEqual([1, 2, 4]);
    expect(t.alerts).toEqual(["firing:3"]);
    expect(t.statuses.at(-1)).toBe("open");
  });

  it("ignores frames it has already seen (resume must never duplicate)", async () => {
    const t = setup();
    await t.client.start();
    const s = t.sockets[0]!;
    s.open();
    s.send(hello(1));
    s.send(frame(5));
    s.send(frame(5));
    s.send(frame(4));
    s.send(alertFrame(5, "firing"));
    expect(t.snaps).toEqual([5]);
    expect(t.alerts).toEqual([]);
  });

  it("a different server epoch means the seq restarted: the old position is dropped", async () => {
    const t = setup();
    await t.client.start();
    const s1 = t.sockets[0]!;
    s1.open();
    s1.send(hello(111));
    s1.send(frame(500));
    s1.drop();
    t.fire();
    await t.flush();
    const s2 = t.sockets[1]!;
    expect(s2.url).toContain("last_seq=500&epoch=111");
    s2.open();
    s2.send(hello(222, false)); // the server restarted
    s2.send(frame(1)); // would be ignored as "old" if we kept seq 500
    expect(t.snaps).toEqual([500, 1]);
    expect(t.client.resume).toEqual({ lastSeq: 1, epoch: 222 });
  });

  it("backs off exponentially up to a cap and resets after a successful connection", async () => {
    const t = setup({ backoffMs: [100, 200, 400] });
    await t.client.start();
    const delays: number[] = [];
    for (let i = 0; i < 5; i++) {
      t.sockets.at(-1)!.drop();
      delays.push(t.timers.filter((x) => x.live).at(-1)!.ms);
      t.fire();
      await t.flush();
    }
    expect(delays).toEqual([100, 200, 400, 400, 400]);
    const s = t.sockets.at(-1)!;
    s.open(); // success resets the attempt counter
    s.drop();
    expect(t.timers.filter((x) => x.live).at(-1)!.ms).toBe(100);
  });

  it("treats a silent socket as dead: closes it and reconnects", async () => {
    const t = setup({ staleMs: 7000 });
    await t.client.start();
    const s = t.sockets[0]!;
    s.open();
    s.send(hello(1));
    s.send(frame(1));
    t.fire((x) => x.ms === 7000); // the stale timer expires without another frame
    expect(s.closed).toEqual({ code: 4000, reason: "stale" });
    expect(t.statuses.at(-1)).toBe("reconnecting");
    t.fire((x) => x.ms === 500);
    await t.flush();
    expect(t.sockets).toHaveLength(2);
    expect(t.sockets[1]!.url).toContain("last_seq=1&epoch=1");
  });

  it("stop() closes the socket and never reconnects", async () => {
    const t = setup();
    await t.client.start();
    const s = t.sockets[0]!;
    s.open();
    t.client.stop();
    expect(s.closed?.code).toBe(1000);
    s.drop(); // a late close event from the old socket
    expect(t.timers.filter((x) => x.live)).toHaveLength(0);
    expect(t.statuses.at(-1)).toBe("closed");
    expect(t.sockets).toHaveLength(1);
  });

  it("tells the app when the server closed for an auth problem so it can refresh the token", async () => {
    const calls: number[] = [];
    const t = setup({ onAuthProblem: () => calls.push(1) });
    await t.client.start();
    t.sockets[0]!.open();
    t.sockets[0]!.drop(1008);
    expect(calls).toHaveLength(1);
    expect(t.statuses.at(-1)).toBe("reconnecting");
  });

  it("retries when building the URL fails (for example the token refresh was offline)", async () => {
    let calls = 0;
    const t = setup({ buildUrl: async () => { calls++; if (calls === 1) throw new Error("offline"); return "ws://x"; } });
    await t.client.start();
    expect(t.sockets).toHaveLength(0);
    t.fire();
    await t.flush();
    expect(t.sockets).toHaveLength(1);
  });

  it("ignores malformed frames", async () => {
    const t = setup();
    await t.client.start();
    const s = t.sockets[0]!;
    s.open();
    s.onmessage?.({ data: "not json" });
    s.onmessage?.({ data: new ArrayBuffer(4) });
    s.send(frame(1));
    expect(t.snaps).toEqual([1]);
  });
});

describe("wsUrl", () => {
  it("swaps the scheme and only adds resume parameters when there is something to resume", () => {
    expect(wsUrl("http://127.0.0.1:8088", "tok", { lastSeq: 0, epoch: 0 })).toBe("ws://127.0.0.1:8088/v1/live?access_token=tok");
    expect(wsUrl("https://api.example.com", "tok", { lastSeq: 12, epoch: 99 })).toBe("wss://api.example.com/v1/live?access_token=tok&last_seq=12&epoch=99");
  });
});
