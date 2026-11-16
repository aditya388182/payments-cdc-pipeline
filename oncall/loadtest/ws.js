// k6 WebSocket load test for the on-call API (SPEC section 10).
//
//   TOKEN=<access token> k6 run -e VARIANT=baseline oncall/loadtest/ws.js
//
// TARGET (default 1,000) clients connect evenly spread over RAMP (default 60s), all stay until RAMP+HOLD (HOLD default
// 5m) and then close themselves, so the hold phase has exactly TARGET concurrent connections. Every client checks that
// `seq` only ever increases. Measured per run: connections established / failed, snapshots received per client against
// the number expected for the time it was connected, delivery latency (client receive time minus the snapshot's server
// timestamp: client and server share one box, so the clocks match), unexpected disconnects. The first snapshot on each
// connection is the server's "latest snapshot" replay (up to 2 s old by design), so it is counted but not timed.
// Server CPU and memory are sampled separately from /metrics by oncall/loadtest/run.sh.
// The summary is written to results/oncall/raw/ws_<VARIANT>.json.
import { WebSocket } from "k6/websockets";
import { sleep } from "k6";
import { Counter, Gauge, Trend } from "k6/metrics";

const TARGET = Number(__ENV.TARGET || 1000);
const RAMP = __ENV.RAMP || "60s";
const HOLD = __ENV.HOLD || "5m";
const VARIANT = __ENV.VARIANT || "baseline";
const BASE = __ENV.WS_URL || "ws://127.0.0.1:8088/v1/live";
const TOKEN = __ENV.TOKEN;
if (!TOKEN) {
  throw new Error("TOKEN env var (a viewer or admin access token) is required");
}

function seconds(d) {
  const m = /^(\d+)(s|m)$/.exec(d);
  return m ? Number(m[1]) * (m[2] === "m" ? 60 : 1) : 0;
}
const HOLD_S = seconds(HOLD);
const RAMP_S = seconds(RAMP);

export const options = {
  scenarios: {
    clients: {
      executor: "per-vu-iterations",
      vus: TARGET,
      iterations: 1,
      maxDuration: `${RAMP_S + HOLD_S + 120}s`,
    },
  },
  summaryTrendStats: ["min", "med", "p(90)", "p(95)", "p(99)", "max", "count"],
};

const connected = new Counter("ws_connected");
const connectFailed = new Counter("ws_connect_failed");
const unexpectedClose = new Counter("ws_unexpected_close");
const snapshots = new Counter("ws_snapshots_received");
const alertsSeen = new Counter("ws_alerts_received");
const seqViolations = new Counter("ws_seq_violations");
const delivery = new Trend("ws_delivery_ms", true);
const perClientSnapshots = new Trend("ws_snapshots_per_client");
const perClientRatio = new Trend("ws_received_over_expected");
const peakConnected = new Gauge("ws_peak_connected");
let open = 0;

export default function () {
  // spread the connection attempts evenly over the ramp
  const delayS = ((__VU - 1) / TARGET) * RAMP_S;
  sleep(delayS);
  const holdForS = Math.max(5, RAMP_S + HOLD_S - delayS);

  const ws = new WebSocket(`${BASE}?access_token=${TOKEN}`);
  let lastSeq = 0;
  let count = 0;
  let openedAt = 0;
  let stopping = false;

  ws.onopen = () => {
    openedAt = Date.now();
    open++;
    connected.add(1);
    peakConnected.add(open);
    setTimeout(() => {
      stopping = true;
      ws.close(1000, "done");
    }, holdForS * 1000);
  };
  ws.onmessage = (ev) => {
    const recv = Date.now();
    let m;
    try {
      m = JSON.parse(ev.data);
    } catch (e) {
      return;
    }
    if (m.type === "snapshot") {
      count++;
      snapshots.add(1);
      if (m.seq <= lastSeq) {
        seqViolations.add(1);
      }
      lastSeq = m.seq;
      if (count > 1) {
        const ts = Date.parse(m.data.ts);
        if (!Number.isNaN(ts)) {
          delivery.add(recv - ts);
        }
      }
    } else if (m.type === "alert") {
      alertsSeen.add(1);
      if (m.seq <= lastSeq) {
        seqViolations.add(1);
      }
      lastSeq = m.seq;
    }
  };
  ws.onerror = () => {
    if (!openedAt) {
      connectFailed.add(1);
    }
  };
  ws.onclose = () => {
    if (openedAt) {
      open--;
      if (!stopping) {
        unexpectedClose.add(1);
      }
      const connectedS = (Date.now() - openedAt) / 1000;
      perClientSnapshots.add(count);
      // the server sends the latest snapshot immediately and then one every 2 s
      perClientRatio.add(count / (1 + Math.floor(connectedS / 2)));
    }
  };
}

export function handleSummary(data) {
  const m = data.metrics;
  const get = (name, key) => (m[name] && m[name].values ? m[name].values[key] : null);
  const out = {
    variant: VARIANT,
    target_clients: TARGET,
    ramp: RAMP,
    hold: HOLD,
    connected: get("ws_connected", "count"),
    connect_failed: get("ws_connect_failed", "count") || 0,
    unexpected_close: get("ws_unexpected_close", "count") || 0,
    snapshots_received: get("ws_snapshots_received", "count"),
    alerts_received: get("ws_alerts_received", "count") || 0,
    seq_violations: get("ws_seq_violations", "count") || 0,
    delivery_ms: {
      count: get("ws_delivery_ms", "count"),
      min: get("ws_delivery_ms", "min"),
      p50: get("ws_delivery_ms", "med"),
      p90: get("ws_delivery_ms", "p(90)"),
      p95: get("ws_delivery_ms", "p(95)"),
      p99: get("ws_delivery_ms", "p(99)"),
      max: get("ws_delivery_ms", "max"),
    },
    snapshots_per_client: {
      min: get("ws_snapshots_per_client", "min"),
      p50: get("ws_snapshots_per_client", "med"),
      max: get("ws_snapshots_per_client", "max"),
      clients: get("ws_snapshots_per_client", "count"),
    },
    received_over_expected: {
      min: get("ws_received_over_expected", "min"),
      p50: get("ws_received_over_expected", "med"),
      p90: get("ws_received_over_expected", "p(90)"),
      max: get("ws_received_over_expected", "max"),
    },
    expected_snapshots_per_client_if_connected_for_ramp_plus_hold: 1 + Math.floor((RAMP_S + HOLD_S) / 2),
    iterations: get("iterations", "count"),
  };
  return { [`results/oncall/raw/ws_${VARIANT}.json`]: JSON.stringify(out, null, 2) };
}
