// Captures the live WebSocket feed (Node 24 has a built-in WebSocket client).
// usage: node ws_capture.mjs <ws-url> <seconds> [out.jsonl]
import { writeFileSync } from "node:fs";

const [url, secs = "20", out] = process.argv.slice(2);
const lines = [];
const t0 = Date.now();
const ws = new WebSocket(url);
ws.onmessage = (ev) => {
  const m = JSON.parse(ev.data);
  const rec = { recv_ms: Date.now() - t0, type: m.type, seq: m.seq };
  if (m.type === "snapshot") {
    const d = m.data;
    Object.assign(rec, {
      server_ts: d.ts, lag_total: d.lag_total, events_in_per_sec: Number(d.events_in_per_sec.toFixed(2)),
      events_out_per_sec: Number(d.events_out_per_sec.toFixed(2)), freshness_p95: Number(d.freshness_p95_seconds.toFixed(2)),
      connector: d.connector.state, source_count: d.source_count, lake_count: d.lake_count, parity: d.parity.state,
      parity_diff: d.parity.diff, dlq_open: d.dlq.open,
    });
  } else if (m.type === "alert") {
    Object.assign(rec, { event: m.event, rule: m.data.rule, state: m.data.state });
  } else {
    Object.assign(rec, m);
  }
  lines.push(rec);
  console.log(JSON.stringify(rec));
};
ws.onerror = (e) => { console.error("ws error", e.message ?? e); };
setTimeout(() => {
  ws.close();
  if (out) writeFileSync(out, lines.map((l) => JSON.stringify(l)).join("\n") + "\n");
  process.exit(0);
}, Number(secs) * 1000);
