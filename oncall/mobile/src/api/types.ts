export type Role = "viewer" | "admin";

export interface User {
  login: string;
  role: Role;
}

export interface TokenResponse {
  access_token: string;
  refresh_token: string;
  token_type: string;
  expires_in: number;
  user: User;
}

export interface PartitionLag {
  partition: number;
  end: number;
  processed: number;
  lag: number;
}

export type ParityState = "in_sync" | "catching_up" | "mismatch" | "unknown";

export interface Parity {
  state: ParityState;
  diff: number;
  allowed: number;
  drained: boolean;
  detail: string;
}

export interface Snapshot {
  seq: number;
  ts: string;
  events_in_per_sec: number;
  events_out_per_sec: number;
  lag_total: number;
  lag_partitions: PartitionLag[];
  caught_up: boolean;
  freshness_seconds: number;
  freshness_p50_seconds: number;
  freshness_p95_seconds: number;
  connector: { state: string; tasks?: string[] };
  source_count: number;
  lake_count: number;
  parity: Parity;
  dlq: { messages: number; open: number; growth_per_min: number };
  sources: Record<string, { ok: boolean; error?: string; age_seconds: number }>;
}

export type AlertState = "firing" | "acknowledged" | "resolved";

export interface Alert {
  id: number;
  rule: string;
  severity: string;
  state: AlertState;
  summary: string;
  fired_at: string;
  acked_at?: string;
  acked_by?: string;
  resolved_at?: string;
}

export interface DlqRecord {
  id: string;
  transaction_id: string;
  merchant_id: string;
  amount_minor: number;
  currency: string;
  status: string;
  event_type: string;
  op: string;
  lsn: number;
  rejection_reason: string;
  rejected_at: string;
  applied: boolean;
  payload_json?: string;
}

export interface DlqPage {
  records: DlqRecord[];
  next_cursor: string;
  topic_messages: number;
  open_total: number;
}

export type ReplayStatus = "APPLIED" | "ALREADY_APPLIED" | "STILL_INVALID" | "NOT_FOUND" | "PENDING";

export interface ReplayResult {
  dlq_id: string;
  status: ReplayStatus;
  detail: string;
  lake_lsn: number;
}

export interface AuditEntry {
  id: number;
  actor: string;
  role: string;
  action: string;
  target: string;
  requested_at: string;
  finished_at: string;
  result: string;
  error?: string;
  idempotency_key?: string;
}

export interface GeneratorStatus {
  running: boolean;
  rate: number;
}

// Server -> client WebSocket frames
export interface HelloFrame {
  type: "hello";
  seq: number;
  epoch: number;
  resumed: boolean;
  missed_alerts: number;
}
export interface SnapshotFrame {
  type: "snapshot";
  seq: number;
  data: Snapshot;
}
export interface AlertFrame {
  type: "alert";
  seq: number;
  event: AlertState;
  data: Alert;
}
export type Frame = HelloFrame | SnapshotFrame | AlertFrame;
