import { newIdempotencyKey } from "@/auth/pkce";
import { SessionExpiredError, type TokenManager } from "@/auth/tokens";
import type {
  Alert, AuditEntry, DlqPage, DlqRecord, GeneratorStatus, ReplayResult, TokenResponse,
} from "./types";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

type FetchFn = typeof fetch;

interface RequestOptions {
  method?: "GET" | "POST" | "DELETE";
  body?: unknown;
  headers?: Record<string, string>;
  auth?: boolean;
}

async function parseError(res: Response): Promise<ApiError> {
  let code = "http_" + String(res.status);
  let message = res.statusText || "request failed";
  try {
    const j = (await res.json()) as { error?: { code?: string; message?: string } };
    if (j.error?.code) code = j.error.code;
    if (j.error?.message) message = j.error.message;
  } catch {
    /* not JSON */
  }
  return new ApiError(res.status, code, message);
}

/** The exchange of a refresh token for a new pair; throws SessionExpiredError when the server says the session is dead. */
export async function refreshWith(baseUrl: string, refreshToken: string, fetchFn: FetchFn = fetch): Promise<TokenResponse> {
  const res = await fetchFn(`${baseUrl}/v1/auth/token`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ refresh_token: refreshToken }),
  });
  if (res.status === 401 || res.status === 400) throw new SessionExpiredError((await parseError(res)).message);
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as TokenResponse;
}

export async function exchangeCode(baseUrl: string, code: string, verifier: string, fetchFn: FetchFn = fetch): Promise<TokenResponse> {
  const res = await fetchFn(`${baseUrl}/v1/auth/token`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ code, code_verifier: verifier }),
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as TokenResponse;
}

export class ApiClient {
  constructor(
    readonly baseUrl: string,
    private readonly tokens: TokenManager,
    // Not `= fetch`: browsers throw "Illegal invocation" when window.fetch is called as a method of another object
    // (this.fetchFn(...) would bind `this` to the ApiClient). Wrapping calls it unbound.
    private readonly fetchFn: FetchFn = (input, init) => fetch(input, init),
  ) {}

  private async send(path: string, opts: RequestOptions, token: string | null): Promise<Response> {
    const headers: Record<string, string> = { Accept: "application/json", ...opts.headers };
    if (opts.body !== undefined) headers["Content-Type"] = "application/json";
    if (token) headers.Authorization = `Bearer ${token}`;
    return this.fetchFn(`${this.baseUrl}${path}`, {
      method: opts.method ?? "GET",
      headers,
      body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
    });
  }

  /** One request with auth. A 401 triggers exactly one refresh and one retry; a dead session signs the user out. */
  async raw<T>(path: string, opts: RequestOptions = {}, accept: number[] = []): Promise<{ status: number; body: T }> {
    const wantAuth = opts.auth !== false;
    let token = wantAuth ? await this.tokens.getAccessToken() : null;
    let res = await this.send(path, opts, token);
    if (res.status === 401 && wantAuth) {
      token = (await this.tokens.refresh()).accessToken;
      res = await this.send(path, opts, token);
    }
    if (!res.ok && !accept.includes(res.status)) throw await parseError(res);
    return { status: res.status, body: (await res.json()) as T };
  }

  async request<T>(path: string, opts: RequestOptions = {}): Promise<T> {
    return (await this.raw<T>(path, opts)).body;
  }

  snapshot = () => this.request<import("./types").Snapshot>("/v1/snapshot");
  alerts = (state?: string) => this.request<{ alerts: Alert[] }>(`/v1/alerts${state ? `?state=${state}` : ""}`);
  ackAlert = (id: number) => this.request<Alert>(`/v1/alerts/${String(id)}/ack`, { method: "POST" });
  dlq = (opts: { cursor?: string; reason?: string; limit?: number } = {}) => {
    const q = new URLSearchParams();
    if (opts.cursor) q.set("cursor", opts.cursor);
    if (opts.reason) q.set("reason", opts.reason);
    q.set("limit", String(opts.limit ?? 25));
    return this.request<DlqPage>(`/v1/dlq?${q.toString()}`);
  };
  dlqRecord = (id: string) => this.request<DlqRecord>(`/v1/dlq/${encodeURIComponent(id)}`);
  /** 200 APPLIED / ALREADY_APPLIED, 202 PENDING, 422 STILL_INVALID and 404 NOT_FOUND all carry a result body. */
  replay = async (id: string, idempotencyKey: string = newIdempotencyKey()): Promise<ReplayResult> => {
    const { status, body } = await this.raw<ReplayResult & { error?: { code: string; message: string } }>(
      `/v1/dlq/${encodeURIComponent(id)}/replay`,
      { method: "POST", headers: { "Idempotency-Key": idempotencyKey } },
      [202, 404, 422],
    );
    if (!body.status) throw new ApiError(status, body.error?.code ?? "bad_response", body.error?.message ?? "unexpected response");
    return body;
  };
  audit = (cursor?: string) => this.request<{ entries: AuditEntry[]; next_cursor: string }>(`/v1/audit${cursor ? `?cursor=${cursor}` : ""}`);
  registerDevice = (token: string, platform: string) =>
    this.request<{ registered: boolean }>("/v1/devices", { method: "POST", body: { expo_push_token: token, platform } });
  unregisterDevice = (token: string) => this.request<{ registered: boolean }>("/v1/devices", { method: "DELETE", body: { expo_push_token: token } });
  generator = () => this.request<{ generator: GeneratorStatus; max_rate: number }>("/v1/generator");
  setGenerator = (action: "start" | "stop", rate?: number) =>
    this.request<{ generator: GeneratorStatus }>("/v1/generator", {
      method: "POST",
      body: action === "start" ? { action, rate } : { action },
      headers: { "Idempotency-Key": newIdempotencyKey() },
    });
  logout = async (): Promise<void> => {
    try {
      const token = await this.tokens.getAccessToken();
      await this.fetchFn(`${this.baseUrl}/v1/auth/logout`, { method: "POST", headers: { Authorization: `Bearer ${token}` } });
    } catch {
      /* signing out locally is what matters */
    }
  };
}
