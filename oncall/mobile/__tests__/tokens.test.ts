import { ApiClient, ApiError, refreshWith } from "@/api/client";
import type { TokenResponse } from "@/api/types";
import { MemoryStore } from "@/auth/storage";
import { SessionExpiredError, TokenManager } from "@/auth/tokens";

jest.mock("expo-crypto", () => ({ randomUUID: () => "11111111-1111-4111-8111-111111111111" }));

const resp = (n: number, role: "viewer" | "admin" = "admin", expiresIn = 900): TokenResponse => ({
  access_token: `access-${String(n)}`, refresh_token: `refresh-${String(n)}`, token_type: "Bearer", expires_in: expiresIn, user: { login: "admin-user", role },
});

function setup(initial?: TokenResponse) {
  let t = 1_000_000;
  const store = new MemoryStore();
  const refresh = jest.fn(async (rt: string): Promise<TokenResponse> => {
    void rt;
    return resp(refresh.mock.calls.length + 1);
  });
  const tm = new TokenManager(store, refresh, () => t);
  return { tm, store, refresh, advance: (ms: number) => (t += ms), ready: async () => { if (initial) await tm.setFromResponse(initial); } };
}

describe("TokenManager", () => {
  it("returns the stored access token without refreshing while it is valid", async () => {
    const s = setup(resp(1));
    await s.ready();
    expect(await s.tm.getAccessToken()).toBe("access-1");
    expect(s.refresh).not.toHaveBeenCalled();
  });

  it("refreshes shortly before expiry, using the stored refresh token, and persists the rotated pair", async () => {
    const s = setup(resp(1));
    await s.ready();
    s.advance(900_000 - 10_000); // 10 s left: inside the 30 s skew
    expect(await s.tm.getAccessToken()).toBe("access-2");
    expect(s.refresh).toHaveBeenCalledTimes(1);
    expect(s.refresh).toHaveBeenCalledWith("refresh-1");
    s.advance(900_000 - 10_000);
    await s.tm.getAccessToken();
    expect(s.refresh).toHaveBeenLastCalledWith("refresh-2"); // the rotated token is the one used next, never the old one
    const stored = JSON.parse((await s.store.get("oncall.tokens.v1")) ?? "{}") as { refreshToken: string };
    expect(stored.refreshToken).toBe("refresh-3");
  });

  it("shares ONE refresh between concurrent callers (a second use of a refresh token would revoke the session)", async () => {
    const s = setup(resp(1));
    await s.ready();
    s.advance(900_000); // expired
    const tokens = await Promise.all([s.tm.getAccessToken(), s.tm.getAccessToken(), s.tm.getAccessToken(), s.tm.refresh().then((t) => t.accessToken)]);
    expect(new Set(tokens)).toEqual(new Set(["access-2"]));
    expect(s.refresh).toHaveBeenCalledTimes(1);
  });

  it("signs out when the server says the session is dead", async () => {
    const s = setup(resp(1));
    await s.ready();
    s.refresh.mockRejectedValueOnce(new SessionExpiredError("revoked"));
    const seen: (string | null)[] = [];
    s.tm.subscribe((t) => seen.push(t?.accessToken ?? null));
    s.advance(900_000);
    await expect(s.tm.getAccessToken()).rejects.toBeInstanceOf(SessionExpiredError);
    expect(s.tm.current()).toBeNull();
    expect(await s.store.get("oncall.tokens.v1")).toBeNull();
    expect(seen).toEqual([null]);
  });

  it("keeps the session on a transient refresh failure (network down) so it can retry", async () => {
    const s = setup(resp(1));
    await s.ready();
    s.refresh.mockRejectedValueOnce(new TypeError("Network request failed"));
    s.advance(900_000);
    await expect(s.tm.getAccessToken()).rejects.toThrow("Network request failed");
    expect(s.tm.current()?.refreshToken).toBe("refresh-1");
    expect(await s.tm.getAccessToken()).toBe("access-3"); // the next attempt works (the failed call also counted)
  });

  it("loads a previous session from storage", async () => {
    const s = setup(resp(7, "viewer"));
    await s.ready();
    const fresh = new TokenManager(s.store, s.refresh);
    expect((await fresh.load())?.user.role).toBe("viewer");
    expect(fresh.role()).toBe("viewer");
  });

  it("throws SessionExpiredError when nobody is signed in", async () => {
    const s = setup();
    await expect(s.tm.getAccessToken()).rejects.toBeInstanceOf(SessionExpiredError);
  });
});

describe("ApiClient token refresh on 401", () => {
  const json = (status: number, body: unknown): Response => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

  it("refreshes once after a 401 and retries the request with the new token", async () => {
    const s = setup(resp(1));
    await s.ready();
    const calls: (string | null)[] = [];
    const fetchFn = jest.fn(async (_url: unknown, init?: RequestInit) => {
      const auth = (init?.headers as Record<string, string>).Authorization ?? null;
      calls.push(auth);
      return auth === "Bearer access-1" ? json(401, { error: { code: "token_expired", message: "expired" } }) : json(200, { alerts: [] });
    });
    const api = new ApiClient("http://x", s.tm, fetchFn as unknown as typeof fetch);
    await expect(api.alerts()).resolves.toEqual({ alerts: [] });
    expect(calls).toEqual(["Bearer access-1", "Bearer access-2"]);
    expect(s.refresh).toHaveBeenCalledTimes(1);
  });

  it("does not loop: a second 401 is an error", async () => {
    const s = setup(resp(1));
    await s.ready();
    const fetchFn = jest.fn(async () => json(401, { error: { code: "unauthorized", message: "nope" } }));
    const api = new ApiClient("http://x", s.tm, fetchFn as unknown as typeof fetch);
    await expect(api.alerts()).rejects.toMatchObject({ status: 401, code: "unauthorized" });
    expect(fetchFn).toHaveBeenCalledTimes(2);
    expect(s.refresh).toHaveBeenCalledTimes(1);
  });

  it("signs the user out when the refresh itself is rejected", async () => {
    const s = setup(resp(1));
    await s.ready();
    s.refresh.mockRejectedValueOnce(new SessionExpiredError("reuse detected"));
    const api = new ApiClient("http://x", s.tm, (async () => json(401, { error: { code: "token_expired", message: "e" } })) as unknown as typeof fetch);
    await expect(api.alerts()).rejects.toBeInstanceOf(SessionExpiredError);
    expect(s.tm.current()).toBeNull();
  });

  it("maps error bodies to ApiError and sends the Idempotency-Key on replay", async () => {
    const s = setup(resp(1));
    await s.ready();
    const fetchFn = jest.fn(async (_u: unknown, init?: RequestInit) => {
      expect((init?.headers as Record<string, string>)["Idempotency-Key"]).toBe("key-123456");
      return json(422, { dlq_id: "5", status: "STILL_INVALID", detail: "validator still rejects the record: UNKNOWN_MERCHANT", lake_lsn: 0 });
    });
    const api = new ApiClient("http://x", s.tm, fetchFn as unknown as typeof fetch);
    await expect(api.replay("5", "key-123456")).resolves.toMatchObject({ status: "STILL_INVALID" });

    const failing = new ApiClient("http://x", s.tm, (async () => json(403, { error: { code: "forbidden", message: "admin role required" } })) as unknown as typeof fetch);
    const e = await failing.replay("5", "key-123456").catch((x: unknown) => x);
    expect(e).toBeInstanceOf(ApiError);
    expect(e).toMatchObject({ status: 403, code: "forbidden", message: "admin role required" });
  });
});

describe("ApiClient and the global fetch", () => {
  it("calls the default fetch unbound (browsers throw 'Illegal invocation' if it is called as a method of the client)", async () => {
    const real = globalThis.fetch;
    const strictFetch = function (this: unknown): Promise<Response> {
      if (this !== undefined && this !== globalThis) throw new TypeError("Failed to execute 'fetch' on 'Window': Illegal invocation");
      return Promise.resolve(new Response(JSON.stringify({ alerts: [] }), { status: 200, headers: { "Content-Type": "application/json" } }));
    };
    globalThis.fetch = strictFetch as unknown as typeof fetch;
    try {
      const s = setup(resp(1));
      await s.ready();
      await expect(new ApiClient("http://x", s.tm).alerts()).resolves.toEqual({ alerts: [] });
    } finally {
      globalThis.fetch = real;
    }
  });
});

describe("refreshWith", () => {
  it("treats 401 as a dead session and other failures as transient", async () => {
    const dead = (async () => new Response(JSON.stringify({ error: { code: "invalid_grant", message: "reused" } }), { status: 401 })) as unknown as typeof fetch;
    await expect(refreshWith("http://x", "r", dead)).rejects.toBeInstanceOf(SessionExpiredError);
    const down = (async () => new Response("bad gateway", { status: 502 })) as unknown as typeof fetch;
    const e = await refreshWith("http://x", "r", down).catch((x: unknown) => x);
    expect(e).toBeInstanceOf(ApiError);
    expect(e).not.toBeInstanceOf(SessionExpiredError);
  });
});
