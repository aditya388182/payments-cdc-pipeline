import type { Role, TokenResponse, User } from "@/api/types";
import type { KeyValueStore } from "./storage";

export interface TokenSet {
  accessToken: string;
  refreshToken: string;
  /** epoch ms when the access token expires */
  expiresAt: number;
  user: User;
}

const KEY = "oncall.tokens.v1";
const REFRESH_SKEW_MS = 30_000;

export class SessionExpiredError extends Error {
  constructor(message = "session expired") {
    super(message);
    this.name = "SessionExpiredError";
  }
}

export type RefreshFn = (refreshToken: string) => Promise<TokenResponse>;

/**
 * Owns the token set. Every caller that needs a token goes through getAccessToken(), which refreshes shortly before
 * expiry. Refreshes are single-flight: concurrent callers share ONE refresh request. This matters because the server
 * rotates refresh tokens and treats a second use of the same token as theft (it revokes the whole session).
 */
export class TokenManager {
  private tokens: TokenSet | null = null;
  private loaded = false;
  private inflight: Promise<TokenSet> | null = null;
  private listeners = new Set<(t: TokenSet | null) => void>();

  constructor(
    private readonly store: KeyValueStore,
    private readonly refreshFn: RefreshFn,
    private readonly now: () => number = Date.now,
  ) {}

  subscribe(fn: (t: TokenSet | null) => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  private emit(): void {
    for (const l of this.listeners) l(this.tokens);
  }

  async load(): Promise<TokenSet | null> {
    if (!this.loaded) {
      const raw = await this.store.get(KEY);
      if (raw) {
        try {
          this.tokens = JSON.parse(raw) as TokenSet;
        } catch {
          this.tokens = null;
        }
      }
      this.loaded = true;
    }
    return this.tokens;
  }

  current(): TokenSet | null {
    return this.tokens;
  }

  role(): Role | null {
    return this.tokens?.user.role ?? null;
  }

  async setFromResponse(r: TokenResponse): Promise<TokenSet> {
    this.tokens = {
      accessToken: r.access_token,
      refreshToken: r.refresh_token,
      expiresAt: this.now() + r.expires_in * 1000,
      user: r.user,
    };
    this.loaded = true;
    await this.store.set(KEY, JSON.stringify(this.tokens));
    this.emit();
    return this.tokens;
  }

  async clear(): Promise<void> {
    this.tokens = null;
    this.loaded = true;
    await this.store.remove(KEY);
    this.emit();
  }

  /** A valid access token, refreshing first if it expires within 30 s. Throws SessionExpiredError if signed out. */
  async getAccessToken(): Promise<string> {
    await this.load();
    if (!this.tokens) throw new SessionExpiredError("not signed in");
    if (this.tokens.expiresAt - this.now() > REFRESH_SKEW_MS) return this.tokens.accessToken;
    return (await this.refresh()).accessToken;
  }

  /** Force a refresh (used after a 401). Single-flight. */
  refresh(): Promise<TokenSet> {
    if (this.inflight) return this.inflight;
    const current = this.tokens;
    if (!current) return Promise.reject(new SessionExpiredError("not signed in"));
    this.inflight = (async () => {
      try {
        const res = await this.refreshFn(current.refreshToken);
        return await this.setFromResponse(res);
      } catch (e) {
        if (e instanceof SessionExpiredError) await this.clear();
        throw e;
      } finally {
        this.inflight = null;
      }
    })();
    return this.inflight;
  }
}
