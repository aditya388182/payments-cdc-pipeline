import * as Linking from "expo-linking";
import * as WebBrowser from "expo-web-browser";
import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import { Platform } from "react-native";
import { ApiClient, exchangeCode, refreshWith } from "@/api/client";
import type { Role, User } from "@/api/types";
import { apiBaseUrl } from "@/config";
import { createPkcePair } from "./pkce";
import { defaultStore, type KeyValueStore } from "./storage";
import { TokenManager } from "./tokens";

export type AuthStatus = "loading" | "signedOut" | "signedIn";

export interface AuthValue {
  status: AuthStatus;
  user: User | null;
  role: Role | null;
  api: ApiClient;
  tokens: TokenManager;
  error: string | null;
  signIn: () => Promise<void>;
  signOut: () => Promise<void>;
  completeSignIn: (code: string) => Promise<void>;
}

const Ctx = createContext<AuthValue | null>(null);

const PKCE_KEY = "oncall.pkce.verifier";

export function useAuth(): AuthValue {
  const v = useContext(Ctx);
  if (!v) throw new Error("useAuth must be used inside <AuthProvider>");
  return v;
}

export function startUrl(base: string, challenge: string, redirectUri: string): string {
  const q = new URLSearchParams({ code_challenge: challenge, code_challenge_method: "S256", redirect_uri: redirectUri });
  return `${base}/v1/auth/github/start?${q.toString()}`;
}

export function AuthProvider({ children, store }: { children: React.ReactNode; store?: KeyValueStore }): React.JSX.Element {
  const base = apiBaseUrl();
  const kv = useMemo(() => store ?? defaultStore(), [store]);
  const tokens = useMemo(() => new TokenManager(kv, (rt) => refreshWith(base, rt)), [kv, base]);
  const api = useMemo(() => new ApiClient(base, tokens), [base, tokens]);
  const [status, setStatus] = useState<AuthStatus>("loading");
  const [user, setUser] = useState<User | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    const unsub = tokens.subscribe((t) => {
      setUser(t?.user ?? null);
      setStatus(t ? "signedIn" : "signedOut");
    });
    void tokens.load().then((t) => {
      if (!alive) return;
      setUser(t?.user ?? null);
      setStatus(t ? "signedIn" : "signedOut");
    });
    return () => {
      alive = false;
      unsub();
    };
  }, [tokens]);

  const completeSignIn = useCallback(
    async (code: string) => {
      const verifier = await kv.get(PKCE_KEY);
      await kv.remove(PKCE_KEY);
      if (!verifier) throw new Error("sign-in was not started from this device");
      await tokens.setFromResponse(await exchangeCode(base, code, verifier));
      setError(null);
    },
    [kv, tokens, base],
  );

  const signIn = useCallback(async () => {
    setError(null);
    try {
      const { verifier, challenge } = await createPkcePair();
      await kv.set(PKCE_KEY, verifier);
      if (Platform.OS === "web") {
        // full-page redirect: the callback route (app/auth/callback) finishes the exchange
        const redirect = `${globalThis.location.origin}/auth/callback`;
        globalThis.location.assign(startUrl(base, challenge, redirect));
        return;
      }
      const redirect = Linking.createURL("auth/callback");
      const res = await WebBrowser.openAuthSessionAsync(startUrl(base, challenge, redirect), redirect);
      if (res.type !== "success") {
        setError("Sign-in was cancelled.");
        return;
      }
      const q = new URL(res.url).searchParams;
      const code = q.get("code");
      if (!code) {
        setError(q.get("error") === "not_allowed" ? "This GitHub account is not allowed to use the console." : "Sign-in failed. Please try again.");
        return;
      }
      await completeSignIn(code);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Sign-in failed.");
    }
  }, [base, kv, completeSignIn]);

  const signOut = useCallback(async () => {
    await api.logout();
    await tokens.clear();
  }, [api, tokens]);

  const value = useMemo<AuthValue>(
    () => ({ status, user, role: user?.role ?? null, api, tokens, error, signIn, signOut, completeSignIn }),
    [status, user, api, tokens, error, signIn, signOut, completeSignIn],
  );
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export { Ctx as AuthContextForTests };
