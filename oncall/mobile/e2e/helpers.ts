import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";

export const REPO = path.resolve(__dirname, "../../..");
export const API = process.env.E2E_API ?? "http://127.0.0.1:8088";
export const SHOTS = path.join(REPO, "docs/oncall/screenshots");
export const STATE_FILE = path.join(__dirname, ".state.json");

export interface E2EState {
  transactionId: string;
  merchant: string;
  dlqId: string;
  fixedAtMs: number;
  cacheWaitMs: number;
}

export function readState(): E2EState {
  return JSON.parse(readFileSync(STATE_FILE, "utf8")) as E2EState;
}

/** Mints a dev access token with the API binary (the secret comes from .env and is never printed). */
export function mintToken(role: "admin" | "viewer", sub: string): string {
  const env: NodeJS.ProcessEnv = { ...process.env };
  for (const line of readFileSync(path.join(REPO, ".env"), "utf8").split("\n")) {
    const i = line.indexOf("=");
    if (i > 0 && !line.startsWith("#")) env[line.slice(0, i)] ??= line.slice(i + 1);
  }
  return execFileSync(path.join(REPO, "oncall/bin/oncall-api"), ["token", "-sub", sub, "-role", role], { env }).toString().trim();
}

export async function api<T>(method: string, route: string, token: string, body?: unknown, headers: Record<string, string> = {}): Promise<{ status: number; body: T }> {
  const res = await fetch(`${API}${route}`, {
    method,
    headers: { Authorization: `Bearer ${token}`, ...(body ? { "Content-Type": "application/json" } : {}), ...headers },
    body: body ? JSON.stringify(body) : undefined,
  });
  return { status: res.status, body: (await res.json().catch(() => ({}))) as T };
}

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
