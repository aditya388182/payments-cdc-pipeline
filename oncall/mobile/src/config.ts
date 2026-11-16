import Constants from "expo-constants";

/** Base URL of the oncall-api. EXPO_PUBLIC_* is inlined at build time. */
export function apiBaseUrl(): string {
  const fromEnv = process.env.EXPO_PUBLIC_API_URL;
  if (fromEnv) return fromEnv.replace(/\/$/, "");
  const fromExtra = (Constants.expoConfig?.extra as { apiUrl?: string } | undefined)?.apiUrl;
  return (fromExtra ?? "http://127.0.0.1:8088").replace(/\/$/, "");
}

export const WS_STALE_MS = 7000; // the server pushes every 2 s; silence for 3.5 intervals means a dead socket
