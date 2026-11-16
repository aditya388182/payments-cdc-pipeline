import { fireEvent, render, screen, waitFor } from "@testing-library/react-native";
import React from "react";
import type { Alert, Role, Snapshot } from "@/api/types";
import { AuthContextForTests, type AuthValue } from "@/auth/AuthContext";
import type { LiveState } from "@/live/useLive";

jest.mock("expo-crypto", () => ({ randomUUID: () => "key-00000001" }));
jest.mock("expo-router", () => ({ useRouter: () => ({ push: jest.fn() }), useFocusEffect: () => undefined, useLocalSearchParams: () => ({}) }));

let mockLive: LiveState;
jest.mock("@/live/useLive", () => ({ useLive: () => mockLive, wsUrl: () => "" }));

import { AlertsScreen } from "@/screens/AlertsScreen";
import { LiveScreen } from "@/screens/LiveScreen";

const snapshot = (over: Partial<Snapshot> = {}): Snapshot => ({
  seq: 42, ts: "2026-11-16T12:00:00Z", events_in_per_sec: 20.4, events_out_per_sec: 11.6, lag_total: 383, lag_partitions: [], caught_up: false,
  freshness_seconds: 12, freshness_p50_seconds: 8, freshness_p95_seconds: 26, connector: { state: "RUNNING", tasks: ["RUNNING"] }, source_count: 4793, lake_count: 4671,
  parity: { state: "in_sync", diff: 122, allowed: 780, drained: false, detail: "" }, dlq: { messages: 110, open: 1, growth_per_min: 0 }, sources: {}, ...over,
});

const authWith = (api: unknown, role: Role = "viewer"): AuthValue =>
  ({ status: "signedIn", role, user: { login: "u", role }, api, tokens: { getAccessToken: async () => "t", refresh: async () => ({}) }, error: null, signIn: jest.fn(), signOut: jest.fn(), completeSignIn: jest.fn() }) as unknown as AuthValue;

const wrap = (a: AuthValue, ui: React.ReactElement): React.ReactElement => <AuthContextForTests.Provider value={a}>{ui}</AuthContextForTests.Provider>;

describe("LiveScreen", () => {
  it("shows the parity badge, connector, lag and a connected banner from the live snapshot", async () => {
    mockLive = { status: "open", attempt: 0, latest: snapshot(), history: [snapshot(), snapshot({ seq: 43 })], lastAlertEvent: null };
    await render(wrap(authWith({}), <LiveScreen />));
    expect(screen.getByTestId("parity-badge").props.accessibilityLabel).toBe("In sync");
    expect(screen.getByTestId("connector-status").props.accessibilityLabel).toBe("RUNNING");
    expect(screen.getByTestId("tile-lag").props.accessibilityLabel).toBe("Consumer lag: 383, records behind");
    expect(screen.getByTestId("tile-freshness").props.accessibilityLabel).toBe("Freshness p95: 26s, p50 8s");
    expect(screen.getByTestId("connection-banner").props.accessibilityLabel).toBe("Connected, live");
    expect(screen.getByTestId("chart-throughput").props.accessibilityLabel).toMatch(/events in now 20.4\/s, merged out now 11.6\/s/);
  });

  it("shows a reconnecting indicator with the attempt number", async () => {
    mockLive = { status: "reconnecting", attempt: 3, latest: snapshot(), history: [], lastAlertEvent: null };
    await render(wrap(authWith({}), <LiveScreen />));
    expect(screen.getByTestId("connection-banner").props.accessibilityLabel).toBe("Reconnecting, attempt 3");
    expect(screen.getByText(/Reconnecting… \(attempt 3\)/)).toBeTruthy();
  });

  it("shows a loading state before the first snapshot", async () => {
    mockLive = { status: "connecting", attempt: 0, latest: null, history: [], lastAlertEvent: null };
    await render(wrap(authWith({}), <LiveScreen />));
    expect(screen.getByTestId("loading")).toBeTruthy();
  });

  it.each([
    ["catching_up", "Catching up"],
    ["mismatch", "Mismatch"],
    ["unknown", "Unknown"],
  ] as const)("renders the %s parity state", async (state, label) => {
    mockLive = { status: "open", attempt: 0, latest: snapshot({ parity: { state, diff: 5, allowed: 1, drained: true, detail: "" } }), history: [], lastAlertEvent: null };
    await render(wrap(authWith({}), <LiveScreen />));
    expect(screen.getByTestId("parity-badge").props.accessibilityLabel).toBe(label);
  });
});

describe("AlertsScreen", () => {
  const firing: Alert = { id: 7, rule: "consumer_lag_high", severity: "critical", state: "firing", summary: "Consumer lag 5000 is above 1000", fired_at: "2026-11-16T12:00:00Z" };
  const resolved: Alert = { ...firing, id: 6, state: "resolved", resolved_at: "2026-11-16T11:00:00Z" };

  it("lists alerts by state and acknowledges a firing one", async () => {
    const api = { alerts: jest.fn(async () => ({ alerts: [firing, resolved] })), ackAlert: jest.fn(async () => ({ ...firing, state: "acknowledged" })) };
    await render(wrap(authWith(api), <AlertsScreen />));
    await waitFor(() => expect(screen.getByTestId("alerts-firing")).toBeTruthy());
    expect(screen.getByText("Firing (1)")).toBeTruthy();
    expect(screen.getByText("Resolved (1)")).toBeTruthy();
    expect(screen.queryByTestId("ack-6")).toBeNull(); // resolved alerts cannot be acknowledged
    await fireEvent.press(screen.getByTestId("ack-7"));
    await waitFor(() => expect(api.ackAlert).toHaveBeenCalledWith(7));
  });

  it("offers the acknowledge button to a viewer too (acknowledging is a viewer action)", async () => {
    const api = { alerts: jest.fn(async () => ({ alerts: [firing] })), ackAlert: jest.fn() };
    await render(wrap(authWith(api, "viewer"), <AlertsScreen />));
    await waitFor(() => expect(screen.getByTestId("ack-7")).toBeTruthy());
  });

  it("shows the empty state and the error state", async () => {
    await render(wrap(authWith({ alerts: async () => ({ alerts: [] }) }), <AlertsScreen />));
    await waitFor(() => expect(screen.getByTestId("empty-state")).toBeTruthy());
  });

  it("shows an error state with a retry when the request fails", async () => {
    const alertsFn = jest.fn().mockRejectedValueOnce(new Error("boom")).mockResolvedValue({ alerts: [firing] });
    await render(wrap(authWith({ alerts: alertsFn }), <AlertsScreen />));
    await waitFor(() => expect(screen.getByTestId("error-state")).toBeTruthy());
    expect(screen.getByText("boom")).toBeTruthy();
    await fireEvent.press(screen.getByLabelText("Try again"));
    await waitFor(() => expect(screen.getByTestId("alerts-firing")).toBeTruthy());
  });
});
